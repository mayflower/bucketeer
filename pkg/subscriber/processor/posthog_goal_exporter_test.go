// Copyright 2026 The Bucketeer Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

func goalMessage(t *testing.T, id string, mutate func(*eventproto.GoalEvent)) (*puller.Message, *ackState) {
	t.Helper()
	e := &eventproto.GoalEvent{
		Timestamp:  time.Now().Unix(),
		GoalId:     "goal-1",
		UserId:     "user-1",
		Value:      42.5,
		Tag:        "web",
		SourceId:   eventproto.SourceId_GO_SERVER,
		SdkVersion: "1.0.0",
	}
	if mutate != nil {
		mutate(e)
	}
	inner, err := anypb.New(e)
	require.NoError(t, err)
	data, err := proto.Marshal(&eventproto.Event{Id: id, EnvironmentId: "env-1", Event: inner})
	require.NoError(t, err)

	state := &ackState{}
	return &puller.Message{
		Attributes: map[string]string{"id": id},
		Data:       data,
		Ack:        state.ack,
		Nack:       state.nack,
	}, state
}

func newTestGoalExporter(
	t *testing.T,
	privacy posthog.PrivacyConfig,
	handler http.HandlerFunc,
) *postHogGoalExporter {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	keyPath := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("phc_test_key"), 0o600))

	config := &posthog.Config{
		Enabled:               true,
		Endpoint:              server.URL,
		ProjectAPIKeyFile:     keyPath,
		AllowInsecureEndpoint: true,
		ExportGoals:           true,
		BatchSize:             1,
		FlushIntervalSec:      1,
		DeliveryTimeoutSec:    5,
		Privacy:               privacy,
	}
	client, err := posthog.NewClient(config, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return NewPostHogGoalExporter(client, config, zap.NewNop()).(*postHogGoalExporter)
}

func TestGoalExporterAcksAfterConfirmedDelivery(t *testing.T) {
	var body []byte
	exporter := newTestGoalExporter(t, posthog.PrivacyConfig{}, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, state := goalMessage(t, postHogTestEventID, nil)
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	assert.True(t, acked)
	assert.False(t, nacked)

	assert.Contains(t, string(body), "bucketeer_goal_reached")
	assert.Contains(t, string(body), "goal-1")
	assert.Contains(t, string(body), postHogTestEventID)
}

func TestGoalExporterNacksOnRetryableFailure(t *testing.T) {
	exporter := newTestGoalExporter(t, posthog.PrivacyConfig{}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	msg, state := goalMessage(t, postHogTestEventID, nil)
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	assert.False(t, acked)
	assert.True(t, nacked)
}

func TestGoalExporterDropsAnEventWithNoDistinctID(t *testing.T) {
	exporter := newTestGoalExporter(t, posthog.PrivacyConfig{}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	msg, state := goalMessage(t, postHogTestEventID, func(e *eventproto.GoalEvent) {
		e.UserId = ""
		e.User = nil
	})
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	assert.True(t, acked)
	assert.False(t, nacked)
}

func TestGoalExporterDoesNotExportDeprecatedEvaluations(t *testing.T) {
	var body []byte
	exporter := newTestGoalExporter(t, posthog.PrivacyConfig{}, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, _ := goalMessage(t, postHogTestEventID, func(e *eventproto.GoalEvent) {
		e.Evaluations = []*featureproto.Evaluation{{Id: "eval-1", FeatureId: "secret-feature"}}
	})
	exporter.handle(context.Background(), msg)

	assert.NotContains(t, string(body), "secret-feature")
}

func TestGoalExporterHonoursTheAllowlist(t *testing.T) {
	var body []byte
	privacy := posthog.PrivacyConfig{
		UserAttributeAllowlist: []string{"plan"},
		MetadataAllowlist:      []string{"region"},
	}
	exporter := newTestGoalExporter(t, privacy, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, _ := goalMessage(t, postHogTestEventID, func(e *eventproto.GoalEvent) {
		e.User = &userproto.User{
			Id:   "user-1",
			Data: map[string]string{"plan": "pro", "email": "someone@example.com"},
		}
		e.Metadata = map[string]string{"region": "eu", "trace": "secret-trace"}
	})
	exporter.handle(context.Background(), msg)

	// Allowlisted keys are exported under their prefixes; everything else is withheld.
	assert.Contains(t, string(body), "bucketeer_user_plan")
	assert.Contains(t, string(body), "bucketeer_metadata_region")
	assert.NotContains(t, string(body), "someone@example.com")
	assert.NotContains(t, string(body), "secret-trace")
}

func TestGoalExporterEmitsGroupsWithoutIdentify(t *testing.T) {
	var body []byte
	privacy := posthog.PrivacyConfig{
		UserAttributeAllowlist: []string{"tenant_id"},
		GroupMappings: []posthog.GroupMapping{
			{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"},
		},
	}
	exporter := newTestGoalExporter(t, privacy, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, _ := goalMessage(t, postHogTestEventID, func(e *eventproto.GoalEvent) {
		e.User = &userproto.User{Id: "user-1", Data: map[string]string{"tenant_id": "acme"}}
	})
	exporter.handle(context.Background(), msg)

	var payload struct {
		Batch []struct {
			Event      string         `json:"event"`
			Properties map[string]any `json:"properties"`
		} `json:"batch"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	require.NotEmpty(t, payload.Batch)

	assert.Equal(t, "bucketeer_goal_reached", payload.Batch[0].Event)
	assert.Equal(t, map[string]any{"tenant": "acme"}, payload.Batch[0].Properties["$groups"])
	// No profile is created for the person or the group.
	assert.Equal(t, false, payload.Batch[0].Properties["$process_person_profile"])
	assert.NotContains(t, string(body), "$identify")
	assert.NotContains(t, string(body), "$groupidentify")
}

func TestGoalAndEvaluationExportersShareOneClient(t *testing.T) {
	// The server owns the client; neither processor may close a client the other still
	// uses, so both must accept the same instance and keep working.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	keyPath := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("phc_test_key"), 0o600))
	config := &posthog.Config{
		Enabled:               true,
		Endpoint:              server.URL,
		ProjectAPIKeyFile:     keyPath,
		AllowInsecureEndpoint: true,
		ExportEvaluations:     true,
		ExportGoals:           true,
		BatchSize:             1,
		FlushIntervalSec:      1,
		DeliveryTimeoutSec:    5,
	}
	client, err := posthog.NewClient(config, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	evaluation := NewPostHogEvaluationExporter(client, config, zap.NewNop()).(*postHogEvaluationExporter)
	goal := NewPostHogGoalExporter(client, config, zap.NewNop()).(*postHogGoalExporter)

	evalMsg, evalState := evaluationMessage(t, postHogTestEventID)
	evaluation.handle(context.Background(), evalMsg)
	goalMsg, goalState := goalMessage(t, "7c9e6679-7425-40de-944b-e07fc1f90ae7", nil)
	goal.handle(context.Background(), goalMsg)

	evalAcked, _ := evalState.get()
	goalAcked, _ := goalState.get()
	assert.True(t, evalAcked)
	assert.True(t, goalAcked)
}
