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
	"sync"
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
)

const postHogTestEventID = "3f6b0c6e-6d1f-4d5f-9f0a-3f6b0c6e6d1f"

// ackState records what the processor decided for one broker message.
type ackState struct {
	mu     sync.Mutex
	acked  bool
	nacked bool
}

func (a *ackState) ack()  { a.mu.Lock(); a.acked = true; a.mu.Unlock() }
func (a *ackState) nack() { a.mu.Lock(); a.nacked = true; a.mu.Unlock() }
func (a *ackState) get() (bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acked, a.nacked
}

func evaluationMessage(t *testing.T, id string) (*puller.Message, *ackState) {
	t.Helper()
	inner, err := anypb.New(&eventproto.EvaluationEvent{
		Timestamp:      time.Now().Unix(),
		FeatureId:      "feature-1",
		FeatureVersion: 3,
		UserId:         "user-1",
		VariationId:    "variation-1",
		Reason:         &featureproto.Reason{Type: featureproto.Reason_DEFAULT},
		Tag:            "web",
		SourceId:       eventproto.SourceId_GO_SERVER,
		SdkVersion:     "1.0.0",
	})
	require.NoError(t, err)
	outer, err := proto.Marshal(&eventproto.Event{
		Id:            id,
		EnvironmentId: "env-1",
		Event:         inner,
	})
	require.NoError(t, err)

	state := &ackState{}
	return &puller.Message{
		Attributes: map[string]string{"id": id},
		Data:       outer,
		Ack:        state.ack,
		Nack:       state.nack,
	}, state
}

// newTestExporter wires a real SDK client at an httptest endpoint, so the ack decision is
// driven by an actual HTTP round trip rather than a stubbed client.
func newTestExporter(t *testing.T, handler http.HandlerFunc) (*postHogEvaluationExporter, *httptest.Server) {
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
		BatchSize:             1,
		FlushIntervalSec:      1,
		DeliveryTimeoutSec:    5,
	}
	client, err := posthog.NewClient(config, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return NewPostHogEvaluationExporter(client, config, zap.NewNop()).(*postHogEvaluationExporter), server
}

func TestAcksOnlyAfterConfirmedDelivery(t *testing.T) {
	var body []byte
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, state := evaluationMessage(t, postHogTestEventID)
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	assert.True(t, acked)
	assert.False(t, nacked)

	// The payload carries the contract, including the preserved outer UUID.
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Contains(t, string(body), "bucketeer_feature_evaluated")
	assert.Contains(t, string(body), postHogTestEventID)
}

func TestNacksOnRetryableServerError(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	msg, state := evaluationMessage(t, postHogTestEventID)
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	// Nacked, so broker redelivery is the next retry layer.
	assert.False(t, acked)
	assert.True(t, nacked)
}

func TestNacksOnDeliveryTimeout(t *testing.T) {
	// Sleep rather than block on a channel: the SDK client's Close runs before any
	// cleanup registered here, so a request that only unblocks at cleanup would
	// deadlock shutdown.
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	})
	exporter.config.DeliveryTimeoutSec = 1

	msg, state := evaluationMessage(t, postHogTestEventID)
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	// The event may still be in flight; the stable UUID makes a resend safe.
	assert.False(t, acked)
	assert.True(t, nacked)
}

func TestAcksAndDropsAMalformedOuterEvent(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	state := &ackState{}
	msg := &puller.Message{
		Attributes: map[string]string{"id": postHogTestEventID},
		Data:       []byte("not a protobuf"),
		Ack:        state.ack,
		Nack:       state.nack,
	}
	exporter.handle(context.Background(), msg)

	acked, nacked := state.get()
	// Acked: an unparseable event stays unparseable, so redelivering it would occupy
	// the subscription forever.
	assert.True(t, acked)
	assert.False(t, nacked)
}

func TestAcksAndDropsAnEventWithNoDistinctID(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	inner, err := anypb.New(&eventproto.EvaluationEvent{
		Timestamp: time.Now().Unix(),
		FeatureId: "feature-1",
	})
	require.NoError(t, err)
	data, err := proto.Marshal(&eventproto.Event{Id: postHogTestEventID, EnvironmentId: "env-1", Event: inner})
	require.NoError(t, err)

	state := &ackState{}
	exporter.handle(context.Background(), &puller.Message{
		Attributes: map[string]string{"id": postHogTestEventID},
		Data:       data,
		Ack:        state.ack,
		Nack:       state.nack,
	})

	acked, _ := state.get()
	assert.True(t, acked)
}

func TestAcksAMessageWithNoID(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	state := &ackState{}
	exporter.handle(context.Background(), &puller.Message{
		Attributes: map[string]string{},
		Data:       []byte{},
		Ack:        state.ack,
		Nack:       state.nack,
	})

	acked, _ := state.get()
	// Without the outer id there is no stable UUID, so a retry could not dedupe.
	assert.True(t, acked)
}

func TestDefaultPayloadCarriesNoUserAttributes(t *testing.T) {
	var body []byte
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	inner, err := anypb.New(&eventproto.EvaluationEvent{
		Timestamp:   time.Now().Unix(),
		FeatureId:   "feature-1",
		UserId:      "user-1",
		VariationId: "variation-1",
		SourceId:    eventproto.SourceId_GO_SERVER,
		Metadata:    map[string]string{"trace": "secret-trace"},
	})
	require.NoError(t, err)
	data, err := proto.Marshal(&eventproto.Event{Id: postHogTestEventID, EnvironmentId: "env-1", Event: inner})
	require.NoError(t, err)

	state := &ackState{}
	exporter.handle(context.Background(), &puller.Message{
		Attributes: map[string]string{"id": postHogTestEventID},
		Data:       data,
		Ack:        state.ack,
		Nack:       state.nack,
	})

	assert.NotContains(t, string(body), "secret-trace")
}

func TestPayloadNeverContainsTheAPIKey(t *testing.T) {
	var body []byte
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	msg, _ := evaluationMessage(t, postHogTestEventID)
	exporter.handle(context.Background(), msg)

	// The legacy batch endpoint authenticates with api_key in the request envelope, so
	// the key is expected there. What must never happen is the key becoming event data.
	var payload struct {
		APIKey string `json:"api_key"`
		Batch  []struct {
			Properties map[string]any `json:"properties"`
		} `json:"batch"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	require.Equal(t, "phc_test_key", payload.APIKey)
	require.NotEmpty(t, payload.Batch)

	for _, event := range payload.Batch {
		encoded, err := json.Marshal(event.Properties)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), "phc_test_key")
	}
}

func TestProcessStopsWhenTheChannelCloses(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	msgChan := make(chan *puller.Message)
	close(msgChan)
	assert.NoError(t, exporter.Process(context.Background(), msgChan))
}

func TestProcessStopsOnContextCancel(t *testing.T) {
	exporter, _ := newTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, exporter.Process(ctx, make(chan *puller.Message)))
}
