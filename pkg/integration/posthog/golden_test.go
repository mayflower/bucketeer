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

package posthog

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	posthogsdk "github.com/posthog/posthog-go"

	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite the runtime event golden files")

const goldenDir = "../../../test/fixtures/analytics_export_v1/events"

// goldenCapture serializes through the SDK's own APIfy path, so the golden file shows the
// wire shape PostHog receives rather than the Go struct. Volatile SDK-added fields ($lib
// version, OS, Go version) are stripped: they change with the toolchain and say nothing
// about the Bucketeer contract.
func goldenCapture(t *testing.T, capture posthogsdk.Capture) []byte {
	t.Helper()
	apified := capture.APIfy()
	raw, err := json.Marshal(apified)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	if props, ok := decoded["properties"].(map[string]any); ok {
		for _, volatile := range []string{
			"$lib", "$lib_version", "$os", "$os_version", "$go_version", "$is_server",
		} {
			delete(props, volatile)
		}
	}

	normalized, err := json.Marshal(decoded)
	require.NoError(t, err)
	var indented bytes.Buffer
	require.NoError(t, json.Indent(&indented, normalized, "", "  "))
	indented.WriteString("\n")
	return indented.Bytes()
}

func assertGolden(t *testing.T, name string, capture posthogsdk.Capture) {
	t.Helper()
	path := filepath.Join(goldenDir, name)
	want := goldenCapture(t, capture)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(goldenDir, 0o755))
		require.NoError(t, os.WriteFile(path, want, 0o644))
		return
	}
	got, err := os.ReadFile(path)
	require.NoError(t, err, "golden %s is missing; run go test -run TestRuntimeEventGolden -update-golden", name)
	var gotAny, wantAny any
	require.NoError(t, json.Unmarshal(got, &gotAny))
	require.NoError(t, json.Unmarshal(want, &wantAny))
	assert.Equal(t, wantAny, gotAny,
		"the %s event contract changed. Property names are part of contract v1, so a rename "+
			"is breaking for every query built on it.", name)
}

// TestRuntimeEventGolden pins the runtime event contract: names, property names, the
// preserved UUID and timestamp, and the absence of unallowlisted user data.
//
// All values are invented. Nothing here comes from production or a customer.
func TestRuntimeEventGolden(t *testing.T) {
	t.Parallel()

	privacy := PrivacyConfig{
		UserAttributeAllowlist: []string{"plan", "tenant_id"},
		MetadataAllowlist:      []string{"region"},
		GroupMappings:          []GroupMapping{{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"}},
	}

	t.Run("bucketeer_feature_evaluated.json", func(t *testing.T) {
		capture, err := MapEvaluationEvent(
			"3f6b0c6e-6d1f-4d5f-9f0a-3f6b0c6e6d1f",
			"env-0000000000",
			&eventproto.EvaluationEvent{
				Timestamp:      1700000000,
				FeatureId:      "feature-alpha",
				FeatureVersion: 3,
				UserId:         "user-0000000000",
				VariationId:    "variation-on",
				Reason:         &featureproto.Reason{Type: featureproto.Reason_RULE, RuleId: "rule-0000000000"},
				Tag:            "web",
				SourceId:       eventproto.SourceId_GO_SERVER,
				SdkVersion:     "1.0.0",
				User: &userproto.User{
					Id: "user-0000000000",
					Data: map[string]string{
						"plan":      "pro",
						"tenant_id": "tenant-0000000000",
						// Not allowlisted, so it must not appear in the golden file.
						"email": "person@example.com",
					},
				},
				Metadata: map[string]string{"region": "eu", "trace": "not-allowlisted"},
			},
			privacy,
			nil,
		)
		require.NoError(t, err)
		assertGolden(t, "bucketeer_feature_evaluated.json", capture)
	})

	t.Run("bucketeer_goal_reached.json", func(t *testing.T) {
		capture, err := MapGoalEvent(
			"7c9e6679-7425-40de-944b-e07fc1f90ae7",
			"env-0000000000",
			&eventproto.GoalEvent{
				Timestamp:  1700000500,
				GoalId:     "goal-checkout-complete",
				UserId:     "user-0000000000",
				Value:      42.5,
				Tag:        "web",
				SourceId:   eventproto.SourceId_GO_SERVER,
				SdkVersion: "1.0.0",
				User: &userproto.User{
					Id:   "user-0000000000",
					Data: map[string]string{"plan": "pro", "tenant_id": "tenant-0000000000"},
				},
				Metadata: map[string]string{"region": "eu"},
				// Deprecated and never exported; present here to prove that.
				Evaluations: []*featureproto.Evaluation{{Id: "eval-1", FeatureId: "feature-alpha"}},
			},
			privacy,
			nil,
		)
		require.NoError(t, err)
		assertGolden(t, "bucketeer_goal_reached.json", capture)
	})
}

// The golden files are the contract a query is written against, so an accidental leak of
// non-allowlisted data would otherwise be invisible until it reached PostHog.
func TestGoldenFilesCarryNoUnallowlistedData(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"bucketeer_feature_evaluated.json", "bucketeer_goal_reached.json"} {
		raw, err := os.ReadFile(filepath.Join(goldenDir, name))
		require.NoError(t, err)
		body := string(raw)
		assert.NotContains(t, body, "person@example.com", name)
		assert.NotContains(t, body, "not-allowlisted", name)
		assert.NotContains(t, body, "$identify", name)
		assert.NotContains(t, body, "evaluations", name)
	}
}
