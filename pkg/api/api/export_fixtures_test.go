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

package api

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	auditlogproto "github.com/bucketeer-io/bucketeer/v2/proto/auditlog"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	gwproto "github.com/bucketeer-io/bucketeer/v2/proto/gateway"
)

var updateFixtures = flag.Bool("update", false, "rewrite the analytics export contract fixtures")

const fixtureDir = "../../../test/fixtures/analytics_export_v1"

// marshalFixture serializes exactly as the gateway does: protojson with unpopulated
// fields emitted, which is why an exhausted cursor appears as "" rather than being
// omitted. A connector relies on that, so the fixtures must show it.
func marshalFixture(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(msg)
	require.NoError(t, err)
	var indented bytes.Buffer
	require.NoError(t, json.Indent(&indented, raw, "", "  "))
	indented.WriteString("\n")
	return indented.Bytes()
}

func assertFixture(t *testing.T, name string, msg proto.Message) {
	t.Helper()
	path := filepath.Join(fixtureDir, name)
	want := marshalFixture(t, msg)
	if *updateFixtures {
		require.NoError(t, os.WriteFile(path, want, 0o644))
		return
	}
	got, err := os.ReadFile(path)
	require.NoError(t, err, "fixture %s is missing; run go test -run TestAnalyticsExportFixtures -update", name)
	// Compared as parsed JSON so formatting alone cannot fail the gate.
	var gotAny, wantAny any
	require.NoError(t, json.Unmarshal(got, &gotAny))
	require.NoError(t, json.Unmarshal(want, &wantAny))
	assert.Equal(t, wantAny, gotAny,
		"fixture %s no longer matches the server response shape. If this change is intended, "+
			"regenerate with -update and coordinate a connector change.", name)
}

// TestAnalyticsExportFixtures pins the wire shape a warehouse connector consumes. It
// serializes the server's own response types, so a field added, renamed or removed on
// the gateway shows up here rather than in a connector's production sync.
//
// Every value is invented. No production or customer data belongs in these files.
func TestAnalyticsExportFixtures(t *testing.T) {
	t.Parallel()

	t.Run("context.json", func(t *testing.T) {
		assertFixture(t, "context.json", &gwproto.GetExportContextResponse{
			ContractVersion:    "1",
			CredentialScope:    "environment",
			OrganizationId:     "org-0000000000",
			ProjectId:          "prj-0000000000",
			ProjectUrlCode:     "example-project",
			EnvironmentId:      "env-0000000000",
			EnvironmentName:    "Production",
			EnvironmentUrlCode: "production",
			Capabilities: []string{
				"feature_flags", "segments", "experiments", "goals", "audit_logs", "code_references",
			},
		})
	})

	// Two pages prove the envelope a connector walks: a non-empty cursor continues, and
	// an empty cursor ends the walk even though the page returned rows.
	t.Run("feature_flags_page_1.json", func(t *testing.T) {
		assertFixture(t, "feature_flags_page_1.json", &gwproto.ListFeaturesResponse{
			Features: []*featureproto.Feature{
				{
					Id:      "feature-alpha",
					Name:    "Alpha rollout",
					Enabled: true,
					Version: 3,
					Variations: []*featureproto.Variation{
						{Id: "variation-on", Value: "true", Name: "On"},
						{Id: "variation-off", Value: "false", Name: "Off"},
					},
					OffVariation: "variation-off",
					Tags:         []string{"example"},
					CreatedAt:    1700000000,
					UpdatedAt:    1700000100,
				},
			},
			Cursor:     "1",
			TotalCount: 2,
		})
	})

	t.Run("feature_flags_page_2.json", func(t *testing.T) {
		assertFixture(t, "feature_flags_page_2.json", &gwproto.ListFeaturesResponse{
			Features: []*featureproto.Feature{
				{
					Id:        "feature-beta",
					Name:      "Beta rollout",
					Enabled:   false,
					Archived:  true,
					Version:   1,
					CreatedAt: 1700000200,
					UpdatedAt: 1700000300,
				},
			},
			// Empty, not absent: the gateway emits unpopulated fields.
			Cursor:     "",
			TotalCount: 2,
		})
	})

	t.Run("segments.json", func(t *testing.T) {
		assertFixture(t, "segments.json", &gwproto.ListSegmentsResponse{
			Segments: []*featureproto.Segment{
				{
					Id:                "segment-early-access",
					Name:              "Early access",
					Description:       "Accounts opted into early access",
					IncludedUserCount: 12,
					CreatedAt:         1700000000,
					UpdatedAt:         1700000100,
				},
			},
			Cursor:     "",
			TotalCount: 1,
		})
	})

	t.Run("experiments.json", func(t *testing.T) {
		assertFixture(t, "experiments.json", &gwproto.ListExperimentsResponse{
			Experiments: []*experimentproto.Experiment{
				{
					Id:              "experiment-alpha",
					Name:            "Alpha checkout test",
					FeatureId:       "feature-alpha",
					FeatureVersion:  3,
					GoalIds:         []string{"goal-checkout-complete"},
					BaseVariationId: "variation-off",
					StartAt:         1700000000,
					StopAt:          1700600000,
					CreatedAt:       1700000000,
					UpdatedAt:       1700000100,
				},
			},
			Cursor:     "",
			TotalCount: 1,
		})
	})

	t.Run("goals.json", func(t *testing.T) {
		assertFixture(t, "goals.json", &gwproto.ListGoalsResponse{
			Goals: []*experimentproto.Goal{
				{
					Id:          "goal-checkout-complete",
					Name:        "Checkout complete",
					Description: "Reached the order confirmation step",
					CreatedAt:   1700000000,
					UpdatedAt:   1700000100,
				},
			},
			Cursor:     "",
			TotalCount: 1,
		})
	})

	t.Run("audit_logs.json", func(t *testing.T) {
		assertFixture(t, "audit_logs.json", &gwproto.ListAuditLogsResponse{
			AuditLogs: []*auditlogproto.AuditLog{
				{
					Id:        "auditlog-0000000000",
					Timestamp: 1700000100,
					EntityId:  "feature-alpha",
				},
			},
			Cursor:     "",
			TotalCount: 1,
		})
	})

	t.Run("code_references.json", func(t *testing.T) {
		assertFixture(t, "code_references.json", &gwproto.ListCodeReferencesResponse{
			CodeReferences: []*coderefproto.CodeReference{
				{
					Id:               "coderef-0000000000",
					FeatureId:        "feature-alpha",
					FilePath:         "src/checkout/index.ts",
					LineNumber:       42,
					RepositoryName:   "example-app",
					RepositoryOwner:  "example-org",
					RepositoryBranch: "main",
					// The record carries its own environment id. A connector must
					// prefer the authenticated export context over this field.
					EnvironmentId: "env-0000000000",
					CreatedAt:     1700000000,
					UpdatedAt:     1700000100,
				},
			},
			Cursor:     "",
			TotalCount: 1,
		})
	})
}
