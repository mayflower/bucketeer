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

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

const (
	// ofrepSmokePythonEnv names a Python interpreter that has
	// tools/ofrep/requirements.txt installed. The smoke test is skipped
	// without it so ordinary e2e runs do not need Python.
	ofrepSmokePythonEnv = "BUCKETEER_OFREP_SMOKE_PYTHON"
	ofrepSmokeScript    = "../../../tools/ofrep/ofrep_smoke_test.py"
	ofrepSmokeTimeout   = 3 * time.Minute
)

// ofrepSmokeFixtures is the contract shared with tools/ofrep/ofrep_smoke_test.py.
type ofrepSmokeFixtures struct {
	Boolean    ofrepBooleanFixture `json:"boolean"`
	Integer    ofrepValueFixture   `json:"integer"`
	Object     ofrepValueFixture   `json:"object"`
	MissingKey string              `json:"missing_key"`
}

type ofrepBooleanFixture struct {
	Key          string            `json:"key"`
	MatchContext map[string]string `json:"match_context"`
	MatchValue   bool              `json:"match_value"`
	OtherValue   bool              `json:"other_value"`
}

type ofrepValueFixture struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// TestOFREPProviderSmoke provisions fixture flags through the existing e2e
// feature helpers, then runs the community OpenFeature OFREP provider against
// the API gateway via tools/ofrep/ofrep_smoke_test.py.
func TestOFREPProviderSmoke(t *testing.T) {
	python := os.Getenv(ofrepSmokePythonEnv)
	if python == "" {
		t.Skipf("%s is not set; the OFREP provider smoke test was not run", ofrepSmokePythonEnv)
	}
	if *apiKeyServerPath == "" {
		t.Fatal("-api-key-server is required for the OFREP provider smoke test")
	}
	serverKey, err := os.ReadFile(*apiKeyServerPath)
	if err != nil {
		t.Fatalf("Failed to read the server API key: %v", err)
	}

	fixtures := createOFREPFixtures(t)
	fixturePath := filepath.Join(t.TempDir(), "fixtures.json")
	encoded, err := json.Marshal(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixturePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	// The Docker Compose recipe serves the API gateway over plain HTTP on
	// port 80; every other documented port is TLS.
	scheme := "https"
	if *gatewayPort == 80 {
		scheme = "http"
	}
	ctx, cancel := context.WithTimeout(context.Background(), ofrepSmokeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, ofrepSmokeScript)
	cmd.Env = append(os.Environ(),
		"BUCKETEER_OFREP_BASE_URL="+fmt.Sprintf("%s://%s:%d", scheme, *gatewayAddr, *gatewayPort),
		"BUCKETEER_SERVER_API_KEY="+strings.TrimSpace(string(serverKey)),
		"BUCKETEER_OFREP_FIXTURES="+fixturePath,
	)
	if *gatewayCert != "" {
		cmd.Env = append(cmd.Env, "REQUESTS_CA_BUNDLE="+*gatewayCert)
	}
	output, err := cmd.CombinedOutput()
	t.Logf("ofrep_smoke_test.py output:\n%s", output)
	if err != nil {
		t.Fatalf("OFREP provider smoke test failed: %v", err)
	}
}

// createOFREPFixtures creates one boolean flag with a targeting rule, one
// number flag, and one object flag, enables them, and refreshes the feature
// cache so the API gateway can evaluate them.
func createOFREPFixtures(t *testing.T) ofrepSmokeFixtures {
	t.Helper()
	client := newFeatureClient(t)
	defer client.Close()

	booleanID := newFeatureID(t, newUUID(t))
	booleanReq := newOFREPFixtureReq(booleanID, featureproto.Feature_BOOLEAN, "false", "true")
	createFeature(t, client, booleanReq)
	// newFixedStrategyRule matches attribute-1 in {value-1,value-2} and
	// attribute-2 in {value-1,value-2}; it selects the second variation.
	addRule(t, booleanID, getFeature(t, booleanID, client).Variations[1].Id, client)
	enableFeature(t, booleanID, client)

	integerID := newFeatureID(t, newUUID(t))
	createFeature(t, client, newOFREPFixtureReq(integerID, featureproto.Feature_NUMBER, "42", "7"))
	enableFeature(t, integerID, client)

	objectID := newFeatureID(t, newUUID(t))
	createFeature(t, client, newOFREPFixtureReq(objectID, featureproto.Feature_JSON, `{"tier":"pro"}`, `{"tier":"free"}`))
	enableFeature(t, objectID, client)

	updateFeatueFlagCache(t)

	return ofrepSmokeFixtures{
		Boolean: ofrepBooleanFixture{
			Key:          booleanID,
			MatchContext: map[string]string{"attribute-1": "value-1", "attribute-2": "value-2"},
			MatchValue:   true,
			OtherValue:   false,
		},
		Integer:    ofrepValueFixture{Key: integerID, Value: 42},
		Object:     ofrepValueFixture{Key: objectID, Value: map[string]string{"tier": "pro"}},
		MissingKey: newFeatureID(t, newUUID(t)),
	}
}

// newOFREPFixtureReq builds a create request whose first variation is the
// default on-variation and whose second variation is the off-variation.
func newOFREPFixtureReq(
	featureID string,
	variationType featureproto.Feature_VariationType,
	defaultValue, otherValue string,
) *featureproto.CreateFeatureRequest {
	return &featureproto.CreateFeatureRequest{
		Id:            featureID,
		Name:          featureID,
		Description:   "e2e-test-ofrep-smoke-fixture",
		VariationType: variationType,
		Variations: []*featureproto.Variation{
			{Value: defaultValue, Name: "default"},
			{Value: otherValue, Name: "other"},
		},
		Tags:                     []string{"e2e-test-tag-1"},
		DefaultOnVariationIndex:  &wrapperspb.Int32Value{Value: 0},
		DefaultOffVariationIndex: &wrapperspb.Int32Value{Value: 1},
		EnvironmentId:            *environmentID,
	}
}
