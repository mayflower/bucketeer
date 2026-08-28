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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeKeyFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestDisabledConfigNeedsNothing(t *testing.T) {
	t.Parallel()
	// Absence of configuration is how the integration stays off.
	config := &Config{}
	assert.NoError(t, config.Validate())
}

func TestEnabledConfigRequiresEndpointAndKeyFile(t *testing.T) {
	t.Parallel()
	config := &Config{Enabled: true}
	assert.ErrorIs(t, config.Validate(), ErrEndpointRequired)

	config = &Config{Enabled: true, Endpoint: "https://eu.i.posthog.com"}
	assert.ErrorIs(t, config.Validate(), ErrAPIKeyFileMissing)
}

func TestPlaintextEndpointNeedsAnExplicitOptIn(t *testing.T) {
	t.Parallel()
	// The API key rides on every request, so http must be a deliberate choice.
	config := &Config{
		Enabled:           true,
		Endpoint:          "http://posthog.internal:8000",
		ProjectAPIKeyFile: writeKeyFile(t, "phc_key"),
	}
	assert.ErrorIs(t, config.Validate(), ErrInsecureEndpoint)

	config.AllowInsecureEndpoint = true
	assert.NoError(t, config.Validate())
}

func TestUnknownCaptureModeIsRejected(t *testing.T) {
	t.Parallel()
	config := &Config{
		Enabled:           true,
		Endpoint:          "https://eu.i.posthog.com",
		ProjectAPIKeyFile: writeKeyFile(t, "phc_key"),
		CaptureMode:       "not-a-mode",
	}
	assert.ErrorIs(t, config.Validate(), ErrUnknownCaptureMode)
}

func TestDefaultsAreBounded(t *testing.T) {
	t.Parallel()
	// Every queue and timeout must have a bound; an unbounded queue would turn a
	// PostHog outage into subscriber memory growth.
	config := &Config{}
	require.NoError(t, config.Validate())
	assert.Equal(t, CaptureModeLegacy, config.CaptureMode)
	assert.Positive(t, config.BatchSize)
	assert.Positive(t, config.MaxQueueSize)
	assert.Positive(t, config.FlushInterval())
	assert.Positive(t, config.DeliveryTimeout())
	assert.Positive(t, config.BatchUploadTimeout())
	assert.Positive(t, config.ShutdownTimeout())
	assert.Positive(t, config.Privacy.MaxValueLength)
	assert.Positive(t, config.Privacy.MaxAttributes)
}

func TestGroupMappingMustUseAnAllowlistedAttribute(t *testing.T) {
	t.Parallel()
	config := &Config{
		Enabled:           true,
		Endpoint:          "https://eu.i.posthog.com",
		ProjectAPIKeyFile: writeKeyFile(t, "phc_key"),
		Privacy: PrivacyConfig{
			GroupMappings: []GroupMapping{{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"}},
		},
	}
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userAttributeAllowlist")

	config.Privacy.UserAttributeAllowlist = []string{"tenant_id"}
	assert.NoError(t, config.Validate())
}

func TestReadAPIKeyTrimsAndRejectsEmpty(t *testing.T) {
	t.Parallel()
	config := &Config{ProjectAPIKeyFile: writeKeyFile(t, "  phc_key\n")}
	key, err := config.ReadAPIKey()
	require.NoError(t, err)
	assert.Equal(t, "phc_key", key)

	config = &Config{ProjectAPIKeyFile: writeKeyFile(t, "   \n")}
	_, err = config.ReadAPIKey()
	assert.ErrorIs(t, err, ErrAPIKeyEmpty)
}

func TestReadAPIKeyErrorDoesNotLeakContents(t *testing.T) {
	t.Parallel()
	config := &Config{ProjectAPIKeyFile: "/nonexistent/path/key"}
	_, err := config.ReadAPIKey()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "phc_")
}

func TestConfigJSONCarriesNoAPIKeyField(t *testing.T) {
	t.Parallel()
	// The key must be mountable from a Secret, so it must not be expressible in the
	// ConfigMap-backed processor config at all.
	encoded, err := json.Marshal(&Config{Enabled: true, Endpoint: "https://eu.i.posthog.com"})
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "apiKey\"")
	assert.NotContains(t, string(encoded), "projectApiKey\"")
	assert.Contains(t, string(encoded), "projectApiKeyFile")
}
