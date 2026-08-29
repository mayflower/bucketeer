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
	"encoding/json"
	"fmt"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
)

// ParsePostHogConfig decodes the exporter's entry from the processors config file.
//
// One entry configures both exporters: they share a destination, a credential and a
// privacy policy, and each is switched on by its own exportEvaluations / exportGoals flag.
// There is deliberately no second entry for the goal exporter — a separate block would be
// read by nothing, so an endpoint or allowlist set there would be silently ignored.
//
// A missing or empty entry yields a disabled exporter, which is how the integration
// stays off until an operator configures it. A malformed entry is an error rather than a
// silent default: an exporter that quietly does nothing is worse than a failed startup.
func ParsePostHogConfig(raw interface{}) (*posthog.Config, error) {
	config := &posthog.Config{}
	if raw == nil {
		return config, nil
	}
	configMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("posthog: processor config must be an object, got %T", raw)
	}
	bytes, err := json.Marshal(configMap)
	if err != nil {
		return nil, fmt.Errorf("posthog: could not re-encode processor config: %w", err)
	}
	if err := json.Unmarshal(bytes, config); err != nil {
		return nil, fmt.Errorf("posthog: could not decode processor config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}
