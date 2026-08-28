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
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// CaptureModeLegacy is the broadly compatible batch endpoint. It is the default
	// because a self-hosted PostHog may predate analytics-v1.
	CaptureModeLegacy = "legacy"
	// CaptureModeAnalyticsV1 reports per-event outcomes, so a single bad event does not
	// fail its whole batch. Opt in only against a target known to support it.
	CaptureModeAnalyticsV1 = "analytics-v1"

	defaultBatchSize          = 100
	defaultFlushInterval      = 5 * time.Second
	defaultDeliveryTimeout    = 30 * time.Second
	defaultMaxQueueSize       = 10000
	defaultBatchUploadTimeout = 30 * time.Second
	defaultShutdownTimeout    = 15 * time.Second
)

var (
	ErrEndpointRequired   = errors.New("posthog: endpoint is required when the exporter is enabled")
	ErrAPIKeyFileMissing  = errors.New("posthog: projectApiKeyFile is required when the exporter is enabled")
	ErrAPIKeyEmpty        = errors.New("posthog: project API key file is empty")
	ErrInsecureEndpoint   = errors.New("posthog: endpoint must be https unless allowInsecureEndpoint is set")
	ErrUnknownCaptureMode = errors.New("posthog: unknown capture mode")
)

// Config is the non-secret configuration for the PostHog exporters. The project API key
// is never a field here: it is read from ProjectAPIKeyFile so it can be mounted from a
// Secret rather than sitting in a ConfigMap.
type Config struct {
	Enabled bool `json:"enabled"`
	// Endpoint is the PostHog instance to capture into, e.g. https://eu.i.posthog.com.
	Endpoint string `json:"endpoint"`
	// ProjectAPIKeyFile is a path to a mounted file holding the project API key.
	ProjectAPIKeyFile string `json:"projectApiKeyFile"`
	// AllowInsecureEndpoint permits a plaintext http endpoint. Only for a local or
	// in-cluster target; the API key is sent on every request.
	AllowInsecureEndpoint bool `json:"allowInsecureEndpoint"`

	CaptureMode string `json:"captureMode"`

	BatchSize             int `json:"batchSize"`
	FlushIntervalSec      int `json:"flushIntervalSeconds"`
	DeliveryTimeoutSec    int `json:"deliveryTimeoutSeconds"`
	MaxQueueSize          int `json:"maxQueueSize"`
	BatchUploadTimeoutSec int `json:"batchUploadTimeoutSeconds"`
	ShutdownTimeoutSec    int `json:"shutdownTimeoutSeconds"`

	// ExportEvaluations and ExportGoals gate each processor independently while sharing
	// one destination and one privacy policy.
	ExportEvaluations bool `json:"exportEvaluations"`
	ExportGoals       bool `json:"exportGoals"`

	Privacy PrivacyConfig `json:"privacy"`
}

// PrivacyConfig is deny-by-default: an empty allowlist exports nothing.
type PrivacyConfig struct {
	// UserAttributeAllowlist names exact keys of Bucketeer User.data to export, each as
	// bucketeer_user_<key>. No patterns, no regex.
	UserAttributeAllowlist []string `json:"userAttributeAllowlist"`
	// MetadataAllowlist names exact keys of event metadata to export, each as
	// bucketeer_metadata_<key>.
	MetadataAllowlist []string `json:"metadataAllowlist"`
	// GroupMappings map an allowlisted user attribute onto a PostHog group type.
	GroupMappings []GroupMapping `json:"groupMappings"`
	// MaxValueLength caps an exported attribute value. Longer values are dropped, not
	// truncated, so a partial value is never mistaken for the real one.
	MaxValueLength int `json:"maxValueLength"`
	// MaxAttributes caps how many allowlisted attributes may be attached to one event.
	MaxAttributes int `json:"maxAttributes"`
}

type GroupMapping struct {
	PostHogGroupType       string `json:"posthogGroupType"`
	BucketeerUserAttribute string `json:"bucketeerUserAttribute"`
}

func (c *Config) applyDefaults() {
	if c.CaptureMode == "" {
		c.CaptureMode = CaptureModeLegacy
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = defaultMaxQueueSize
	}
	if c.FlushIntervalSec <= 0 {
		c.FlushIntervalSec = int(defaultFlushInterval / time.Second)
	}
	if c.DeliveryTimeoutSec <= 0 {
		c.DeliveryTimeoutSec = int(defaultDeliveryTimeout / time.Second)
	}
	if c.BatchUploadTimeoutSec <= 0 {
		c.BatchUploadTimeoutSec = int(defaultBatchUploadTimeout / time.Second)
	}
	if c.ShutdownTimeoutSec <= 0 {
		c.ShutdownTimeoutSec = int(defaultShutdownTimeout / time.Second)
	}
	if c.Privacy.MaxValueLength <= 0 {
		c.Privacy.MaxValueLength = defaultMaxValueLength
	}
	if c.Privacy.MaxAttributes <= 0 {
		c.Privacy.MaxAttributes = defaultMaxAttributes
	}
}

// Validate applies defaults and reports a configuration that cannot work. It never
// includes the API key or the file's contents in an error.
func (c *Config) Validate() error {
	c.applyDefaults()
	if !c.Enabled {
		return nil
	}
	if c.Endpoint == "" {
		return ErrEndpointRequired
	}
	if !strings.HasPrefix(c.Endpoint, "https://") {
		if !strings.HasPrefix(c.Endpoint, "http://") {
			return fmt.Errorf("posthog: endpoint must be an http(s) URL")
		}
		if !c.AllowInsecureEndpoint {
			return ErrInsecureEndpoint
		}
	}
	if c.ProjectAPIKeyFile == "" {
		return ErrAPIKeyFileMissing
	}
	switch c.CaptureMode {
	case CaptureModeLegacy, CaptureModeAnalyticsV1:
	default:
		return fmt.Errorf("%w: %q", ErrUnknownCaptureMode, c.CaptureMode)
	}
	for _, m := range c.Privacy.GroupMappings {
		if m.PostHogGroupType == "" || m.BucketeerUserAttribute == "" {
			return errors.New("posthog: a group mapping needs both posthogGroupType and bucketeerUserAttribute")
		}
		if !contains(c.Privacy.UserAttributeAllowlist, m.BucketeerUserAttribute) {
			return fmt.Errorf(
				"posthog: group mapping uses user attribute %q, which is not in userAttributeAllowlist",
				m.BucketeerUserAttribute,
			)
		}
	}
	return nil
}

// ReadAPIKey loads the project API key from its mounted file. The key is returned to the
// caller and never logged; a read failure reports the path, never the contents.
func (c *Config) ReadAPIKey() (string, error) {
	raw, err := os.ReadFile(c.ProjectAPIKeyFile)
	if err != nil {
		return "", fmt.Errorf("posthog: could not read project API key file %q: %w", c.ProjectAPIKeyFile, err)
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", ErrAPIKeyEmpty
	}
	return key, nil
}

func (c *Config) FlushInterval() time.Duration {
	return time.Duration(c.FlushIntervalSec) * time.Second
}

func (c *Config) DeliveryTimeout() time.Duration {
	return time.Duration(c.DeliveryTimeoutSec) * time.Second
}

func (c *Config) BatchUploadTimeout() time.Duration {
	return time.Duration(c.BatchUploadTimeoutSec) * time.Second
}

func (c *Config) ShutdownTimeout() time.Duration {
	return time.Duration(c.ShutdownTimeoutSec) * time.Second
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
