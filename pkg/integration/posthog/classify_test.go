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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	posthogsdk "github.com/posthog/posthog-go"
)

func TestClassify(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc     string
		err      error
		expected Classification
	}{
		{
			desc:     "delivered",
			err:      nil,
			expected: ClassificationDelivered,
		},
		{
			// The SDK never accepted the event, so nothing was sent.
			desc:     "queue full is retryable",
			err:      posthogsdk.ErrQueueFull,
			expected: ClassificationRetryable,
		},
		{
			// The event may still be in flight; the stable UUID makes a resend safe.
			desc:     "delivery timeout is retryable",
			err:      context.DeadlineExceeded,
			expected: ClassificationRetryable,
		},
		{
			desc:     "shutdown cancellation is retryable",
			err:      context.Canceled,
			expected: ClassificationRetryable,
		},
		{
			desc:     "wrapped queue full is still retryable",
			err:      fmt.Errorf("enqueue: %w", posthogsdk.ErrQueueFull),
			expected: ClassificationRetryable,
		},
		{
			// An operator can rotate the key; the broker holding the event is what
			// makes that recoverable.
			desc:     "401 is retryable",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 401},
			expected: ClassificationRetryable,
		},
		{
			desc:     "403 is retryable",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 403},
			expected: ClassificationRetryable,
		},
		{
			// Too large now means too large on every resend.
			desc:     "413 is terminal",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 413},
			expected: ClassificationTerminal,
		},
		{
			desc:     "400 is terminal",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 400},
			expected: ClassificationTerminal,
		},
		{
			desc:     "429 is retryable",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 429},
			expected: ClassificationRetryable,
		},
		{
			desc:     "500 is retryable",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 500},
			expected: ClassificationRetryable,
		},
		{
			desc:     "503 is retryable",
			err:      &posthogsdk.CaptureRequestError{StatusCode: 503},
			expected: ClassificationRetryable,
		},
		{
			// An unexplained error redelivers rather than silently discards.
			desc:     "unknown error is retryable",
			err:      errors.New("connection refused"),
			expected: ClassificationRetryable,
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			assert.Equal(t, p.expected, Classify(p.err), p.desc)
		})
	}
}

func TestPerEventErrorIsTerminal(t *testing.T) {
	t.Parallel()
	// analytics-v1 rejects one event within an otherwise fine batch; resending the same
	// malformed payload cannot start working.
	err := &posthogsdk.CaptureEventError{}
	assert.Equal(t, ClassificationTerminal, Classify(err))
}

// legacyHTTPError mimics the SDK's unexported httpError, whose Error() is "<status> <text>".
// The default capture mode reports every upload failure this way, so a classifier that only
// matches the typed analytics-v1 errors treats a permanent rejection as retryable and the
// broker redelivers it forever.
type legacyHTTPError struct{ message string }

func (e legacyHTTPError) Error() string { return e.message }

func TestClassifyLegacyCaptureModeErrors(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc     string
		message  string
		expected Classification
	}{
		{"400 is terminal", "400 Bad Request", ClassificationTerminal},
		{"413 is terminal", "413 Request Entity Too Large", ClassificationTerminal},
		{"401 is retryable so a rotated key recovers", "401 Unauthorized", ClassificationRetryable},
		{"429 is retryable", "429 Too Many Requests", ClassificationRetryable},
		{"503 is retryable", "503 Service Unavailable", ClassificationRetryable},
		{"a message with no status stays retryable", "connection reset", ClassificationRetryable},
		{"a non-numeric prefix stays retryable", "err 400", ClassificationRetryable},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			assert.Equal(t, p.expected, Classify(legacyHTTPError{message: p.message}), p.desc)
		})
	}
}

func TestOversizedMessageIsTerminal(t *testing.T) {
	t.Parallel()
	// The SDK refuses it before HTTP, identically on every attempt, so nacking would
	// occupy the subscription forever.
	assert.Equal(t, ClassificationTerminal, Classify(posthogsdk.ErrMessageTooBig))
}
