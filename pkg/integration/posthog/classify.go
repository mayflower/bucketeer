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
	"strconv"
	"strings"

	posthogsdk "github.com/posthog/posthog-go"
)

// Classification decides what happens to the broker message behind an event.
type Classification string

const (
	// ClassificationDelivered — the event reached PostHog. Ack.
	ClassificationDelivered Classification = "delivered"
	// ClassificationRetryable — the event may still land on a later attempt. Nack, and
	// let broker redelivery be the next retry layer. The SDK already retries uploads,
	// so this must not be wrapped in a second retry loop.
	ClassificationRetryable Classification = "retryable"
	// ClassificationTerminal — this event will never be accepted, however often it is
	// resent. Ack so it stops consuming the subscription, and count it as dropped.
	ClassificationTerminal Classification = "terminal"
)

// Classify maps a delivery outcome to what should happen to the broker message.
func Classify(err error) Classification {
	if err == nil {
		return ClassificationDelivered
	}
	// A full queue means the SDK never accepted the event, so nothing was sent.
	if errors.Is(err, posthogsdk.ErrQueueFull) {
		return ClassificationRetryable
	}
	// The SDK refuses an oversized message before it ever reaches HTTP. The same event
	// will be refused identically on every redelivery, so nacking it would occupy the
	// subscription forever. This is the client-side twin of a 413.
	if errors.Is(err, posthogsdk.ErrMessageTooBig) {
		return ClassificationTerminal
	}
	// A timeout or shutdown leaves the event's fate unknown. Redelivery is safe because
	// the UUID and timestamp are preserved, so PostHog dedupes a double send.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ClassificationRetryable
	}

	// analytics-v1 reports per-event outcomes. A rejected event is terminal: resending
	// an identically malformed payload cannot start working.
	var eventErr *posthogsdk.CaptureEventError
	if errors.As(err, &eventErr) {
		return ClassificationTerminal
	}

	// A transport-level failure carries the HTTP status, which decides retryability.
	// analytics-v1 reports it as a typed error.
	var reqErr *posthogsdk.CaptureRequestError
	if errors.As(err, &reqErr) {
		return classifyStatus(reqErr.StatusCode)
	}

	// The default legacy capture mode reports an upload failure as the SDK's own
	// unexported httpError, so no errors.As can reach it. Its Error() is "<status> <text>",
	// which is the only exported surface carrying the status. Without this, a 400 or 413
	// in the default mode falls through to retryable and the broker redelivers a message
	// PostHog will never accept, forever.
	if status, ok := statusFromLegacyError(err); ok {
		return classifyStatus(status)
	}

	// An unrecognized error is treated as retryable: redelivering a duplicate is
	// cheaper than silently discarding an event we cannot explain.
	return ClassificationRetryable
}

// statusFromLegacyError reads the HTTP status off the SDK's unexported httpError, whose
// Error() is formatted "%d %s". Matching on the message is unpleasant, but the type is not
// exported and the status is what decides whether a redelivery can ever succeed.
func statusFromLegacyError(err error) (int, bool) {
	message := err.Error()
	end := strings.IndexByte(message, ' ')
	if end <= 0 {
		return 0, false
	}
	status, convErr := strconv.Atoi(message[:end])
	if convErr != nil || status < 100 || status > 599 {
		return 0, false
	}
	return status, true
}

func classifyStatus(status int) Classification {
	switch {
	case status == 401 || status == 403:
		// Bad credentials will not fix themselves by resending, but the operator can
		// rotate the key, and the broker holding the event is what makes that
		// recoverable. Retryable on purpose.
		return ClassificationRetryable
	case status == 413:
		// The payload is too large for this instance and will be too large next time.
		return ClassificationTerminal
	case status == 429 || status >= 500:
		return ClassificationRetryable
	case status >= 400:
		// Any other 4xx is a malformed request for this event.
		return ClassificationTerminal
	default:
		return ClassificationRetryable
	}
}
