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
	"sync"

	posthogsdk "github.com/posthog/posthog-go"
)

// DeliveryResult is the outcome of one event's trip to PostHog.
type DeliveryResult struct {
	UUID string
	Err  error
}

// DeliveryTracker correlates SDK callbacks back to the Bucketeer event UUID that
// produced them, so a broker message is acked only once its event is confirmed
// delivered. Enqueue alone proves nothing: it only means the SDK accepted the event
// into its in-memory queue.
//
// It satisfies posthog.Callback. The SDK invokes callbacks from its own goroutines, so
// Success and Failure must never block: both send on a buffered channel and fall through
// if nobody is waiting, which happens whenever a processor already timed out.
type DeliveryTracker struct {
	mu      sync.Mutex
	pending map[string]chan DeliveryResult
}

func NewDeliveryTracker() *DeliveryTracker {
	return &DeliveryTracker{pending: make(map[string]chan DeliveryResult)}
}

// Watch registers interest in one UUID before it is enqueued. The returned function
// releases the registration and must always be called, or the map grows without bound.
func (t *DeliveryTracker) Watch(uuid string) (<-chan DeliveryResult, func()) {
	// Buffered so a callback can complete even when the waiter has already given up.
	ch := make(chan DeliveryResult, 1)
	t.mu.Lock()
	t.pending[uuid] = ch
	t.mu.Unlock()
	return ch, func() {
		t.mu.Lock()
		delete(t.pending, uuid)
		t.mu.Unlock()
	}
}

// Pending reports how many events are awaiting a callback. Used for the inflight metric.
func (t *DeliveryTracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func (t *DeliveryTracker) Success(msg posthogsdk.APIMessage) {
	t.resolve(uuidOf(msg), nil)
}

func (t *DeliveryTracker) Failure(msg posthogsdk.APIMessage, err error) {
	t.resolve(uuidOf(msg), err)
}

func (t *DeliveryTracker) resolve(uuid string, err error) {
	if uuid == "" {
		return
	}
	t.mu.Lock()
	ch, ok := t.pending[uuid]
	t.mu.Unlock()
	if !ok {
		// The waiter timed out and released its registration, or the SDK delivered a
		// callback twice. Either way there is nobody to tell, and dropping it here is
		// what keeps a late callback from panicking on a closed channel.
		return
	}
	select {
	case ch <- DeliveryResult{UUID: uuid, Err: err}:
	default:
		// Already resolved; never block an SDK goroutine.
	}
}

// Await blocks until the event's callback arrives, the delivery timeout expires, or the
// context is cancelled. A timeout is reported as an error so the caller nacks: the event
// may still be in flight, and redelivery is safe because the UUID is stable.
func Await(ctx context.Context, ch <-chan DeliveryResult) error {
	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func uuidOf(msg posthogsdk.APIMessage) string {
	if capture, ok := msg.(posthogsdk.CaptureInApi); ok {
		return capture.Uuid
	}
	return ""
}
