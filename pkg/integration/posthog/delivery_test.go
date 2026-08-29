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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	posthogsdk "github.com/posthog/posthog-go"
)

func TestTrackerResolvesSuccessByUUID(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	ch, release := tracker.Watch("uuid-a")
	defer release()

	tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, Await(ctx, ch))
}

func TestTrackerResolvesFailureByUUID(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	ch, release := tracker.Watch("uuid-a")
	defer release()

	sentinel := errors.New("boom")
	tracker.Failure(posthogsdk.CaptureInApi{Uuid: "uuid-a"}, sentinel)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.ErrorIs(t, Await(ctx, ch), sentinel)
}

func TestTrackerDoesNotCrossWireDifferentUUIDs(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	chA, releaseA := tracker.Watch("uuid-a")
	defer releaseA()
	chB, releaseB := tracker.Watch("uuid-b")
	defer releaseB()

	tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-b"})

	select {
	case <-chA:
		t.Fatal("uuid-a resolved from uuid-b's callback")
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, Await(ctx, chB))
}

func TestAwaitReportsTimeout(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	ch, release := tracker.Watch("uuid-a")
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, Await(ctx, ch), context.DeadlineExceeded)
}

func TestCallbackAfterTimeoutIsHarmless(t *testing.T) {
	t.Parallel()
	// The processor gives up, releases its registration, and only then does the SDK
	// deliver the callback. That must not panic or block an SDK goroutine.
	tracker := NewDeliveryTracker()
	_, release := tracker.Watch("uuid-a")
	release()

	done := make(chan struct{})
	go func() {
		tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback blocked after the waiter released")
	}
}

func TestDuplicateCallbackDoesNotBlock(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	ch, release := tracker.Watch("uuid-a")
	defer release()

	done := make(chan struct{})
	go func() {
		tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})
		// A second callback for the same UUID must fall through, not block.
		tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("duplicate callback blocked an SDK goroutine")
	}
	<-ch
}

func TestTrackerIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	const n = 200

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uuid := string(rune('a'+i%26)) + string(rune('0'+i%10))
			ch, release := tracker.Watch(uuid)
			defer release()
			go tracker.Success(posthogsdk.CaptureInApi{Uuid: uuid})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = Await(ctx, ch)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 0, tracker.Pending())
}

func TestReleaseClearsPending(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	_, release := tracker.Watch("uuid-a")
	require.Equal(t, 1, tracker.Pending())
	release()
	assert.Equal(t, 0, tracker.Pending())
}

func TestUnknownAPIMessageIsIgnored(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	// A non-capture APIMessage carries no UUID; resolving must be a no-op, not a panic.
	tracker.Success(struct{}{})
	assert.Equal(t, 0, tracker.Pending())
}

func TestTwoWaitersOnOneUUIDBothResolve(t *testing.T) {
	t.Parallel()
	// A redelivery can arrive while the first attempt is still settling. If the second
	// registration replaced the first, the callback would reach neither and both messages
	// would be nacked despite a successful delivery.
	tracker := NewDeliveryTracker()
	first, releaseFirst := tracker.Watch("uuid-a")
	defer releaseFirst()
	second, releaseSecond := tracker.Watch("uuid-a")
	defer releaseSecond()

	tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, Await(ctx, first))
	assert.NoError(t, Await(ctx, second))
}

func TestReleasingOneWaiterLeavesTheOtherRegistered(t *testing.T) {
	t.Parallel()
	tracker := NewDeliveryTracker()
	_, releaseFirst := tracker.Watch("uuid-a")
	second, releaseSecond := tracker.Watch("uuid-a")
	defer releaseSecond()

	// The first gives up; the second must still be told the outcome.
	releaseFirst()
	tracker.Success(posthogsdk.CaptureInApi{Uuid: "uuid-a"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, Await(ctx, second))
}
