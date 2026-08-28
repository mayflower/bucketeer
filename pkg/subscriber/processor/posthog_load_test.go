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
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

// uuidFor builds a distinct, well-formed UUID per event. The SDK replaces an invalid
// UUID with a generated one, which would break callback correlation.
func uuidFor(i int) string {
	const hex = "0123456789abcdef"
	digits := make([]byte, 12)
	for d := range digits {
		digits[d] = hex[(i>>(4*d))&0xf]
	}
	return "3f6b0c6e-6d1f-4d5f-9f0a-" + string(digits)
}

// TestExporterSustains10kEvents is the load run the production-readiness audit requires.
// It measures throughput, memory growth and goroutine stability across 10,000 events
// against a local endpoint, and asserts every message was acked.
//
// Skipped under -short: it takes seconds, not milliseconds.
func TestExporterSustains10kEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("load run; skipped under -short")
	}

	const eventCount = 10000

	var requests int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	keyPath := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("phc_test_key"), 0o600))
	config := &posthog.Config{
		Enabled:               true,
		Endpoint:              server.URL,
		ProjectAPIKeyFile:     keyPath,
		AllowInsecureEndpoint: true,
		ExportEvaluations:     true,
		// Batching is what makes the per-event ack wait affordable: many events ride
		// one upload, so throughput is not one round trip per event.
		BatchSize:          100,
		FlushIntervalSec:   1,
		DeliveryTimeoutSec: 30,
		MaxQueueSize:       10000,
	}
	client, err := posthog.NewClient(config, zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	exporter := NewPostHogEvaluationExporter(client, config, zap.NewNop()).(*postHogEvaluationExporter)

	inner, err := anypb.New(&eventproto.EvaluationEvent{
		Timestamp:      time.Now().Unix(),
		FeatureId:      "feature-alpha",
		FeatureVersion: 1,
		UserId:         "user-1",
		VariationId:    "variation-on",
		Reason:         &featureproto.Reason{Type: featureproto.Reason_DEFAULT},
		SourceId:       eventproto.SourceId_GO_SERVER,
	})
	require.NoError(t, err)

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	// Drive the real processor loop: messages arrive on a channel and are batched.
	msgChan := make(chan *puller.Message, 256)
	states := make([]*ackState, eventCount)
	loopDone := make(chan struct{})

	start := time.Now()
	go func() {
		_ = exporter.Process(context.Background(), msgChan)
		close(loopDone)
	}()
	for i := 0; i < eventCount; i++ {
		data, err := proto.Marshal(&eventproto.Event{
			Id:            uuidFor(i),
			EnvironmentId: "env-1",
			Event:         inner,
		})
		require.NoError(t, err)
		state := &ackState{}
		states[i] = state
		msgChan <- &puller.Message{
			Attributes: map[string]string{"id": uuidFor(i)},
			Data:       data,
			Ack:        state.ack,
			Nack:       state.nack,
		}
	}
	close(msgChan)
	<-loopDone
	elapsed := time.Since(start)

	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	acked, nacked := 0, 0
	for _, state := range states {
		if state == nil {
			continue
		}
		a, n := state.get()
		if a {
			acked++
		}
		if n {
			nacked++
		}
	}

	heapGrowthMB := (float64(memAfter.HeapAlloc) - float64(memBefore.HeapAlloc)) / (1 << 20)
	t.Logf(
		"10k events in %s (%.0f events/sec), %d uploads, heap growth %.1f MiB, goroutines %d -> %d",
		elapsed.Round(time.Millisecond),
		float64(eventCount)/elapsed.Seconds(),
		atomic.LoadInt64(&requests),
		heapGrowthMB,
		goroutinesBefore,
		runtime.NumGoroutine(),
	)

	assert.Equal(t, eventCount, acked, "every event should be acked after confirmed delivery")
	assert.Zero(t, nacked)
	// Nothing may be left waiting once every message has been settled; a leak here
	// would mean the tracker's pending map grows without bound.
	assert.Zero(t, client.Pending())
	// Batching must actually happen, or each event would cost its own upload.
	assert.Less(t, atomic.LoadInt64(&requests), int64(eventCount))
}
