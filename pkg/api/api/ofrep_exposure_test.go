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
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	publishermock "github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher/mock"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

func newOFREPExposureTestEvent(t *testing.T) *eventproto.Event {
	t.Helper()
	event, err := newOFREPExposureEvent(
		ofrepTestEnvironmentID,
		&userproto.User{Id: "user-1"},
		&featureproto.Evaluation{
			FeatureId:   "flag",
			VariationId: "on",
			Reason:      &featureproto.Reason{Type: featureproto.Reason_DEFAULT},
		},
	)
	require.NoError(t, err)
	return event
}

func ofrepDropped(reason string) float64 {
	return testutil.ToFloat64(ofrepExposureDroppedCounter.WithLabelValues(reason))
}

func TestEnqueueOFREPExposureDropsWhenQueueFull(t *testing.T) {
	service := newGrpcGatewayServiceWithMock(t, gomock.NewController(t))
	startOFREPExposureWorkersForTest(t, service, 1, 1)
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(ctx context.Context, _ any) error {
		once.Do(func() { close(firstStarted) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Times(2)

	service.enqueueOFREPExposure(newOFREPExposureTestEvent(t)) // taken by the single worker
	awaitOFREP(t, firstStarted)
	service.enqueueOFREPExposure(newOFREPExposureTestEvent(t)) // fills the one-slot queue
	before := ofrepDropped(ofrepExposureDropQueueFull)
	service.enqueueOFREPExposure(newOFREPExposureTestEvent(t)) // returns at once, dropped
	assert.Equal(t, before+1, ofrepDropped(ofrepExposureDropQueueFull))
	close(release)
}

func TestEnqueueOFREPExposureDropsWithoutWorkers(t *testing.T) {
	service := newGrpcGatewayServiceWithMock(t, gomock.NewController(t))
	before := ofrepDropped(ofrepExposureDropQueueClosed)
	service.enqueueOFREPExposure(newOFREPExposureTestEvent(t))
	assert.Equal(t, before+1, ofrepDropped(ofrepExposureDropQueueClosed))
}

func TestShutdownOFREPExposuresDrainsQueue(t *testing.T) {
	service := newGrpcGatewayServiceWithMock(t, gomock.NewController(t))
	service.startOFREPExposureWorkers(1, 8)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(
		gomock.Any(), gomock.Any(),
	).Return(nil).Times(3)
	for i := 0; i < 3; i++ {
		service.enqueueOFREPExposure(newOFREPExposureTestEvent(t))
	}

	ctx, cancel := context.WithTimeout(context.Background(), ofrepTestTimeout)
	defer cancel()
	service.ShutdownOFREPExposures(ctx)

	// Repeated shutdown is a no-op and later events are dropped as closed.
	service.ShutdownOFREPExposures(ctx)
	before := ofrepDropped(ofrepExposureDropQueueClosed)
	service.enqueueOFREPExposure(newOFREPExposureTestEvent(t))
	assert.Equal(t, before+1, ofrepDropped(ofrepExposureDropQueueClosed))
}

func TestShutdownOFREPExposuresAbandonsStalledPublisher(t *testing.T) {
	service := newGrpcGatewayServiceWithMock(t, gomock.NewController(t))
	service.startOFREPExposureWorkers(1, 8)
	started := make(chan struct{})
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(ctx context.Context, _ any) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}).Times(1)
	for i := 0; i < 3; i++ {
		service.enqueueOFREPExposure(newOFREPExposureTestEvent(t))
	}
	awaitOFREP(t, started)

	before := ofrepDropped(ofrepExposureDropShutdown)
	shutdownCtx, expireBudget := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		service.ShutdownOFREPExposures(shutdownCtx)
		close(done)
	}()
	expireBudget()
	awaitOFREP(t, done)

	// The stalled publication was attempted once; the two queued behind it
	// were abandoned and counted, and the worker has exited.
	assert.Equal(t, before+2, ofrepDropped(ofrepExposureDropShutdown))
}
