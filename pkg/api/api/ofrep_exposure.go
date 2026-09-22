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
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

const (
	// ofrepExposureQueueSize bounds the number of exposure events waiting
	// for a worker. Requests never wait for space; overflow is dropped.
	ofrepExposureQueueSize = 1024
	// ofrepExposureWorkers is the number of concurrent publications.
	ofrepExposureWorkers = 4
	// ofrepExposurePublishTimeout bounds one publication attempt.
	ofrepExposurePublishTimeout = 250 * time.Millisecond

	ofrepExposureDropQueueFull   = "queue_full"
	ofrepExposureDropQueueClosed = "queue_closed"
	ofrepExposureDropShutdown    = "shutdown"
)

// ofrepExposureQueue hands OFREP exposure events to a fixed set of workers
// that publish them off the request path. Delivery is best effort: enqueueing
// never blocks a request, and an event is dropped and counted when the queue
// is full, not started, or shutting down.
type ofrepExposureQueue struct {
	events  chan *eventproto.Event
	workers sync.WaitGroup
	mu      sync.RWMutex
	closed  bool
	// cancel aborts in-flight and pending publications once the shutdown
	// budget is spent. Workers are bound to this context, never to a request.
	cancel context.CancelFunc
}

func (s *grpcGatewayService) startOFREPExposureWorkers(workers, queueSize int) {
	ctx, cancel := context.WithCancel(context.Background())
	queue := &ofrepExposureQueue{
		events: make(chan *eventproto.Event, queueSize),
		cancel: cancel,
	}
	s.ofrepExposures = queue
	for i := 0; i < workers; i++ {
		queue.workers.Add(1)
		go func() {
			defer queue.workers.Done()
			for event := range queue.events {
				if ctx.Err() != nil {
					ofrepExposureDroppedCounter.WithLabelValues(ofrepExposureDropShutdown).Inc()
					continue
				}
				s.publishOFREPExposure(ctx, event)
			}
		}()
	}
}

// enqueueOFREPExposure hands an event to the workers without waiting for
// broker I/O, worker availability, or queue space.
func (s *grpcGatewayService) enqueueOFREPExposure(event *eventproto.Event) {
	queue := s.ofrepExposures
	if queue == nil {
		ofrepExposureDroppedCounter.WithLabelValues(ofrepExposureDropQueueClosed).Inc()
		return
	}
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	if queue.closed {
		ofrepExposureDroppedCounter.WithLabelValues(ofrepExposureDropQueueClosed).Inc()
		return
	}
	select {
	case queue.events <- event:
	default:
		ofrepExposureDroppedCounter.WithLabelValues(ofrepExposureDropQueueFull).Inc()
	}
}

// ShutdownOFREPExposures stops accepting exposure events, publishes the ones
// already queued until ctx is done, then abandons the rest and joins the
// workers. It returns only after every worker has exited.
func (s *grpcGatewayService) ShutdownOFREPExposures(ctx context.Context) {
	queue := s.ofrepExposures
	if queue == nil {
		return
	}
	queue.mu.Lock()
	if queue.closed {
		queue.mu.Unlock()
		return
	}
	queue.closed = true
	close(queue.events)
	queue.mu.Unlock()

	drained := make(chan struct{})
	go func() {
		queue.workers.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-ctx.Done():
		queue.cancel()
		<-drained
	}
	queue.cancel()
}

func (s *grpcGatewayService) publishOFREPExposure(ctx context.Context, event *eventproto.Event) {
	publishCtx, cancel := context.WithTimeout(ctx, ofrepExposurePublishTimeout)
	defer cancel()
	err := s.evaluationPublisher.Publish(publishCtx, event)
	if err == nil {
		eventCounter.WithLabelValues(callerGatewayService, typeEvaluation, codeOK).Inc()
		return
	}
	code := codeRepeatableError
	if errors.Is(err, publisher.ErrBadMessage) {
		code = codeNonRepeatableError
	}
	eventCounter.WithLabelValues(callerGatewayService, typeEvaluation, code).Inc()
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.logger.Error(
			"Failed to publish OFREP exposure event",
			zap.Error(err),
			zap.String("environmentID", event.EnvironmentId),
			zap.String("eventID", event.Id),
		)
	}
}

// newOFREPExposureEvent builds the complete exposure event at evaluation time
// so the queued job carries no reference to the request.
func newOFREPExposureEvent(
	environmentID string,
	user *userproto.User,
	eval *featureproto.Evaluation,
) (*eventproto.Event, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	wrapped, err := anypb.New(&eventproto.EvaluationEvent{
		Timestamp:      time.Now().Unix(),
		FeatureId:      eval.FeatureId,
		FeatureVersion: eval.FeatureVersion,
		UserId:         user.Id,
		VariationId:    eval.VariationId,
		User:           user,
		Reason:         eval.Reason,
		SourceId:       eventproto.SourceId_OPEN_FEATURE_OFREP,
		SdkVersion:     ofrepVersion,
	})
	if err != nil {
		return nil, err
	}
	return &eventproto.Event{
		Id:            id.String(),
		Event:         wrapped,
		EnvironmentId: environmentID,
	}, nil
}
