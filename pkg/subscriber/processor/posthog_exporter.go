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
	"time"

	posthogsdk "github.com/posthog/posthog-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller/codes"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
)

const (
	dropReasonMalformedOuter   = "malformed_outer_event"
	dropReasonMalformedInner   = "malformed_inner_event"
	dropReasonMissingDistinct  = "missing_distinct_id"
	dropReasonTerminalDelivery = "terminal_delivery_failure"
)

// postHogExporter carries the delivery contract shared by the evaluation and goal
// exporters. Only the event mapping differs between them, so transport, classification,
// ack/nack and metrics live here once.
type postHogExporter struct {
	client    *posthog.Client
	config    *posthog.Config
	eventType string
	// mapEvent turns one decoded outer event into a capture, or reports a terminal
	// reason why it can never become one.
	mapEvent func(outer *eventproto.Event) (posthogsdk.Capture, error)
	logger   *zap.Logger
}

// run is the processor loop. It performs no work on the evaluation or goal request path:
// this runs in the subscriber, so a slow or unreachable PostHog delays export only.
func (e *postHogExporter) run(ctx context.Context, msgChan <-chan *puller.Message) error {
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				e.logger.Info("Message channel closed, stopping")
				return nil
			}
			e.handle(ctx, msg)
		case <-ctx.Done():
			// Shutdown: stop accepting new work. Anything already handed to us has
			// been acked or nacked by handle, so nothing is silently acked.
			e.logger.Info("Context done, stopping")
			return nil
		}
	}
}

func (e *postHogExporter) handle(ctx context.Context, msg *puller.Message) {
	posthogReceivedCounter.WithLabelValues(e.eventType).Inc()

	id := msg.Attributes["id"]
	if id == "" {
		// Without the outer id there is no stable UUID, so a retry could not dedupe.
		msg.Ack()
		posthogDroppedCounter.WithLabelValues(e.eventType, codes.MissingID.String()).Inc()
		return
	}

	outer := &eventproto.Event{}
	if err := proto.Unmarshal(msg.Data, outer); err != nil {
		e.drop(msg, id, dropReasonMalformedOuter, err)
		return
	}

	capture, err := e.mapEvent(outer)
	if err != nil {
		reason := dropReasonMalformedInner
		if err == posthog.ErrMissingDistinctID {
			reason = dropReasonMissingDistinct
		}
		e.drop(msg, id, reason, err)
		return
	}

	posthogEventLagHistogram.WithLabelValues(e.eventType).Observe(time.Since(capture.Timestamp).Seconds())

	start := time.Now()
	posthogEnqueuedCounter.WithLabelValues(e.eventType).Inc()
	posthogInflightGauge.WithLabelValues(e.eventType).Set(float64(e.client.Pending()))

	deliverErr := e.client.Deliver(ctx, capture)
	posthogDeliveryLatencyHistogram.WithLabelValues(e.eventType).Observe(time.Since(start).Seconds())
	posthogInflightGauge.WithLabelValues(e.eventType).Set(float64(e.client.Pending()))

	switch posthog.Classify(deliverErr) {
	case posthog.ClassificationDelivered:
		// Ack only here: Enqueue returning nil is not delivery.
		msg.Ack()
		posthogDeliveredCounter.WithLabelValues(e.eventType).Inc()
	case posthog.ClassificationTerminal:
		// Redelivery cannot help, so free the subscription and record the loss.
		msg.Ack()
		posthogDroppedCounter.WithLabelValues(e.eventType, dropReasonTerminalDelivery).Inc()
		e.logger.Error("Dropping event PostHog will never accept",
			zap.String("eventId", outer.Id),
			zap.String("environmentId", outer.EnvironmentId),
			zap.Error(deliverErr),
		)
	default:
		// Nack: the broker is the next retry layer. The UUID and timestamp are stable,
		// so a redelivery is deduped rather than double counted.
		msg.Nack()
		posthogDeliveryFailedCounter.
			WithLabelValues(e.eventType, string(posthog.ClassificationRetryable)).Inc()
		e.logger.Warn("Retrying event after delivery failure",
			zap.String("eventId", outer.Id),
			zap.String("environmentId", outer.EnvironmentId),
			zap.Error(deliverErr),
		)
	}
}

func (e *postHogExporter) drop(msg *puller.Message, id, reason string, err error) {
	// Acked, not nacked: an event that can never be mapped stays unmappable, so
	// redelivering it would occupy the subscription forever.
	msg.Ack()
	posthogDroppedCounter.WithLabelValues(e.eventType, reason).Inc()
	e.logger.Error("Dropping unusable event",
		zap.String("messageId", id),
		zap.String("reason", reason),
		zap.Error(err),
	)
}

func observePrivacyFilter(reason posthog.FilterReason) {
	posthogPrivacyFilteredCounter.WithLabelValues(string(reason)).Inc()
}
