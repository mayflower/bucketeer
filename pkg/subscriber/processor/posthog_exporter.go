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

// pending pairs a broker message with the delivery it is waiting on, so each message can
// be settled by its own outcome once the batch has been uploaded.
type pending struct {
	msg      *puller.Message
	outer    *eventproto.Event
	delivery *posthog.PendingDelivery
	start    time.Time
}

// run is the processor loop. It performs no work on the evaluation or goal request path:
// this runs in the subscriber, so a slow or unreachable PostHog delays export only.
//
// Messages are enqueued into the SDK up to the configured batch size before anything is
// waited on. Waiting per event before enqueueing the next would leave every SDK batch
// holding a single event, so each upload would cost a full flush interval and throughput
// would collapse.
func (e *postHogExporter) run(ctx context.Context, msgChan <-chan *puller.Message) error {
	batch := make([]*pending, 0, e.config.BatchSize)
	ticker := time.NewTicker(e.config.FlushInterval())
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				e.settle(ctx, batch)
				e.logger.Info("Message channel closed, stopping")
				return nil
			}
			if p := e.enqueue(msg); p != nil {
				batch = append(batch, p)
			}
			if len(batch) >= e.config.BatchSize {
				e.settle(ctx, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			// A partial batch must not wait for more traffic that may never arrive.
			if len(batch) > 0 {
				e.settle(ctx, batch)
				batch = batch[:0]
			}
		case <-ctx.Done():
			// Shutdown: resolve what is already in flight, then stop. Nothing is
			// silently acked, because settle acks only on a success callback.
			e.settle(ctx, batch)
			e.logger.Info("Context done, stopping")
			return nil
		}
	}
}

// enqueue decodes one message and hands it to the SDK, returning nil when the message was
// already settled (dropped) and so is not part of the batch.
func (e *postHogExporter) enqueue(msg *puller.Message) *pending {
	posthogReceivedCounter.WithLabelValues(e.eventType).Inc()

	id := msg.Attributes["id"]
	if id == "" {
		// Without the outer id there is no stable UUID, so a retry could not dedupe.
		msg.Ack()
		posthogDroppedCounter.WithLabelValues(e.eventType, codes.MissingID.String()).Inc()
		return nil
	}

	outer := &eventproto.Event{}
	if err := proto.Unmarshal(msg.Data, outer); err != nil {
		e.drop(msg, id, dropReasonMalformedOuter, err)
		return nil
	}

	capture, err := e.mapEvent(outer)
	if err != nil {
		reason := dropReasonMalformedInner
		if err == posthog.ErrMissingDistinctID {
			reason = dropReasonMissingDistinct
		}
		e.drop(msg, id, reason, err)
		return nil
	}

	posthogEventLagHistogram.WithLabelValues(e.eventType).Observe(time.Since(capture.Timestamp).Seconds())
	posthogEnqueuedCounter.WithLabelValues(e.eventType).Inc()

	return &pending{
		msg:      msg,
		outer:    outer,
		delivery: e.client.Enqueue(capture),
		start:    time.Now(),
	}
}

// settle waits for each enqueued event's callback and acks or nacks its message.
func (e *postHogExporter) settle(ctx context.Context, batch []*pending) {
	if len(batch) == 0 {
		return
	}
	posthogInflightGauge.WithLabelValues(e.eventType).Set(float64(e.client.Pending()))

	for _, p := range batch {
		deliverErr := e.client.Wait(ctx, p.delivery)
		posthogDeliveryLatencyHistogram.WithLabelValues(e.eventType).
			Observe(time.Since(p.start).Seconds())

		switch posthog.Classify(deliverErr) {
		case posthog.ClassificationDelivered:
			// Ack only here: Enqueue returning nil is not delivery.
			p.msg.Ack()
			posthogDeliveredCounter.WithLabelValues(e.eventType).Inc()
		case posthog.ClassificationTerminal:
			// Redelivery cannot help, so free the subscription and record the loss.
			p.msg.Ack()
			posthogDroppedCounter.WithLabelValues(e.eventType, dropReasonTerminalDelivery).Inc()
			e.logger.Error("Dropping event PostHog will never accept",
				zap.String("eventId", p.outer.Id),
				zap.String("environmentId", p.outer.EnvironmentId),
				zap.Error(deliverErr),
			)
		default:
			// Nack: the broker is the next retry layer. The UUID and timestamp are
			// stable, so a redelivery is deduped rather than double counted.
			p.msg.Nack()
			posthogDeliveryFailedCounter.
				WithLabelValues(e.eventType, string(posthog.ClassificationRetryable)).Inc()
			e.logger.Warn("Retrying event after delivery failure",
				zap.String("eventId", p.outer.Id),
				zap.String("environmentId", p.outer.EnvironmentId),
				zap.Error(deliverErr),
			)
		}
	}
	posthogInflightGauge.WithLabelValues(e.eventType).Set(float64(e.client.Pending()))
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
