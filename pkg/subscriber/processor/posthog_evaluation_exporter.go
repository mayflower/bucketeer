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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller/codes"
	"github.com/bucketeer-io/bucketeer/v2/pkg/subscriber"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
)

const (
	dropReasonMalformedOuter   = "malformed_outer_event"
	dropReasonMalformedInner   = "malformed_inner_event"
	dropReasonMissingDistinct  = "missing_distinct_id"
	dropReasonTerminalDelivery = "terminal_delivery_failure"
)

type postHogEvaluationExporter struct {
	client *posthog.Client
	config *posthog.Config
	logger *zap.Logger
}

// NewPostHogEvaluationExporter builds the evaluation exporter. The client is owned by the
// server and shared with the goal exporter, so this processor never closes it.
func NewPostHogEvaluationExporter(
	client *posthog.Client,
	config *posthog.Config,
	logger *zap.Logger,
) subscriber.PubSubProcessor {
	return &postHogEvaluationExporter{
		client: client,
		config: config,
		logger: logger.Named("posthog-evaluation-exporter"),
	}
}

// Process consumes the existing evaluation topic. It performs no work on the evaluation
// request path: this runs in the subscriber, so a slow or unreachable PostHog delays
// export only, never a feature flag evaluation.
func (p *postHogEvaluationExporter) Process(ctx context.Context, msgChan <-chan *puller.Message) error {
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				p.logger.Info("Message channel closed, stopping")
				return nil
			}
			p.handle(ctx, msg)
		case <-ctx.Done():
			// Shutdown: stop accepting new work. Messages already handed to us have
			// been resolved or nacked by handle, so nothing is silently acked.
			p.logger.Info("Context done, stopping")
			return nil
		}
	}
}

func (p *postHogEvaluationExporter) handle(ctx context.Context, msg *puller.Message) {
	posthogReceivedCounter.WithLabelValues(posthogEventTypeEvaluation).Inc()

	id := msg.Attributes["id"]
	if id == "" {
		// Without the outer id there is no stable UUID, so a retry could not dedupe.
		msg.Ack()
		posthogDroppedCounter.WithLabelValues(posthogEventTypeEvaluation, codes.MissingID.String()).Inc()
		return
	}

	outer := &eventproto.Event{}
	if err := proto.Unmarshal(msg.Data, outer); err != nil {
		p.dropMalformed(msg, id, dropReasonMalformedOuter, err)
		return
	}
	inner := &eventproto.EvaluationEvent{}
	if err := outer.Event.UnmarshalTo(inner); err != nil {
		p.dropMalformed(msg, id, dropReasonMalformedInner, err)
		return
	}

	capture, err := posthog.MapEvaluationEvent(
		outer.Id, outer.EnvironmentId, inner, p.config.Privacy, observePrivacyFilter,
	)
	if err != nil {
		// A missing distinct id cannot be supplied by resending the same event.
		p.dropMalformed(msg, id, dropReasonMissingDistinct, err)
		return
	}

	posthogEventLagHistogram.WithLabelValues(posthogEventTypeEvaluation).
		Observe(time.Since(capture.Timestamp).Seconds())

	start := time.Now()
	posthogEnqueuedCounter.WithLabelValues(posthogEventTypeEvaluation).Inc()
	posthogInflightGauge.WithLabelValues(posthogEventTypeEvaluation).Set(float64(p.client.Pending()))

	deliverErr := p.client.Deliver(ctx, capture)
	posthogDeliveryLatencyHistogram.WithLabelValues(posthogEventTypeEvaluation).
		Observe(time.Since(start).Seconds())
	posthogInflightGauge.WithLabelValues(posthogEventTypeEvaluation).Set(float64(p.client.Pending()))

	switch posthog.Classify(deliverErr) {
	case posthog.ClassificationDelivered:
		// Ack only here: Enqueue returning nil is not delivery.
		msg.Ack()
		posthogDeliveredCounter.WithLabelValues(posthogEventTypeEvaluation).Inc()
	case posthog.ClassificationTerminal:
		// Redelivery cannot help, so free the subscription and record the loss.
		msg.Ack()
		posthogDroppedCounter.WithLabelValues(posthogEventTypeEvaluation, dropReasonTerminalDelivery).Inc()
		p.logger.Error("Dropping event PostHog will never accept",
			zap.String("eventId", outer.Id),
			zap.String("environmentId", outer.EnvironmentId),
			zap.Error(deliverErr),
		)
	default:
		// Nack: the broker is the next retry layer. The UUID and timestamp are stable,
		// so a redelivery is deduped rather than double counted.
		msg.Nack()
		posthogDeliveryFailedCounter.
			WithLabelValues(posthogEventTypeEvaluation, string(posthog.ClassificationRetryable)).Inc()
		p.logger.Warn("Retrying event after delivery failure",
			zap.String("eventId", outer.Id),
			zap.String("environmentId", outer.EnvironmentId),
			zap.Error(deliverErr),
		)
	}
}

func (p *postHogEvaluationExporter) dropMalformed(msg *puller.Message, id, reason string, err error) {
	// Acked, not nacked: an unparseable event stays unparseable, so redelivering it
	// would occupy the subscription forever.
	msg.Ack()
	posthogDroppedCounter.WithLabelValues(posthogEventTypeEvaluation, reason).Inc()
	p.logger.Error("Dropping unusable evaluation event",
		zap.String("messageId", id),
		zap.String("reason", reason),
		zap.Error(err),
	)
}

func observePrivacyFilter(reason posthog.FilterReason) {
	posthogPrivacyFilteredCounter.WithLabelValues(string(reason)).Inc()
}
