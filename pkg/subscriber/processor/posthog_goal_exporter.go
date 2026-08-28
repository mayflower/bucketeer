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

	posthogsdk "github.com/posthog/posthog-go"
	"go.uber.org/zap"

	"github.com/bucketeer-io/bucketeer/v2/pkg/integration/posthog"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	"github.com/bucketeer-io/bucketeer/v2/pkg/subscriber"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
)

type postHogGoalExporter struct {
	*postHogExporter
}

// NewPostHogGoalExporter builds the goal exporter on the existing goal topic, using its
// own subscription. It reuses the evaluation exporter's delivery contract and the shared
// SDK client; the server owns that client, so neither processor closes it.
//
// This is a regular subscriber rather than an on-demand one. The other goal consumers are
// on-demand because their Switch gates them on running experiments, but a goal event is
// worth exporting whether or not a Bucketeer experiment is running.
func NewPostHogGoalExporter(
	client *posthog.Client,
	config *posthog.Config,
	logger *zap.Logger,
) subscriber.PubSubProcessor {
	e := &postHogExporter{
		client:    client,
		config:    config,
		eventType: posthogEventTypeGoal,
		logger:    logger.Named("posthog-goal-exporter"),
	}
	e.mapEvent = func(outer *eventproto.Event) (posthogsdk.Capture, error) {
		inner := &eventproto.GoalEvent{}
		if err := outer.Event.UnmarshalTo(inner); err != nil {
			return posthogsdk.Capture{}, err
		}
		return posthog.MapGoalEvent(
			outer.Id, outer.EnvironmentId, inner, config.Privacy, observePrivacyFilter,
		)
	}
	return &postHogGoalExporter{postHogExporter: e}
}

func (p *postHogGoalExporter) Process(ctx context.Context, msgChan <-chan *puller.Message) error {
	return p.run(ctx, msgChan)
}
