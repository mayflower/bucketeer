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
	"fmt"
	"strings"

	"go.uber.org/zap"

	posthogsdk "github.com/posthog/posthog-go"
)

// Client owns one SDK client and the tracker correlating its callbacks. Evaluation and
// goal processors share one Client per destination; the server owns it, and no processor
// closes a client another processor may still be using.
type Client struct {
	sdk     posthogsdk.Client
	tracker *DeliveryTracker
	config  *Config
	logger  *zap.Logger
}

// redactingLogger adapts zap to the SDK's Logger interface while keeping the project API
// key out of anything the SDK prints. The SDK echoes configuration and request detail on
// error, so the key is scrubbed rather than trusted not to appear.
type redactingLogger struct {
	logger *zap.Logger
	secret string
}

func (l redactingLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debug("posthog sdk", zap.String("message", l.redact(fmt.Sprintf(format, args...))))
}

func (l redactingLogger) Logf(format string, args ...interface{}) {
	l.logger.Info("posthog sdk", zap.String("message", l.redact(fmt.Sprintf(format, args...))))
}

func (l redactingLogger) Warnf(format string, args ...interface{}) {
	l.logger.Warn("posthog sdk", zap.String("message", l.redact(fmt.Sprintf(format, args...))))
}

func (l redactingLogger) Errorf(format string, args ...interface{}) {
	l.logger.Error("posthog sdk", zap.String("message", l.redact(fmt.Sprintf(format, args...))))
}

func (l redactingLogger) redact(msg string) string {
	if l.secret == "" {
		return msg
	}
	return strings.ReplaceAll(msg, l.secret, "[REDACTED]")
}

// NewClient builds the SDK client from validated config. The API key is read from its
// mounted file here and never stored on Config.
func NewClient(config *Config, logger *zap.Logger) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	apiKey, err := config.ReadAPIKey()
	if err != nil {
		return nil, err
	}

	tracker := NewDeliveryTracker()
	sdkConfig := posthogsdk.Config{
		Endpoint: config.Endpoint,
		// Every bound is explicit: an unbounded queue would trade a PostHog outage for
		// subscriber memory growth.
		Interval:           config.FlushInterval(),
		BatchSize:          config.BatchSize,
		MaxQueueSize:       config.MaxQueueSize,
		BatchUploadTimeout: config.BatchUploadTimeout(),
		ShutdownTimeout:    config.ShutdownTimeout(),
		Callback:           tracker,
		Logger:             redactingLogger{logger: logger, secret: apiKey},
	}
	if config.CaptureMode == CaptureModeAnalyticsV1 {
		sdkConfig.CaptureMode = posthogsdk.CaptureModeAnalyticsV1
	}

	sdk, err := posthogsdk.NewWithConfig(apiKey, sdkConfig)
	if err != nil {
		return nil, fmt.Errorf("posthog: could not create client: %w", err)
	}
	return &Client{sdk: sdk, tracker: tracker, config: config, logger: logger}, nil
}

// Pending delivery, returned by Enqueue so the caller can wait for it later.
type PendingDelivery struct {
	ch      <-chan DeliveryResult
	release func()
	// err is set when the SDK refused the event outright, in which case there is no
	// callback to wait for.
	err error
}

// Enqueue hands one capture to the SDK and returns a handle to its eventual callback.
//
// Separating enqueue from the wait is what lets a caller fill a batch: waiting per event
// before enqueueing the next would leave the SDK's batch permanently under-filled, so
// every upload would carry a single event and throughput would collapse to one event per
// flush interval.
func (c *Client) Enqueue(capture posthogsdk.Capture) *PendingDelivery {
	// Registered before enqueueing: a fast callback must not arrive before there is
	// anywhere to deliver it.
	ch, release := c.tracker.Watch(capture.Uuid)
	if err := c.sdk.Enqueue(capture); err != nil {
		release()
		return &PendingDelivery{err: err}
	}
	return &PendingDelivery{ch: ch, release: release}
}

// Wait blocks until the event's callback arrives or the delivery timeout expires.
//
// Enqueue returning nil means only that the SDK queued the event, so this wait is what
// makes an ack meaningful. The returned error is classified by Classify to decide ack,
// nack, or terminal drop.
func (c *Client) Wait(ctx context.Context, pending *PendingDelivery) error {
	if pending.err != nil {
		return pending.err
	}
	defer pending.release()
	waitCtx, cancel := context.WithTimeout(ctx, c.config.DeliveryTimeout())
	defer cancel()
	return Await(waitCtx, pending.ch)
}

// Deliver enqueues one capture and waits for it. Convenience for a single event; a
// processor handling a stream should batch with Enqueue and Wait instead.
func (c *Client) Deliver(ctx context.Context, capture posthogsdk.Capture) error {
	return c.Wait(ctx, c.Enqueue(capture))
}

// Pending is the number of events awaiting a callback.
func (c *Client) Pending() int {
	return c.tracker.Pending()
}

// Close flushes and shuts the SDK down. It is bounded by the SDK's ShutdownTimeout so a
// pod termination cannot hang on an unreachable PostHog.
func (c *Client) Close() error {
	return c.sdk.Close()
}
