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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bucketeer-io/bucketeer/v2/pkg/metrics"
)

// Every label below is drawn from a closed set. Event id, user id, feature id,
// environment id and raw error text are deliberately absent: they are unbounded, and a
// per-event label would multiply the series count by the cardinality of production data.
const (
	posthogEventTypeEvaluation = "evaluation"
	posthogEventTypeGoal       = "goal"
)

var (
	posthogReceivedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "received_total",
			Help:      "Total number of broker messages received by the PostHog exporters",
		}, []string{"event_type"})

	posthogEnqueuedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "enqueued_total",
			Help:      "Total number of events accepted into the PostHog SDK queue",
		}, []string{"event_type"})

	posthogDeliveredCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "delivered_total",
			Help:      "Total number of events PostHog confirmed",
		}, []string{"event_type"})

	posthogDeliveryFailedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "delivery_failed_total",
			Help:      "Total number of events whose delivery failed, by classification",
		}, []string{"event_type", "classification"})

	posthogDroppedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "dropped_total",
			Help:      "Total number of events dropped without delivery, by reason",
		}, []string{"event_type", "reason"})

	posthogInflightGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "inflight",
			Help:      "Number of events awaiting a PostHog delivery callback",
		}, []string{"event_type"})

	posthogDeliveryLatencyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "delivery_latency_seconds",
			Help:      "Time from enqueue to delivery callback",
			Buckets:   prometheus.DefBuckets,
		}, []string{"event_type"})

	posthogEventLagHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "event_lag_seconds",
			Help:      "Time between the Bucketeer event timestamp and its export",
			// Reaches hours: this is what shows a backlog draining after a PostHog outage.
			Buckets: []float64{1, 5, 15, 60, 300, 900, 3600, 21600, 86400},
		}, []string{"event_type"})

	posthogPrivacyFilteredCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bucketeer",
			Subsystem: "posthog_exporter",
			Name:      "privacy_filtered_total",
			Help:      "Total number of fields withheld by the privacy filter, by reason",
		}, []string{"reason"})
)

func registerPostHogExporterMetrics(r metrics.Registerer) {
	r.MustRegister(
		posthogReceivedCounter,
		posthogEnqueuedCounter,
		posthogDeliveredCounter,
		posthogDeliveryFailedCounter,
		posthogDroppedCounter,
		posthogInflightGauge,
		posthogDeliveryLatencyHistogram,
		posthogEventLagHistogram,
		posthogPrivacyFilteredCounter,
	)
}
