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
	"errors"
	"sort"
	"time"

	posthogsdk "github.com/posthog/posthog-go"

	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

const (
	// ContractVersion is the analytics export contract these events conform to.
	ContractVersion = "1"

	EventFeatureEvaluated = "bucketeer_feature_evaluated"
	EventGoalReached      = "bucketeer_goal_reached"

	userAttributePrefix = "bucketeer_user_"
	metadataPrefix      = "bucketeer_metadata_"

	defaultMaxValueLength = 512
	defaultMaxAttributes  = 20
)

// ErrMissingDistinctID is terminal: PostHog cannot attribute an event with no distinct
// id, and resending the same event will never supply one.
var ErrMissingDistinctID = errors.New("posthog: event has no user id")

// reservedProperties are set by the mapper itself. An allowlisted attribute that
// collides with one is skipped rather than allowed to overwrite the contract.
var reservedProperties = map[string]struct{}{
	"bucketeer_contract_version": {},
	"bucketeer_event_id":         {},
	"bucketeer_environment_id":   {},
	"bucketeer_feature_id":       {},
	"bucketeer_feature_version":  {},
	"bucketeer_variation_id":     {},
	"bucketeer_reason":           {},
	"bucketeer_rule_id":          {},
	"bucketeer_tag":              {},
	"bucketeer_source_id":        {},
	"bucketeer_sdk_version":      {},
	"bucketeer_goal_id":          {},
	"bucketeer_goal_value":       {},
	"$feature_flag":              {},
	"$feature_flag_response":     {},
	"$process_person_profile":    {},
	"$geoip_disable":             {},
}

// FilterReason explains why an attribute was not exported. Values are bounded so they
// are safe as a metric label.
type FilterReason string

const (
	FilterAttributeNotAllowlisted FilterReason = "attribute_not_allowlisted"
	FilterMetadataNotAllowlisted  FilterReason = "metadata_not_allowlisted"
	FilterValueTooLong            FilterReason = "value_too_long"
	FilterTooManyAttributes       FilterReason = "too_many_attributes"
	FilterReservedKey             FilterReason = "reserved_key"
	FilterMissingGroupValue       FilterReason = "missing_group_value"
)

// FilterObserver records a privacy-filter decision. Never called with a field value.
type FilterObserver func(reason FilterReason)

// MapEvaluationEvent builds the PostHog capture for one Bucketeer evaluation event.
//
// The outer event id and timestamp come from the broker message, not from the clock, so
// a redelivery produces a byte-identical event and PostHog dedupes it.
func MapEvaluationEvent(
	eventID string,
	environmentID string,
	e *eventproto.EvaluationEvent,
	privacy PrivacyConfig,
	observe FilterObserver,
) (posthogsdk.Capture, error) {
	distinctID := distinctIDOf(e.UserId, e.User)
	if distinctID == "" {
		return posthogsdk.Capture{}, ErrMissingDistinctID
	}

	props := posthogsdk.Properties{
		"bucketeer_contract_version": ContractVersion,
		"bucketeer_event_id":         eventID,
		"bucketeer_environment_id":   environmentID,
		"bucketeer_feature_id":       e.FeatureId,
		"bucketeer_feature_version":  e.FeatureVersion,
		"bucketeer_variation_id":     e.VariationId,
		"bucketeer_tag":              e.Tag,
		// Stable enum names, not numeric values: a renumbered proto must not silently
		// change the meaning of historical events.
		"bucketeer_source_id":   e.SourceId.String(),
		"bucketeer_sdk_version": e.SdkVersion,
		// Compatibility properties so PostHog's flag-aware queries work. The event name
		// stays bucketeer_feature_evaluated: PostHog did not perform this assignment.
		"$feature_flag":          e.FeatureId,
		"$feature_flag_response": e.VariationId,
		// Deny-by-default: no person profile, no geo enrichment from the exporter's IP.
		"$process_person_profile": false,
		"$geoip_disable":          true,
	}
	if e.Reason != nil {
		// The original reason is preserved even for error fallbacks: an ERROR_* reason
		// must not be presented as if it were a normal targeting result.
		props["bucketeer_reason"] = e.Reason.Type.String()
		if e.Reason.RuleId != "" {
			props["bucketeer_rule_id"] = e.Reason.RuleId
		}
	}

	applyPrivacy(props, e.User, e.Metadata, privacy, observe)

	return posthogsdk.Capture{
		Uuid:       eventID,
		DistinctId: distinctID,
		Event:      EventFeatureEvaluated,
		Timestamp:  time.Unix(e.Timestamp, 0).UTC(),
		Properties: props,
		Groups:     groupsOf(e.User, privacy, observe),
	}, nil
}

// MapGoalEvent builds the PostHog capture for one Bucketeer goal event.
func MapGoalEvent(
	eventID string,
	environmentID string,
	e *eventproto.GoalEvent,
	privacy PrivacyConfig,
	observe FilterObserver,
) (posthogsdk.Capture, error) {
	distinctID := distinctIDOf(e.UserId, e.User)
	if distinctID == "" {
		return posthogsdk.Capture{}, ErrMissingDistinctID
	}

	props := posthogsdk.Properties{
		"bucketeer_contract_version": ContractVersion,
		"bucketeer_event_id":         eventID,
		"bucketeer_environment_id":   environmentID,
		"bucketeer_goal_id":          e.GoalId,
		"bucketeer_goal_value":       e.Value,
		"bucketeer_tag":              e.Tag,
		"bucketeer_source_id":        e.SourceId.String(),
		"bucketeer_sdk_version":      e.SdkVersion,
		"$process_person_profile":    false,
		"$geoip_disable":             true,
	}
	// GoalEvent.evaluations is deprecated and is deliberately not exported.

	applyPrivacy(props, e.User, e.Metadata, privacy, observe)

	return posthogsdk.Capture{
		Uuid:       eventID,
		DistinctId: distinctID,
		Event:      EventGoalReached,
		Timestamp:  time.Unix(e.Timestamp, 0).UTC(),
		Properties: props,
		Groups:     groupsOf(e.User, privacy, observe),
	}, nil
}

func distinctIDOf(userID string, user *userproto.User) string {
	if userID != "" {
		return userID
	}
	if user != nil {
		return user.Id
	}
	return ""
}

// applyPrivacy copies only exactly-allowlisted scalar values onto the event. An empty
// allowlist exports nothing, which is the default.
func applyPrivacy(
	props posthogsdk.Properties,
	user *userproto.User,
	metadata map[string]string,
	privacy PrivacyConfig,
	observe FilterObserver,
) {
	added := 0
	maxLen := privacy.MaxValueLength
	if maxLen <= 0 {
		maxLen = defaultMaxValueLength
	}
	maxAttrs := privacy.MaxAttributes
	if maxAttrs <= 0 {
		maxAttrs = defaultMaxAttributes
	}

	copyAllowed := func(source map[string]string, allowlist []string, prefix string, notAllowed FilterReason) {
		if source == nil {
			return
		}
		// Sorted, so which attributes survive the count cap is the same on every attempt.
		// Ranging the map directly would let a redelivery carry a different property set
		// than the original, breaking the identical-retry guarantee above.
		keys := make([]string, 0, len(source))
		for key := range source {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := source[key]
			if !contains(allowlist, key) {
				report(observe, notAllowed)
				continue
			}
			if len(value) > maxLen {
				report(observe, FilterValueTooLong)
				continue
			}
			name := prefix + key
			if _, reserved := reservedProperties[name]; reserved {
				report(observe, FilterReservedKey)
				continue
			}
			if added >= maxAttrs {
				report(observe, FilterTooManyAttributes)
				continue
			}
			props[name] = value
			added++
		}
	}

	if user != nil {
		// User.tagged_data is never exported: it has no safe contract in v1.
		copyAllowed(user.Data, privacy.UserAttributeAllowlist, userAttributePrefix, FilterAttributeNotAllowlisted)
	}
	copyAllowed(metadata, privacy.MetadataAllowlist, metadataPrefix, FilterMetadataNotAllowlisted)
}

// groupsOf builds PostHog group associations from explicitly mapped, allowlisted user
// attributes. No mapping means no groups, and no GroupIdentify is ever emitted, so no
// group profile properties are written.
func groupsOf(user *userproto.User, privacy PrivacyConfig, observe FilterObserver) posthogsdk.Groups {
	if len(privacy.GroupMappings) == 0 || user == nil || user.Data == nil {
		return nil
	}
	var groups posthogsdk.Groups
	for _, m := range privacy.GroupMappings {
		// Only an allowlisted attribute may become a group key; Validate enforces this
		// too, so this is defense in depth against a hand-edited config.
		if !contains(privacy.UserAttributeAllowlist, m.BucketeerUserAttribute) {
			report(observe, FilterAttributeNotAllowlisted)
			continue
		}
		value := user.Data[m.BucketeerUserAttribute]
		if value == "" {
			report(observe, FilterMissingGroupValue)
			continue
		}
		if groups == nil {
			groups = posthogsdk.NewGroups()
		}
		groups = groups.Set(m.PostHogGroupType, value)
	}
	return groups
}

func report(observe FilterObserver, reason FilterReason) {
	if observe != nil {
		observe(reason)
	}
}
