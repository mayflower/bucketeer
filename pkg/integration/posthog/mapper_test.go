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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

const (
	testEventID = "3f6b0c6e-6d1f-4d5f-9f0a-3f6b0c6e6d1f"
	testEnvID   = "env-1"
	testUserID  = "user-1"
)

func evaluationEvent() *eventproto.EvaluationEvent {
	return &eventproto.EvaluationEvent{
		Timestamp:      1700000000,
		FeatureId:      "feature-1",
		FeatureVersion: 7,
		UserId:         testUserID,
		VariationId:    "variation-1",
		Reason:         &featureproto.Reason{Type: featureproto.Reason_RULE, RuleId: "rule-1"},
		Tag:            "web",
		SourceId:       eventproto.SourceId_GO_SERVER,
		SdkVersion:     "1.2.3",
	}
}

func goalEvent() *eventproto.GoalEvent {
	return &eventproto.GoalEvent{
		Timestamp:  1700000000,
		GoalId:     "goal-1",
		UserId:     testUserID,
		Value:      12.5,
		Tag:        "web",
		SourceId:   eventproto.SourceId_GO_SERVER,
		SdkVersion: "1.2.3",
	}
}

func TestMapEvaluationEvent(t *testing.T) {
	t.Parallel()
	capture, err := MapEvaluationEvent(testEventID, testEnvID, evaluationEvent(), PrivacyConfig{}, nil)
	require.NoError(t, err)

	// The outer id becomes the PostHog UUID so a redelivery is deduped rather than
	// double counted.
	assert.Equal(t, testEventID, capture.Uuid)
	assert.Equal(t, testUserID, capture.DistinctId)
	assert.Equal(t, EventFeatureEvaluated, capture.Event)
	// Bucketeer timestamps are Unix seconds, not milliseconds.
	assert.Equal(t, time.Unix(1700000000, 0).UTC(), capture.Timestamp)

	props := capture.Properties
	assert.Equal(t, "1", props["bucketeer_contract_version"])
	assert.Equal(t, testEventID, props["bucketeer_event_id"])
	assert.Equal(t, testEnvID, props["bucketeer_environment_id"])
	assert.Equal(t, "feature-1", props["bucketeer_feature_id"])
	assert.Equal(t, int32(7), props["bucketeer_feature_version"])
	assert.Equal(t, "variation-1", props["bucketeer_variation_id"])
	assert.Equal(t, "web", props["bucketeer_tag"])
	assert.Equal(t, "1.2.3", props["bucketeer_sdk_version"])
	// Stable enum names, never numeric values.
	assert.Equal(t, "RULE", props["bucketeer_reason"])
	assert.Equal(t, "rule-1", props["bucketeer_rule_id"])
	assert.Equal(t, "GO_SERVER", props["bucketeer_source_id"])
	// Compatibility properties for flag-aware queries.
	assert.Equal(t, "feature-1", props["$feature_flag"])
	assert.Equal(t, "variation-1", props["$feature_flag_response"])
	// Privacy defaults.
	assert.Equal(t, false, props["$process_person_profile"])
	assert.Equal(t, true, props["$geoip_disable"])
	assert.Nil(t, capture.Groups)
}

func TestMapGoalEvent(t *testing.T) {
	t.Parallel()
	capture, err := MapGoalEvent(testEventID, testEnvID, goalEvent(), PrivacyConfig{}, nil)
	require.NoError(t, err)

	assert.Equal(t, testEventID, capture.Uuid)
	assert.Equal(t, EventGoalReached, capture.Event)
	assert.Equal(t, time.Unix(1700000000, 0).UTC(), capture.Timestamp)

	props := capture.Properties
	assert.Equal(t, "goal-1", props["bucketeer_goal_id"])
	assert.Equal(t, 12.5, props["bucketeer_goal_value"])
	assert.Equal(t, false, props["$process_person_profile"])
	assert.Equal(t, true, props["$geoip_disable"])
}

func TestMapGoalEventDoesNotExportDeprecatedEvaluations(t *testing.T) {
	t.Parallel()
	e := goalEvent()
	e.Evaluations = []*featureproto.Evaluation{{Id: "eval-1", FeatureId: "feature-1"}}

	capture, err := MapGoalEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	require.NoError(t, err)

	for key := range capture.Properties {
		assert.NotContains(t, key, "evaluation")
	}
}

func TestDistinctIDFallsBackToUserObject(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.UserId = ""
	e.User = &userproto.User{Id: "fallback-user"}

	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "fallback-user", capture.DistinctId)
}

func TestMissingDistinctIDIsTerminal(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.UserId = ""
	e.User = nil

	_, err := MapEvaluationEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	assert.ErrorIs(t, err, ErrMissingDistinctID)
}

func TestErrorReasonIsPreserved(t *testing.T) {
	t.Parallel()
	// An error fallback must not be presented as a normal targeting result.
	e := evaluationEvent()
	e.Reason = &featureproto.Reason{Type: featureproto.Reason_ERROR_FLAG_NOT_FOUND}

	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "ERROR_FLAG_NOT_FOUND", capture.Properties["bucketeer_reason"])
}

func TestNoUserAttributesOrMetadataByDefault(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{
		Id:         testUserID,
		Data:       map[string]string{"email": "someone@example.com", "plan": "pro"},
		TaggedData: map[string]*userproto.User_Data{"web": {Value: map[string]string{"x": "y"}}},
	}
	e.Metadata = map[string]string{"trace": "abc"}

	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	require.NoError(t, err)

	for key, value := range capture.Properties {
		assert.NotContains(t, key, "email")
		assert.NotContains(t, key, "plan")
		assert.NotContains(t, key, "trace")
		assert.NotEqual(t, "someone@example.com", value)
	}
}

func TestAllowlistExportsOnlyNamedKeys(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{
		Id:   testUserID,
		Data: map[string]string{"plan": "pro", "email": "someone@example.com"},
	}
	e.Metadata = map[string]string{"region": "eu", "trace": "abc"}

	privacy := PrivacyConfig{
		UserAttributeAllowlist: []string{"plan"},
		MetadataAllowlist:      []string{"region"},
	}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, nil)
	require.NoError(t, err)

	assert.Equal(t, "pro", capture.Properties["bucketeer_user_plan"])
	assert.Equal(t, "eu", capture.Properties["bucketeer_metadata_region"])
	_, hasEmail := capture.Properties["bucketeer_user_email"]
	assert.False(t, hasEmail)
	_, hasTrace := capture.Properties["bucketeer_metadata_trace"]
	assert.False(t, hasTrace)
}

func TestAllowlistCapsValueLength(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{"plan": "0123456789"}}

	var reasons []FilterReason
	privacy := PrivacyConfig{UserAttributeAllowlist: []string{"plan"}, MaxValueLength: 4}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, func(r FilterReason) {
		reasons = append(reasons, r)
	})
	require.NoError(t, err)

	// Dropped rather than truncated: a partial value must not look like the real one.
	_, present := capture.Properties["bucketeer_user_plan"]
	assert.False(t, present)
	assert.Contains(t, reasons, FilterValueTooLong)
}

func TestAllowlistCannotOverwriteAReservedProperty(t *testing.T) {
	t.Parallel()
	// A user attribute named so its prefixed form collides with a contract property
	// must not be able to rewrite the contract.
	e := evaluationEvent()
	e.Metadata = map[string]string{"id": "spoofed"}

	var reasons []FilterReason
	privacy := PrivacyConfig{MetadataAllowlist: []string{"id"}}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, func(r FilterReason) {
		reasons = append(reasons, r)
	})
	require.NoError(t, err)
	assert.Equal(t, testEventID, capture.Properties["bucketeer_event_id"])
	_ = reasons
}

func TestGroupMappingIsExplicitAndProfileFree(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{"tenant_id": "acme"}}

	privacy := PrivacyConfig{
		UserAttributeAllowlist: []string{"tenant_id"},
		GroupMappings:          []GroupMapping{{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"}},
	}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, nil)
	require.NoError(t, err)

	require.NotNil(t, capture.Groups)
	assert.Equal(t, "acme", capture.Groups["tenant"])
}

func TestNoGroupsWithoutAMapping(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{"tenant_id": "acme"}}

	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, PrivacyConfig{}, nil)
	require.NoError(t, err)
	assert.Nil(t, capture.Groups)
}

func TestGroupMappingSkipsAMissingValue(t *testing.T) {
	t.Parallel()
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{}}

	var reasons []FilterReason
	privacy := PrivacyConfig{
		UserAttributeAllowlist: []string{"tenant_id"},
		GroupMappings:          []GroupMapping{{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"}},
	}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, func(r FilterReason) {
		reasons = append(reasons, r)
	})
	require.NoError(t, err)
	assert.Nil(t, capture.Groups)
	assert.Contains(t, reasons, FilterMissingGroupValue)
}

func TestGroupMappingRefusesANonAllowlistedAttribute(t *testing.T) {
	t.Parallel()
	// Defense in depth: Validate rejects this config, so reaching here means a
	// hand-edited config, and the attribute still must not leak as a group key.
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{"tenant_id": "acme"}}

	privacy := PrivacyConfig{
		GroupMappings: []GroupMapping{{PostHogGroupType: "tenant", BucketeerUserAttribute: "tenant_id"}},
	}
	capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, nil)
	require.NoError(t, err)
	assert.Nil(t, capture.Groups)
}

func TestAttributeCapIsReportedUnderItsOwnReason(t *testing.T) {
	t.Parallel()
	// Reporting the count cap as value_too_long sends an operator to raise
	// maxValueLength, which changes nothing.
	e := evaluationEvent()
	e.User = &userproto.User{Id: testUserID, Data: map[string]string{"a": "1", "b": "2", "c": "3"}}

	var reasons []FilterReason
	privacy := PrivacyConfig{UserAttributeAllowlist: []string{"a", "b", "c"}, MaxAttributes: 1}
	_, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, func(r FilterReason) {
		reasons = append(reasons, r)
	})
	require.NoError(t, err)

	assert.Contains(t, reasons, FilterTooManyAttributes)
	assert.NotContains(t, reasons, FilterValueTooLong)
}

func TestCappedAttributesAreTheSameOnEveryAttempt(t *testing.T) {
	t.Parallel()
	// A retry must produce the identical event, so which attributes survive the cap
	// cannot depend on Go's map iteration order.
	privacy := PrivacyConfig{
		UserAttributeAllowlist: []string{"a", "b", "c", "d", "e", "f"},
		MaxAttributes:          2,
	}
	data := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6"}

	var first []string
	for attempt := 0; attempt < 25; attempt++ {
		e := evaluationEvent()
		e.User = &userproto.User{Id: testUserID, Data: data}
		capture, err := MapEvaluationEvent(testEventID, testEnvID, e, privacy, nil)
		require.NoError(t, err)

		exported := []string{}
		for key := range capture.Properties {
			if len(key) > len(userAttributePrefix) && key[:len(userAttributePrefix)] == userAttributePrefix {
				exported = append(exported, key)
			}
		}
		sort.Strings(exported)
		if first == nil {
			first = exported
			require.Len(t, first, 2)
			continue
		}
		assert.Equal(t, first, exported, "the surviving attributes changed between attempts")
	}
}
