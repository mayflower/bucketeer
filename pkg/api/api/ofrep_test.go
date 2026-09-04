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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
	"go.uber.org/mock/gomock"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	accstorage "github.com/bucketeer-io/bucketeer/v2/pkg/account/storage/v2"
	accountstoragemock "github.com/bucketeer-io/bucketeer/v2/pkg/account/storage/v2/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/cache"
	cachev3mock "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3/mock"
	publishermock "github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher/mock"
	rpcmetadata "github.com/bucketeer-io/bucketeer/v2/pkg/rpc/metadata"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	environmentproto "github.com/bucketeer-io/bucketeer/v2/proto/environment"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

const (
	ofrepTestAPIKey        = "server-api-key"
	ofrepTestEnvironmentID = "environment-id"
)

func TestDecodeOFREPUser(t *testing.T) {
	t.Parallel()

	user, failure := decodeOFREPUser(strings.NewReader(`{
		"context": {
			"targetingKey": " user-1 ",
			"email": "user@example.com",
			"enabled": true,
			"count": 12.50,
			"profile": {"tier":"pro"},
			"groups": ["one",2],
			"optional": null
		}
	}`), "flag-key")

	require.Nil(t, failure)
	require.NotNil(t, user)
	assert.Equal(t, " user-1 ", user.Id)
	assert.Equal(t, map[string]string{
		"email":    "user@example.com",
		"enabled":  "true",
		"count":    "12.50",
		"profile":  `{"tier":"pro"}`,
		"groups":   `["one",2]`,
		"optional": "null",
	}, user.Data)
	assert.NotContains(t, user.Data, "targetingKey")
}

func TestDecodeOFREPUserFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		errorCode string
	}{
		{name: "malformed JSON", body: `{"context":`, errorCode: ofrepErrorParse},
		{name: "trailing JSON", body: `{"context":{"targetingKey":"user"}} {}`, errorCode: ofrepErrorParse},
		{name: "missing context", body: `{}`, errorCode: ofrepErrorInvalidContext},
		{name: "null context", body: `{"context":null}`, errorCode: ofrepErrorInvalidContext},
		{name: "array context", body: `{"context":[]}`, errorCode: ofrepErrorInvalidContext},
		{name: "missing targeting key", body: `{"context":{"email":"a@example.com"}}`, errorCode: ofrepErrorTargetingKeyMissing},
		{name: "wrong targeting key type", body: `{"context":{"targetingKey":42}}`, errorCode: ofrepErrorTargetingKeyMissing},
		{name: "blank targeting key", body: `{"context":{"targetingKey":" \t"}}`, errorCode: ofrepErrorTargetingKeyMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			user, failure := decodeOFREPUser(strings.NewReader(test.body), "flag-key")
			require.Nil(t, user)
			require.NotNil(t, failure)
			assert.Equal(t, "flag-key", failure.Key)
			assert.Equal(t, test.errorCode, failure.ErrorCode)
		})
	}
}

func TestOFREPTypedValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		variationType featureproto.Feature_VariationType
		input         string
		expectedJSON  string
		wantError     bool
	}{
		{name: "string", variationType: featureproto.Feature_STRING, input: "unchanged", expectedJSON: `"unchanged"`},
		{name: "boolean", variationType: featureproto.Feature_BOOLEAN, input: "true", expectedJSON: `true`},
		{name: "integer", variationType: featureproto.Feature_NUMBER, input: "9007199254740993", expectedJSON: `9007199254740993`},
		{name: "fraction", variationType: featureproto.Feature_NUMBER, input: "12.50", expectedJSON: `12.50`},
		{name: "object", variationType: featureproto.Feature_JSON, input: `{"count":9007199254740993}`, expectedJSON: `{"count":9007199254740993}`},
		{name: "yaml converted by evaluator", variationType: featureproto.Feature_YAML, input: `{"tier":"pro"}`, expectedJSON: `{"tier":"pro"}`},
		{name: "invalid boolean", variationType: featureproto.Feature_BOOLEAN, input: "yes", wantError: true},
		{name: "NaN", variationType: featureproto.Feature_NUMBER, input: "NaN", wantError: true},
		{name: "positive infinity", variationType: featureproto.Feature_NUMBER, input: "Inf", wantError: true},
		{name: "negative infinity", variationType: featureproto.Feature_NUMBER, input: "-Inf", wantError: true},
		{name: "overflow to infinity", variationType: featureproto.Feature_NUMBER, input: "1e999", wantError: true},
		{name: "object array", variationType: featureproto.Feature_JSON, input: `[]`, wantError: true},
		{name: "object scalar", variationType: featureproto.Feature_JSON, input: `"value"`, wantError: true},
		{name: "object null", variationType: featureproto.Feature_JSON, input: `null`, wantError: true},
		{name: "YAML array after evaluator conversion", variationType: featureproto.Feature_YAML, input: `[]`, wantError: true},
		{name: "YAML scalar after evaluator conversion", variationType: featureproto.Feature_YAML, input: `42`, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value, err := ofrepTypedValue(test.variationType, test.input)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			actual, err := json.Marshal(value)
			require.NoError(t, err)
			assert.JSONEq(t, test.expectedJSON, string(actual))
			if test.expectedJSON == `12.50` {
				assert.Equal(t, test.expectedJSON, string(actual))
			}
		})
	}
}

func TestOFREPReason(t *testing.T) {
	t.Parallel()

	fixed := &featureproto.Strategy{Type: featureproto.Strategy_FIXED}
	rollout := &featureproto.Strategy{Type: featureproto.Strategy_ROLLOUT}
	feature := &featureproto.Feature{
		DefaultStrategy: fixed,
		Rules: []*featureproto.Rule{
			{Id: "fixed", Strategy: fixed},
			{Id: "rollout", Strategy: rollout},
		},
	}
	tests := []struct {
		reason   *featureproto.Reason
		expected string
	}{
		{reason: &featureproto.Reason{Type: featureproto.Reason_TARGET}, expected: ofrepReasonTargetingMatch},
		{reason: &featureproto.Reason{Type: featureproto.Reason_PREREQUISITE}, expected: ofrepReasonTargetingMatch},
		{reason: &featureproto.Reason{Type: featureproto.Reason_OFF_VARIATION}, expected: ofrepReasonDisabled},
		{reason: &featureproto.Reason{Type: featureproto.Reason_RULE, RuleId: "fixed"}, expected: ofrepReasonTargetingMatch},
		{reason: &featureproto.Reason{Type: featureproto.Reason_RULE, RuleId: "rollout"}, expected: ofrepReasonSplit},
		{reason: &featureproto.Reason{Type: featureproto.Reason_DEFAULT}, expected: ofrepReasonStatic},
		{reason: &featureproto.Reason{Type: featureproto.Reason_CLIENT}, expected: ofrepReasonUnknown},
		{reason: nil, expected: ofrepReasonUnknown},
	}
	for _, test := range tests {
		assert.Equal(t, test.expected, ofrepReason(feature, test.reason))
	}
	feature.DefaultStrategy = rollout
	assert.Equal(t, ofrepReasonSplit, ofrepReason(feature, &featureproto.Reason{Type: featureproto.Reason_DEFAULT}))
}

func TestOFREPSingleEvaluationPublishesExposure(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	feature := newOFREPFeature("enabled-flag", featureproto.Feature_BOOLEAN, "enabled", "true")
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)

	publisher := service.evaluationPublisher.(*publishermock.MockPublisher)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ any, message any) error {
			event, ok := message.(*eventproto.Event)
			require.True(t, ok)
			evaluationEvent := &eventproto.EvaluationEvent{}
			require.NoError(t, anypb.UnmarshalTo(event.Event, evaluationEvent, proto.UnmarshalOptions{}))
			assert.Equal(t, ofrepTestEnvironmentID, event.EnvironmentId)
			assert.Equal(t, "enabled-flag", evaluationEvent.FeatureId)
			assert.Equal(t, int32(1), evaluationEvent.FeatureVersion)
			assert.Equal(t, "user-1", evaluationEvent.UserId)
			assert.Equal(t, "enabled", evaluationEvent.VariationId)
			assert.Equal(t, featureproto.Reason_DEFAULT, evaluationEvent.Reason.Type)
			assert.Equal(t, eventproto.SourceId_OPEN_FEATURE_OFREP, evaluationEvent.SourceId)
			assert.Equal(t, ofrepVersion, evaluationEvent.SdkVersion)
			return nil
		},
	)

	response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/enabled-flag", ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1","plan":"pro"}}`, "")
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"enabled-flag",
		"value":true,
		"reason":"STATIC",
		"variant":"enabled",
		"metadata":{"featureVersion":1,"bucketeerReason":"DEFAULT"}
	}`, response.Body.String())
	validateOFREPResponse(t, "serverEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPSingleEvaluationUsesSegmentsAndToleratesPublishFailure(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	feature := newOFREPFeature("segment-flag", featureproto.Feature_STRING, "off", "off")
	feature.Variations = append(feature.Variations, &featureproto.Variation{Id: "on", Value: "on"})
	feature.Rules = []*featureproto.Rule{{
		Id:       "segment-rule",
		Strategy: &featureproto.Strategy{Type: featureproto.Strategy_FIXED, FixedStrategy: &featureproto.FixedStrategy{Variation: "on"}},
		Clauses: []*featureproto.Clause{{
			Operator: featureproto.Clause_SEGMENT,
			Values:   []string{"premium-users"},
		}},
	}}
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)
	service.segmentUsersCache.(*cachev3mock.MockSegmentUsersCache).EXPECT().Get(
		"premium-users", ofrepTestEnvironmentID,
	).Return(&featureproto.SegmentUsers{
		SegmentId: "premium-users",
		Rules: []*featureproto.Rule{{
			Id: "premium-plan",
			Clauses: []*featureproto.Clause{{
				Attribute: "plan",
				Operator:  featureproto.Clause_EQUALS,
				Values:    []string{"premium"},
			}},
		}},
	}, nil)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(
		gomock.Any(), gomock.Any(),
	).Return(errors.New("publisher unavailable"))

	response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/segment-flag", ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1","plan":"premium"}}`, "")
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"segment-flag",
		"value":"on",
		"reason":"TARGETING_MATCH",
		"variant":"on",
		"metadata":{"featureVersion":1,"bucketeerReason":"RULE","ruleId":"segment-rule"}
	}`, response.Body.String())
}

func TestOFREPSingleEvaluationUsesPrerequisites(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	prerequisite := newOFREPFeature("prerequisite", featureproto.Feature_BOOLEAN, "base-off", "false")
	prerequisite.Variations = append(prerequisite.Variations, &featureproto.Variation{Id: "base-on", Value: "true"})
	target := newOFREPFeature("dependent", featureproto.Feature_BOOLEAN, "target-on", "true")
	target.Variations = append(target.Variations, &featureproto.Variation{Id: "target-off", Value: "false"})
	target.OffVariation = "target-off"
	target.Prerequisites = []*featureproto.Prerequisite{{FeatureId: "prerequisite", VariationId: "base-on"}}
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{target, prerequisite}, 1)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/dependent", ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1"}}`, "")
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"dependent",
		"value":false,
		"reason":"TARGETING_MATCH",
		"variant":"target-off",
		"metadata":{"featureVersion":1,"bucketeerReason":"PREREQUISITE"}
	}`, response.Body.String())
}

func TestOFREPAuthenticationAndFlagNotFound(t *testing.T) {
	t.Run("missing authorization", func(t *testing.T) {
		controller := gomock.NewController(t)
		service := newGrpcGatewayServiceWithMock(t, controller)
		response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", "",
			`{"context":{"targetingKey":"user-1"}}`, "")
		assert.Equal(t, http.StatusUnauthorized, response.Code)
	})

	t.Run("client key forbidden", func(t *testing.T) {
		controller := gomock.NewController(t)
		service := newGrpcGatewayServiceWithMock(t, controller)
		expectOFREPAuth(service, "client-api-key", accountproto.APIKey_SDK_CLIENT, 1)
		response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", "client-api-key",
			`{"context":{"targetingKey":"user-1"}}`, "")
		assert.Equal(t, http.StatusForbidden, response.Code)
	})

	t.Run("invalid key forbidden", func(t *testing.T) {
		controller := gomock.NewController(t)
		service := newGrpcGatewayServiceWithMock(t, controller)
		service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get("invalid-api-key").Return(
			nil, cache.ErrNotFound,
		)
		service.accountStorage.(*accountstoragemock.MockAccountStorage).EXPECT().GetEnvironmentAPIKey(
			gomock.Any(), "invalid-api-key",
		).Return(nil, accstorage.ErrAPIKeyNotFound)
		response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", "invalid-api-key",
			`{"context":{"targetingKey":"user-1"}}`, "")
		assert.Equal(t, http.StatusForbidden, response.Code)
	})

	t.Run("disabled key forbidden", func(t *testing.T) {
		controller := gomock.NewController(t)
		service := newGrpcGatewayServiceWithMock(t, controller)
		disabled := newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER)
		disabled.ApiKey.Disabled = true
		service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get("disabled-api-key").Return(
			disabled, nil,
		)
		response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", "disabled-api-key",
			`{"context":{"targetingKey":"user-1"}}`, "")
		assert.Equal(t, http.StatusForbidden, response.Code)
	})

	t.Run("authenticate before lookup", func(t *testing.T) {
		controller := gomock.NewController(t)
		service := newGrpcGatewayServiceWithMock(t, controller)
		expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
		expectOFREPFeatures(service, []*featureproto.Feature{}, 1)
		response := performOFREPRequest(t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", ofrepTestAPIKey,
			`{"context":{"targetingKey":"user-1"}}`, "")
		assert.Equal(t, http.StatusNotFound, response.Code)
		assert.JSONEq(t, `{"key":"unknown","errorCode":"FLAG_NOT_FOUND","errorDetails":"Flag \"unknown\" was not found"}`,
			response.Body.String())
		validateOFREPResponse(t, "flagNotFound", response.Body.Bytes())
	})
}

func TestOFREPStandardAuthentication(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
	}{
		{name: "X-API-Key", headers: http.Header{"X-Api-Key": []string{ofrepTestAPIKey}}},
		{name: "Bearer", headers: http.Header{"Authorization": []string{"Bearer " + ofrepTestAPIKey}}},
		{name: "case-insensitive bearer", headers: http.Header{"Authorization": []string{"bearer " + ofrepTestAPIKey}}},
		{name: "matching schemes", headers: http.Header{
			"Authorization": []string{"Bearer " + ofrepTestAPIKey},
			"X-Api-Key":     []string{ofrepTestAPIKey},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			service := newGrpcGatewayServiceWithMock(t, controller)
			expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
			expectOFREPFeatures(service, nil, 1)

			response := performOFREPRequestWithHeaders(
				t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", test.headers,
				`{"context":{"targetingKey":"user-1"}}`, "",
			)

			assert.Equal(t, http.StatusNotFound, response.Code)
		})
	}
}

func TestOFREPAuthenticationRejectsAmbiguousCredentials(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
	}{
		{name: "conflicting schemes", headers: http.Header{
			"Authorization": []string{"Bearer first-key"},
			"X-Api-Key":     []string{"second-key"},
		}},
		{name: "multiple bearer tokens", headers: http.Header{
			"Authorization": []string{"Bearer first-key", "Bearer second-key"},
		}},
		{name: "malformed bearer token", headers: http.Header{
			"Authorization": []string{"Bearer first-key extra"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			service := newGrpcGatewayServiceWithMock(t, controller)

			response := performOFREPRequestWithHeaders(
				t, service, http.MethodPost, "/ofrep/v1/evaluate/flags/unknown", test.headers,
				`{"context":{"targetingKey":"user-1"}}`, "",
			)

			assert.Equal(t, http.StatusUnauthorized, response.Code)
		})
	}
}

func TestOFREPIncomingContextPreservesCorrelation(t *testing.T) {
	tests := []struct {
		name        string
		headers     http.Header
		expectTrace string
	}{
		{
			name: "W3C Trace Context",
			headers: http.Header{
				"Authorization": []string{"Bearer " + ofrepTestAPIKey},
				"Traceparent":   []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
				"X-Request-Id":  []string{"request-w3c"},
			},
			expectTrace: "4bf92f3577b34da6a3ce929d0e0e4736",
		},
		{
			name: "B3",
			headers: http.Header{
				"X-Api-Key":    []string{ofrepTestAPIKey},
				"X-B3-Traceid": []string{"463ac35c9f6413ad48485a3953bb6124"},
				"X-B3-Spanid":  []string{"a2fb4a1d1a96d312"},
				"X-B3-Sampled": []string{"1"},
				"X-Request-Id": []string{"request-b3"},
			},
			expectTrace: "463ac35c9f6413ad48485a3953bb6124",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, ofrepBulkEvaluationPath, nil)
			request.Header = test.headers

			ctx, span := ofrepIncomingContext(request, ofrepBulkEvaluationSpan)
			defer span.End()

			assert.Equal(t, test.headers.Get("X-Request-ID"), rpcmetadata.GetXRequestIDFromIncomingContext(ctx))
			assert.Equal(t, test.expectTrace, trace.FromContext(ctx).SpanContext().TraceID.String())
			apiKey, err := (&grpcGatewayService{}).extractAPIKey(ctx)
			require.NoError(t, err)
			assert.Equal(t, ofrepTestAPIKey, apiKey)
		})
	}
}

func TestOFREPIncomingContextGeneratesRequestID(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, ofrepBulkEvaluationPath, nil)
	ctx, span := ofrepIncomingContext(request, ofrepBulkEvaluationSpan)
	defer span.End()

	assert.NotEmpty(t, rpcmetadata.GetXRequestIDFromIncomingContext(ctx))
}

func TestOFREPAuthorizationMapsContextErrors(t *testing.T) {
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	deadlineContext, cancelDeadline := context.WithDeadline(context.Background(), time.Time{})
	t.Cleanup(cancelDeadline)
	tests := []struct {
		name         string
		ctx          context.Context
		code         codes.Code
		errorDetails string
	}{
		{name: "canceled", ctx: canceledContext, code: codes.Canceled, errorDetails: "gateway: context canceled"},
		{name: "deadline exceeded", ctx: deadlineContext, code: codes.DeadlineExceeded, errorDetails: "gateway: context deadline exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			service := newGrpcGatewayServiceWithMock(t, controller)
			response := httptest.NewRecorder()

			_, ok := service.authorizeOFREP(response, test.ctx)

			assert.False(t, ok)
			assert.Equal(t, runtime.HTTPStatusFromCode(test.code), response.Code)
			assert.JSONEq(t, fmt.Sprintf(`{"errorDetails":%q}`, test.errorDetails), response.Body.String())
		})
	}
}

func TestOFREPBulkEvaluationIsSortedAndConditional(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	features := []*featureproto.Feature{
		newOFREPFeature("z-object", featureproto.Feature_JSON, "object", `{"tier":"pro"}`),
		newOFREPFeature("y-yaml", featureproto.Feature_YAML, "yaml", "tier: pro"),
		newOFREPFeature("a-number", featureproto.Feature_NUMBER, "number", "12.50"),
		newOFREPFeature("m-bool", featureproto.Feature_BOOLEAN, "boolean", "true"),
		{Id: "archived", Archived: true},
	}
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 2)
	expectOFREPFeatures(service, features, 2)

	first := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1"}}`, "")
	require.Equal(t, http.StatusOK, first.Code)
	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag)
	assert.Equal(t, '"', rune(etag[0]))
	assert.NotContains(t, first.Body.String(), "archived")
	assert.NotContains(t, first.Body.String(), "FLAG_NOT_FOUND")
	assert.NotContains(t, first.Body.String(), "eventStreams")
	assert.Less(t, strings.Index(first.Body.String(), "a-number"), strings.Index(first.Body.String(), "m-bool"))
	assert.Less(t, strings.Index(first.Body.String(), "m-bool"), strings.Index(first.Body.String(), "y-yaml"))
	assert.Less(t, strings.Index(first.Body.String(), "y-yaml"), strings.Index(first.Body.String(), "z-object"))
	assert.Contains(t, first.Body.String(), `"key":"y-yaml","value":{"tier":"pro"}`)
	validateOFREPResponse(t, "bulkEvaluationSuccess", first.Body.Bytes())

	second := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1"}}`, `"unrelated", `+etag)
	assert.Equal(t, http.StatusNotModified, second.Code)
	assert.Empty(t, second.Body.String())
	assert.Equal(t, etag, second.Header().Get("ETag"))
}

func TestOFREPBulkEvaluationEmptyEnvironment(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
	expectOFREPFeatures(service, nil, 1)

	response := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1"}}`, "")
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"flags":[]}`, response.Body.String())
	validateOFREPResponse(t, "bulkEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPBulkKeepsPerFlagSerializationFailure(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	features := []*featureproto.Feature{
		newOFREPFeature("bad-object", featureproto.Feature_JSON, "bad", `[]`),
		newOFREPFeature("good-string", featureproto.Feature_STRING, "good", "value"),
	}
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 1)
	expectOFREPFeatures(service, features, 1)

	response := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1"}}`, "")
	require.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Body.String(), `"key":"bad-object","errorCode":"PARSE_ERROR"`)
	assert.Contains(t, response.Body.String(), `"key":"good-string","value":"value"`)
	validateOFREPResponse(t, "bulkEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPBulkETagChangesWithSegmentEvaluation(t *testing.T) {
	controller := gomock.NewController(t)
	service := newGrpcGatewayServiceWithMock(t, controller)
	feature := newOFREPFeature("segment-flag", featureproto.Feature_STRING, "off", "off")
	feature.Variations = append(feature.Variations, &featureproto.Variation{Id: "on", Value: "on"})
	feature.Rules = []*featureproto.Rule{{
		Id:       "segment-rule",
		Strategy: &featureproto.Strategy{Type: featureproto.Strategy_FIXED, FixedStrategy: &featureproto.FixedStrategy{Variation: "on"}},
		Clauses:  []*featureproto.Clause{{Operator: featureproto.Clause_SEGMENT, Values: []string{"premium-users"}}},
	}}
	expectOFREPAuth(service, ofrepTestAPIKey, accountproto.APIKey_SDK_SERVER, 2)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 2)
	service.segmentUsersCache.(*cachev3mock.MockSegmentUsersCache).EXPECT().Get(
		"premium-users", ofrepTestEnvironmentID,
	).Return(&featureproto.SegmentUsers{
		SegmentId: "premium-users",
		Rules: []*featureproto.Rule{{Clauses: []*featureproto.Clause{{
			Attribute: "plan", Operator: featureproto.Clause_EQUALS, Values: []string{"premium"},
		}}}},
	}, nil).Times(2)

	premium := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1","plan":"premium"}}`, "")
	free := performOFREPRequest(t, service, http.MethodPost, ofrepBulkEvaluationPath, ofrepTestAPIKey,
		`{"context":{"targetingKey":"user-1","plan":"free"}}`, "")
	require.Equal(t, http.StatusOK, premium.Code)
	require.Equal(t, http.StatusOK, free.Code)
	assert.NotEqual(t, premium.Header().Get("ETag"), free.Header().Get("ETag"))
	assert.Contains(t, premium.Body.String(), `"value":"on"`)
	assert.Contains(t, free.Body.String(), `"value":"off"`)
}

func TestOFREPResponseSchemas(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		schemaName string
		body       string
	}{
		{name: "evaluation failure", schemaName: "evaluationFailure", body: `{"key":"flag","errorCode":"PARSE_ERROR","errorDetails":"invalid value"}`},
		{name: "bulk failure", schemaName: "bulkEvaluationFailure", body: `{"errorCode":"INVALID_CONTEXT","errorDetails":"invalid context"}`},
		{name: "general error", schemaName: "generalErrorResponse", body: `{"errorDetails":"internal error"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validateOFREPResponse(t, test.schemaName, []byte(test.body))
		})
	}
}

func TestOFREPETagMatches(t *testing.T) {
	t.Parallel()
	assert.True(t, ofrepETagMatches(`"one", "two"`, `"two"`))
	assert.True(t, ofrepETagMatches(`*`, `"two"`))
	assert.False(t, ofrepETagMatches(`W/"two"`, `"two"`))
	assert.False(t, ofrepETagMatches(`"one"`, `"two"`))
}

func newOFREPFeature(
	key string,
	variationType featureproto.Feature_VariationType,
	variationID string,
	value string,
) *featureproto.Feature {
	return &featureproto.Feature{
		Id:            key,
		Enabled:       true,
		Version:       1,
		VariationType: variationType,
		Variations: []*featureproto.Variation{
			{Id: variationID, Value: value},
		},
		DefaultStrategy: &featureproto.Strategy{
			Type:          featureproto.Strategy_FIXED,
			FixedStrategy: &featureproto.FixedStrategy{Variation: variationID},
		},
	}
}

func expectOFREPAuth(
	service *grpcGatewayService,
	apiKey string,
	role accountproto.APIKey_Role,
	times int,
) {
	service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(apiKey).Return(
		newOFREPEnvironmentAPIKey(role), nil,
	).Times(times)
}

func newOFREPEnvironmentAPIKey(role accountproto.APIKey_Role) *accountproto.EnvironmentAPIKey {
	return &accountproto.EnvironmentAPIKey{
		ApiKey:    &accountproto.APIKey{Id: "api-key-id", Role: role},
		ProjectId: "project-id",
		Environment: &environmentproto.EnvironmentV2{
			Id:             ofrepTestEnvironmentID,
			ProjectId:      "project-id",
			OrganizationId: "organization-id",
			UrlCode:        "environment",
		},
		ProjectUrlCode: "project",
	}
}

func expectOFREPFeatures(service *grpcGatewayService, features []*featureproto.Feature, times int) {
	service.featuresCache.(*cachev3mock.MockFeaturesCache).EXPECT().Get(ofrepTestEnvironmentID).Return(
		&featureproto.Features{Features: features}, nil,
	).Times(times)
}

func performOFREPRequest(
	t *testing.T,
	service *grpcGatewayService,
	method string,
	path string,
	authorization string,
	body string,
	ifNoneMatch string,
) *httptest.ResponseRecorder {
	t.Helper()
	header := make(http.Header)
	if authorization != "" {
		header.Set("authorization", authorization)
	}
	return performOFREPRequestWithHeaders(t, service, method, path, header, body, ifNoneMatch)
}

func performOFREPRequestWithHeaders(
	t *testing.T,
	service *grpcGatewayService,
	method string,
	path string,
	header http.Header,
	body string,
	ifNoneMatch string,
) *httptest.ResponseRecorder {
	t.Helper()
	mux := runtimeServeMux(t, service)
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header = header.Clone()
	if ifNoneMatch != "" {
		request.Header.Set("If-None-Match", ifNoneMatch)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func runtimeServeMux(t *testing.T, service *grpcGatewayService) http.Handler {
	t.Helper()
	mux := runtime.NewServeMux()
	require.NoError(t, service.RegisterOFREPHandlers(mux))
	return mux
}

// ../../../api-description/ofrep.openapi.yaml is derived from the OFREP schema pinned at:
// https://github.com/open-feature/protocol/blob/56d798eb9ee6608ca5554bdffe5f2b67c4e8bb10/service/openapi.yaml
func validateOFREPResponse(t *testing.T, schemaName string, response []byte) {
	t.Helper()
	specification, err := os.ReadFile("../../../api-description/ofrep.openapi.yaml")
	require.NoError(t, err)
	var document any
	require.NoError(t, yaml.Unmarshal(specification, &document))
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	require.NoError(t, compiler.AddResource("ofrep.json", document))
	schema, err := compiler.Compile("ofrep.json#/components/schemas/" + schemaName)
	require.NoError(t, err)
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(response))
	require.NoError(t, err)
	require.NoError(t, schema.Validate(instance))
}
