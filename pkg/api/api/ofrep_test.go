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
	"sync"
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
	"google.golang.org/grpc/status"
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
	ofrepTestTimeout       = 5 * time.Second
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
	}`))

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
			user, failure := decodeOFREPUser(strings.NewReader(test.body))
			require.Nil(t, user)
			require.NotNil(t, failure)
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
		{name: "signed integer", variationType: featureproto.Feature_NUMBER, input: "+1", expectedJSON: `1`},
		{name: "signed large integer", variationType: featureproto.Feature_NUMBER, input: "+9007199254740993", expectedJSON: `9007199254740993`},
		{name: "leading zero", variationType: featureproto.Feature_NUMBER, input: "01", expectedJSON: `1`},
		{name: "leading decimal point", variationType: featureproto.Feature_NUMBER, input: ".5", expectedJSON: `0.5`},
		{name: "trailing decimal point", variationType: featureproto.Feature_NUMBER, input: "1.", expectedJSON: `1.0`},
		{name: "underscored integer", variationType: featureproto.Feature_NUMBER, input: "1_000", expectedJSON: `1000`},
		{name: "hexadecimal float", variationType: featureproto.Feature_NUMBER, input: "0x1p2", expectedJSON: `4`},
		{name: "fraction", variationType: featureproto.Feature_NUMBER, input: "12.50", expectedJSON: `12.50`},
		{name: "object", variationType: featureproto.Feature_JSON, input: `{"count":9007199254740993}`, expectedJSON: `{"count":9007199254740993}`},
		{name: "yaml converted by evaluator", variationType: featureproto.Feature_YAML, input: `{"tier":"pro"}`, expectedJSON: `{"tier":"pro"}`},
		{name: "invalid boolean", variationType: featureproto.Feature_BOOLEAN, input: "yes", wantError: true},
		{name: "null boolean", variationType: featureproto.Feature_BOOLEAN, input: "null", wantError: true},
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
			assert.Equal(t, test.expectedJSON, string(actual))
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
	}
	for _, test := range tests {
		assert.Equal(t, test.expected, ofrepReason(feature, test.reason))
	}
	feature.DefaultStrategy = rollout
	assert.Equal(t, ofrepReasonSplit, ofrepReason(feature, &featureproto.Reason{Type: featureproto.Reason_DEFAULT}))
}

func TestOFREPSingleEvaluationPublishesExposure(t *testing.T) {
	service := newOFREPService(t)
	feature := newOFREPFeature("enabled-flag", featureproto.Feature_BOOLEAN, "enabled", "true")
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)
	published := expectOFREPExposure(service, nil)

	before := time.Now().Unix()
	response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/enabled-flag",
		`{"context":{"targetingKey":"user-1","plan":"pro"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"enabled-flag",
		"value":true,
		"reason":"STATIC",
		"variant":"enabled",
		"metadata":{"featureVersion":1,"bucketeerReason":"DEFAULT"}
	}`, response.Body.String())
	validateOFREPResponse(t, "serverEvaluationSuccess", response.Body.Bytes())

	event := awaitOFREP(t, published)
	evaluationEvent := &eventproto.EvaluationEvent{}
	require.NoError(t, anypb.UnmarshalTo(event.Event, evaluationEvent, proto.UnmarshalOptions{}))
	assert.NotEmpty(t, event.Id)
	assert.Equal(t, ofrepTestEnvironmentID, event.EnvironmentId)
	assert.GreaterOrEqual(t, evaluationEvent.Timestamp, before)
	assert.LessOrEqual(t, evaluationEvent.Timestamp, time.Now().Unix())
	assert.Equal(t, "enabled-flag", evaluationEvent.FeatureId)
	assert.Equal(t, int32(1), evaluationEvent.FeatureVersion)
	assert.Equal(t, "user-1", evaluationEvent.UserId)
	assert.Equal(t, map[string]string{"plan": "pro"}, evaluationEvent.User.Data)
	assert.Equal(t, "enabled", evaluationEvent.VariationId)
	assert.Equal(t, featureproto.Reason_DEFAULT, evaluationEvent.Reason.Type)
	assert.Equal(t, eventproto.SourceId_OPEN_FEATURE_OFREP, evaluationEvent.SourceId)
	assert.Equal(t, ofrepVersion, evaluationEvent.SdkVersion)
}

func TestOFREPSingleEvaluationUsesSegmentsAndToleratesPublishFailure(t *testing.T) {
	service := newOFREPService(t)
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
	expectOFREPAuth(service, 1)
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

	response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/segment-flag",
		`{"context":{"targetingKey":"user-1","plan":"premium"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"segment-flag",
		"value":"on",
		"reason":"TARGETING_MATCH",
		"variant":"on",
		"metadata":{"featureVersion":1,"bucketeerReason":"RULE","ruleId":"segment-rule"}
	}`, response.Body.String())
}

func TestOFREPSingleEvaluationDoesNotWaitForPublisher(t *testing.T) {
	service := newOFREPService(t)
	feature := newOFREPFeature("enabled-flag", featureproto.Feature_BOOLEAN, "enabled", "true")
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)
	release := make(chan struct{})
	published := expectOFREPExposure(service, func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// The publisher is still blocked when the response is complete.
	response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/enabled-flag", `{"context":{"targetingKey":"user-1"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Body.String(), `"value":true`)
	awaitOFREP(t, published)
	close(release)
}

func TestOFREPExposurePublicationOutlivesRequest(t *testing.T) {
	service := newOFREPService(t)
	feature := newOFREPFeature("enabled-flag", featureproto.Feature_BOOLEAN, "enabled", "true")
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)
	requestCanceled := make(chan struct{})
	publishErr := make(chan error, 1)
	expectOFREPExposure(service, func(ctx context.Context) error {
		<-requestCanceled
		publishErr <- ctx.Err()
		return nil
	})

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/ofrep/v1/evaluate/flags/enabled-flag",
		strings.NewReader(`{"context":{"targetingKey":"user-1"}}`)).WithContext(requestCtx)
	request.Header = ofrepAuthHeader(ofrepTestAPIKey)
	response := httptest.NewRecorder()
	runtimeServeMux(t, service).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)

	// Finishing and canceling the request must not cancel the accepted publication.
	cancelRequest()
	close(requestCanceled)
	assert.NoError(t, awaitOFREP(t, publishErr))
}

func TestOFREPSingleIntegerResponseMatchesSchema(t *testing.T) {
	service := newOFREPService(t)
	feature := newOFREPFeature("max-items", featureproto.Feature_NUMBER, "default", "42")
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 1)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/max-items", `{"context":{"targetingKey":"user-1"}}`)

	require.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Body.String(), `"value":42`)
	validateOFREPResponse(t, "serverEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPSingleEvaluationUsesPrerequisites(t *testing.T) {
	service := newOFREPService(t)
	prerequisite := newOFREPFeature("prerequisite", featureproto.Feature_BOOLEAN, "base-off", "false")
	prerequisite.Variations = append(prerequisite.Variations, &featureproto.Variation{Id: "base-on", Value: "true"})
	target := newOFREPFeature("dependent", featureproto.Feature_BOOLEAN, "target-on", "true")
	target.Variations = append(target.Variations, &featureproto.Variation{Id: "target-off", Value: "false"})
	target.OffVariation = "target-off"
	target.Prerequisites = []*featureproto.Prerequisite{{FeatureId: "prerequisite", VariationId: "base-on"}}
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, []*featureproto.Feature{target, prerequisite}, 1)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/dependent", `{"context":{"targetingKey":"user-1"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{
		"key":"dependent",
		"value":false,
		"reason":"TARGETING_MATCH",
		"variant":"target-off",
		"metadata":{"featureVersion":1,"bucketeerReason":"PREREQUISITE"}
	}`, response.Body.String())
}

// ofrepAuthCase describes one credential outcome shared by both OFREP routes.
type ofrepAuthCase struct {
	name       string
	apiKey     string
	expect     func(service *grpcGatewayService)
	statusCode int
	details    string
}

func ofrepAuthCases() []ofrepAuthCase {
	keyCache := func(service *grpcGatewayService) *cachev3mock.MockEnvironmentAPIKeyCache {
		return service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache)
	}
	return []ofrepAuthCase{
		{
			name:       "missing credential",
			expect:     func(*grpcGatewayService) {},
			statusCode: http.StatusUnauthorized,
			details:    "gateway: missing APIKey",
		},
		{
			name:   "unknown key",
			apiKey: "invalid-api-key",
			expect: func(service *grpcGatewayService) {
				keyCache(service).EXPECT().Get("invalid-api-key").Return(nil, cache.ErrNotFound)
				service.accountStorage.(*accountstoragemock.MockAccountStorage).EXPECT().GetEnvironmentAPIKey(
					gomock.Any(), "invalid-api-key",
				).Return(nil, accstorage.ErrAPIKeyNotFound)
			},
			statusCode: http.StatusUnauthorized,
			details:    "gateway: invalid APIKey",
		},
		{
			name:   "disabled key",
			apiKey: "disabled-api-key",
			expect: func(service *grpcGatewayService) {
				disabled := newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER)
				disabled.ApiKey.Disabled = true
				keyCache(service).EXPECT().Get("disabled-api-key").Return(disabled, nil)
			},
			statusCode: http.StatusUnauthorized,
			details:    "gateway: disabled APIKey",
		},
		{
			name:   "disabled environment",
			apiKey: "disabled-environment-key",
			expect: func(service *grpcGatewayService) {
				disabled := newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER)
				disabled.EnvironmentDisabled = true
				keyCache(service).EXPECT().Get("disabled-environment-key").Return(disabled, nil)
			},
			statusCode: http.StatusUnauthorized,
			details:    "gateway: disabled APIKey",
		},
		{
			name:   "client key",
			apiKey: "client-api-key",
			expect: func(service *grpcGatewayService) {
				keyCache(service).EXPECT().Get("client-api-key").Return(
					newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_CLIENT), nil,
				)
			},
			statusCode: http.StatusForbidden,
			details:    "gateway: bad role",
		},
		{
			name:   "public key",
			apiKey: "public-api-key",
			expect: func(service *grpcGatewayService) {
				keyCache(service).EXPECT().Get("public-api-key").Return(
					newOFREPEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY), nil,
				)
			},
			statusCode: http.StatusForbidden,
			details:    "gateway: bad role",
		},
		{
			name:   "storage failure",
			apiKey: "unavailable-api-key",
			expect: func(service *grpcGatewayService) {
				keyCache(service).EXPECT().Get("unavailable-api-key").Return(nil, cache.ErrNotFound)
				service.accountStorage.(*accountstoragemock.MockAccountStorage).EXPECT().GetEnvironmentAPIKey(
					gomock.Any(), "unavailable-api-key",
				).Return(nil, errors.New("storage unavailable"))
			},
			statusCode: http.StatusInternalServerError,
			details:    ofrepInternalErrorDetails,
		},
	}
}

func TestOFREPAuthenticationStatuses(t *testing.T) {
	paths := map[string]string{
		"single": "/ofrep/v1/evaluate/flags/unknown",
		"bulk":   ofrepBulkEvaluationPath,
	}
	for _, test := range ofrepAuthCases() {
		for route, path := range paths {
			t.Run(test.name+"/"+route, func(t *testing.T) {
				service := newOFREPService(t)
				test.expect(service)
				header := http.Header{}
				if test.apiKey != "" {
					header = ofrepAuthHeader(test.apiKey)
				}

				response := ofrepPostWithHeaders(t, service, path, header, `{"context":{"targetingKey":"user-1"}}`)

				assert.Equal(t, test.statusCode, response.Code)
				assert.JSONEq(t, fmt.Sprintf(`{"errorDetails":%q}`, test.details), response.Body.String())
				assertOFREPChallenge(t, response, test.statusCode == http.StatusUnauthorized)
				if test.apiKey != "" {
					assert.NotContains(t, response.Body.String(), test.apiKey)
				}
			})
		}
	}
}

// TestOFREPAuthorizationMappingIsLocal shows the shared checker still yields
// its original sentinels and gRPC codes; only the OFREP HTTP mapping differs.
func TestOFREPAuthorizationMappingIsLocal(t *testing.T) {
	tests := []struct {
		err      error
		grpcCode codes.Code
		httpCode int
	}{
		{err: ErrMissingAPIKey, grpcCode: codes.Unauthenticated, httpCode: http.StatusUnauthorized},
		{err: ErrInvalidAPIKey, grpcCode: codes.PermissionDenied, httpCode: http.StatusUnauthorized},
		{err: ErrDisabledAPIKey, grpcCode: codes.PermissionDenied, httpCode: http.StatusUnauthorized},
		{err: ErrBadRole, grpcCode: codes.PermissionDenied, httpCode: http.StatusForbidden},
		{err: ErrInternal, grpcCode: codes.Internal, httpCode: http.StatusInternalServerError},
		{err: ErrContextCanceled, grpcCode: codes.Canceled, httpCode: runtime.HTTPStatusFromCode(codes.Canceled)},
	}
	for _, test := range tests {
		assert.Equal(t, test.grpcCode, status.Code(test.err))
		httpCode, _ := ofrepAuthorizationError(test.err)
		assert.Equal(t, test.httpCode, httpCode, test.err.Error())
	}

	service := newOFREPService(t)
	disabled := newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER)
	disabled.ApiKey.Disabled = true
	service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get("disabled-api-key").Return(
		disabled, nil,
	)
	request := httptest.NewRequest(http.MethodPost, ofrepBulkEvaluationPath, nil)
	request.Header = ofrepAuthHeader("disabled-api-key")
	ctx, span := ofrepIncomingContext(request, methodOFREPEvaluateFlags)
	defer span.End()
	_, err := service.checkRequest(ctx, []accountproto.APIKey_Role{accountproto.APIKey_SDK_SERVER})
	assert.ErrorIs(t, err, ErrDisabledAPIKey)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func assertOFREPChallenge(t *testing.T, response *httptest.ResponseRecorder, expected bool) {
	t.Helper()
	if expected {
		assert.Equal(t, ofrepAuthenticateChallenge, response.Header().Get("WWW-Authenticate"))
		return
	}
	assert.Empty(t, response.Header().Get("WWW-Authenticate"))
}

func TestOFREPRejectsOversizedBody(t *testing.T) {
	paths := map[string]string{
		"single": "/ofrep/v1/evaluate/flags/flag",
		"bulk":   ofrepBulkEvaluationPath,
	}
	oversized := `{"context":{"targetingKey":"user-1","blob":"` + strings.Repeat("x", ofrepMaxBodyBytes) + `"}}`
	for route, path := range paths {
		t.Run(route, func(t *testing.T) {
			service := newOFREPService(t)
			expectOFREPAuth(service, 1)

			response := ofrepPost(t, service, path, oversized)

			assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
			assert.Contains(t, response.Body.String(), `"errorCode":"GENERAL"`)
		})
	}
}

func TestOFREPAuthenticationAndFlagNotFound(t *testing.T) {
	t.Run("authenticate before lookup", func(t *testing.T) {
		service := newOFREPService(t)
		expectOFREPAuth(service, 1)
		expectOFREPFeatures(service, []*featureproto.Feature{}, 1)
		response := ofrepPost(t, service, "/ofrep/v1/evaluate/flags/unknown", `{"context":{"targetingKey":"user-1"}}`)
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
			service := newOFREPService(t)
			expectOFREPAuth(service, 1)
			expectOFREPFeatures(service, nil, 1)

			response := ofrepPostWithHeaders(t, service, "/ofrep/v1/evaluate/flags/unknown", test.headers,
				`{"context":{"targetingKey":"user-1"}}`)

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
			service := newOFREPService(t)

			response := ofrepPostWithHeaders(t, service, "/ofrep/v1/evaluate/flags/unknown", test.headers,
				`{"context":{"targetingKey":"user-1"}}`)

			assert.Equal(t, http.StatusUnauthorized, response.Code)
			assertOFREPChallenge(t, response, true)
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

			ctx, span := ofrepIncomingContext(request, methodOFREPEvaluateFlags)
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
	ctx, span := ofrepIncomingContext(request, methodOFREPEvaluateFlags)
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
			service := newOFREPService(t)
			response := httptest.NewRecorder()

			_, ok := service.authorizeOFREP(response, test.ctx)

			assert.False(t, ok)
			assert.Equal(t, runtime.HTTPStatusFromCode(test.code), response.Code)
			assert.JSONEq(t, fmt.Sprintf(`{"errorDetails":%q}`, test.errorDetails), response.Body.String())
			assertOFREPChallenge(t, response, false)
		})
	}
}

func TestOFREPContextTerminationAfterAuthenticationIsNotInternalError(t *testing.T) {
	tests := []struct {
		name         string
		newContext   func(t *testing.T) (ctx context.Context, terminate func())
		code         codes.Code
		errorDetails string
	}{
		{
			name: "canceled",
			newContext: func(t *testing.T) (context.Context, func()) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			code:         codes.Canceled,
			errorDetails: "gateway: context canceled",
		},
		{
			name: "deadline exceeded",
			newContext: func(t *testing.T) (context.Context, func()) {
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				t.Cleanup(cancel)
				return ctx, func() { <-ctx.Done() }
			},
			code:         codes.DeadlineExceeded,
			errorDetails: "gateway: context deadline exceeded",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newOFREPService(t)
			expectOFREPAuth(service, 1)
			started := make(chan struct{})
			release := make(chan struct{})
			fetchDone := make(chan struct{})
			service.featuresCache.(*cachev3mock.MockFeaturesCache).EXPECT().Get(ofrepTestEnvironmentID).DoAndReturn(
				func(string) (*featureproto.Features, error) {
					close(started)
					<-release
					close(fetchDone)
					return &featureproto.Features{}, nil
				},
			)

			ctx, terminate := test.newContext(t)
			request := httptest.NewRequest(http.MethodPost, "/ofrep/v1/evaluate/flags/flag",
				strings.NewReader(`{"context":{"targetingKey":"user-1"}}`)).WithContext(ctx)
			request.Header.Set("Authorization", ofrepTestAPIKey)
			response := httptest.NewRecorder()
			mux := runtimeServeMux(t, service)
			done := make(chan struct{})
			go func() {
				mux.ServeHTTP(response, request)
				close(done)
			}()

			<-started
			terminate()
			<-done
			close(release)
			<-fetchDone

			assert.Equal(t, runtime.HTTPStatusFromCode(test.code), response.Code)
			assert.JSONEq(t, fmt.Sprintf(`{"errorDetails":%q}`, test.errorDetails), response.Body.String())
		})
	}
}

func TestOFREPBulkEvaluationIsSortedAndConditional(t *testing.T) {
	service := newOFREPService(t)
	features := []*featureproto.Feature{
		newOFREPFeature("z-object", featureproto.Feature_JSON, "object", `{"tier":"pro"}`),
		newOFREPFeature("y-yaml", featureproto.Feature_YAML, "yaml", "tier: pro"),
		newOFREPFeature("a-number", featureproto.Feature_NUMBER, "number", "42"),
		newOFREPFeature("m-bool", featureproto.Feature_BOOLEAN, "boolean", "true"),
		{Id: "archived", Archived: true},
	}
	expectOFREPAuth(service, 2)
	expectOFREPFeatures(service, features, 2)

	first := ofrepPost(t, service, ofrepBulkEvaluationPath, `{"context":{"targetingKey":"user-1"}}`)
	require.Equal(t, http.StatusOK, first.Code)
	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag)
	assert.Equal(t, '"', rune(etag[0]))
	// Sorted by key, archived flags excluded, and no per-flag error entries.
	assert.Equal(t, []string{"a-number", "m-bool", "y-yaml", "z-object"}, ofrepBulkKeys(t, first.Body.Bytes()))
	assert.NotContains(t, first.Body.String(), `"errorCode"`)
	assert.NotContains(t, first.Body.String(), "eventStreams")
	assert.Contains(t, first.Body.String(), `"key":"y-yaml","value":{"tier":"pro"}`)
	validateOFREPResponse(t, "bulkEvaluationSuccess", first.Body.Bytes())

	conditional := ofrepAuthHeader(ofrepTestAPIKey)
	conditional.Set("If-None-Match", `"unrelated", W/`+etag)
	second := ofrepPostWithHeaders(t, service, ofrepBulkEvaluationPath, conditional,
		`{"context":{"targetingKey":"user-1"}}`)
	assert.Equal(t, http.StatusNotModified, second.Code)
	assert.Empty(t, second.Body.String())
	assert.Equal(t, etag, second.Header().Get("ETag"))
}

func TestOFREPBulkEvaluationEmptyEnvironment(t *testing.T) {
	service := newOFREPService(t)
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, nil, 1)

	response := ofrepPost(t, service, ofrepBulkEvaluationPath, `{"context":{"targetingKey":"user-1"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"flags":[]}`, response.Body.String())
	validateOFREPResponse(t, "bulkEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPBulkKeepsPerFlagSerializationFailure(t *testing.T) {
	service := newOFREPService(t)
	features := []*featureproto.Feature{
		newOFREPFeature("bad-object", featureproto.Feature_JSON, "bad", `[]`),
		newOFREPFeature("good-string", featureproto.Feature_STRING, "good", "value"),
	}
	expectOFREPAuth(service, 1)
	expectOFREPFeatures(service, features, 1)

	response := ofrepPost(t, service, ofrepBulkEvaluationPath, `{"context":{"targetingKey":"user-1"}}`)
	require.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Body.String(), `"key":"bad-object","errorCode":"PARSE_ERROR"`)
	assert.Contains(t, response.Body.String(), `"key":"good-string","value":"value"`)
	validateOFREPResponse(t, "bulkEvaluationSuccess", response.Body.Bytes())
}

func TestOFREPBulkETagChangesWithSegmentEvaluation(t *testing.T) {
	service := newOFREPService(t)
	feature := newOFREPFeature("segment-flag", featureproto.Feature_STRING, "off", "off")
	feature.Variations = append(feature.Variations, &featureproto.Variation{Id: "on", Value: "on"})
	feature.Rules = []*featureproto.Rule{{
		Id:       "segment-rule",
		Strategy: &featureproto.Strategy{Type: featureproto.Strategy_FIXED, FixedStrategy: &featureproto.FixedStrategy{Variation: "on"}},
		Clauses:  []*featureproto.Clause{{Operator: featureproto.Clause_SEGMENT, Values: []string{"premium-users"}}},
	}}
	expectOFREPAuth(service, 2)
	expectOFREPFeatures(service, []*featureproto.Feature{feature}, 2)
	service.segmentUsersCache.(*cachev3mock.MockSegmentUsersCache).EXPECT().Get(
		"premium-users", ofrepTestEnvironmentID,
	).Return(&featureproto.SegmentUsers{
		SegmentId: "premium-users",
		Rules: []*featureproto.Rule{{Clauses: []*featureproto.Clause{{
			Attribute: "plan", Operator: featureproto.Clause_EQUALS, Values: []string{"premium"},
		}}}},
	}, nil).Times(2)

	premium := ofrepPost(t, service, ofrepBulkEvaluationPath, `{"context":{"targetingKey":"user-1","plan":"premium"}}`)
	free := ofrepPost(t, service, ofrepBulkEvaluationPath, `{"context":{"targetingKey":"user-1","plan":"free"}}`)
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
		{name: "integer success", schemaName: "serverEvaluationSuccess", body: `{"key":"max-items","value":42,"reason":"STATIC"}`},
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
	assert.True(t, ofrepETagMatches(`W/"two"`, `"two"`))
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

func newOFREPService(t *testing.T) *grpcGatewayService {
	t.Helper()
	service := newGrpcGatewayServiceWithMock(t, gomock.NewController(t))
	startOFREPExposureWorkersForTest(t, service, ofrepExposureWorkers, ofrepExposureQueueSize)
	return service
}

// startOFREPExposureWorkersForTest starts the exposure workers and drains
// them on cleanup, before the gomock controller verifies its expectations.
func startOFREPExposureWorkersForTest(t *testing.T, service *grpcGatewayService, workers, queueSize int) {
	t.Helper()
	service.startOFREPExposureWorkers(workers, queueSize)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), ofrepTestTimeout)
		defer cancel()
		service.ShutdownOFREPExposures(ctx)
	})
}

// expectOFREPExposure expects exactly one publication and returns a channel
// that receives the published event. publish, when set, runs inside the
// publisher call with the worker's context and decides its result.
func expectOFREPExposure(
	service *grpcGatewayService,
	publish func(ctx context.Context) error,
) <-chan *eventproto.Event {
	published := make(chan *eventproto.Event, 1)
	service.evaluationPublisher.(*publishermock.MockPublisher).EXPECT().Publish(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(ctx context.Context, message any) error {
		published <- message.(*eventproto.Event)
		if publish == nil {
			return nil
		}
		return publish(ctx)
	})
	return published
}

// awaitOFREP receives from ch or fails the test after ofrepTestTimeout.
func awaitOFREP[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(ofrepTestTimeout):
		t.Fatal("timed out waiting for asynchronous OFREP work")
		var zero T
		return zero
	}
}

// expectOFREPAuth resolves ofrepTestAPIKey to a server key the given number of times.
func expectOFREPAuth(service *grpcGatewayService, times int) {
	service.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(ofrepTestAPIKey).Return(
		newOFREPEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER), nil,
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

func ofrepAuthHeader(apiKey string) http.Header {
	return http.Header{"Authorization": []string{apiKey}}
}

// ofrepPost sends an OFREP request authenticated with ofrepTestAPIKey.
func ofrepPost(t *testing.T, service *grpcGatewayService, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return ofrepPostAs(t, service, path, ofrepTestAPIKey, body)
}

func ofrepPostAs(t *testing.T, service *grpcGatewayService, path, apiKey, body string) *httptest.ResponseRecorder {
	t.Helper()
	return ofrepPostWithHeaders(t, service, path, ofrepAuthHeader(apiKey), body)
}

func ofrepPostWithHeaders(
	t *testing.T,
	service *grpcGatewayService,
	path string,
	header http.Header,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	mux := runtimeServeMux(t, service)
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header = header.Clone()
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

// ofrepBulkKeys returns the flag keys of a bulk evaluation body in response order.
func ofrepBulkKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var decoded struct {
		Flags []struct {
			Key string `json:"key"`
		} `json:"flags"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	keys := make([]string, 0, len(decoded.Flags))
	for _, flag := range decoded.Flags {
		keys = append(keys, flag.Key)
	}
	return keys
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
	schema, err := compileOFREPSchema(schemaName)
	require.NoError(t, err)
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(response))
	require.NoError(t, err)
	require.NoError(t, schema.Validate(instance))
}

var (
	// ofrepSchemaMu serialises access to the shared compiler, which is not
	// safe for concurrent use; compiled schemas are.
	ofrepSchemaMu       sync.Mutex
	ofrepSchemaCompiler *jsonschema.Compiler
)

// compileOFREPSchema loads the OFREP OpenAPI document once for all tests and
// compiles the named component schema from it.
func compileOFREPSchema(schemaName string) (*jsonschema.Schema, error) {
	ofrepSchemaMu.Lock()
	defer ofrepSchemaMu.Unlock()
	if ofrepSchemaCompiler == nil {
		specification, err := os.ReadFile("../../../api-description/ofrep.openapi.yaml")
		if err != nil {
			return nil, err
		}
		var document any
		if err := yaml.Unmarshal(specification, &document); err != nil {
			return nil, err
		}
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)
		if err := compiler.AddResource("ofrep.json", document); err != nil {
			return nil, err
		}
		ofrepSchemaCompiler = compiler
	}
	return ofrepSchemaCompiler.Compile("ofrep.json#/components/schemas/" + schemaName)
}
