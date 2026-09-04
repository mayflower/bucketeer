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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opencensus.io/plugin/ochttp/propagation/b3"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	gmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	evaluation "github.com/bucketeer-io/bucketeer/v2/evaluation/go"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher"
	"github.com/bucketeer-io/bucketeer/v2/pkg/rpc"
	rpcmetadata "github.com/bucketeer-io/bucketeer/v2/pkg/rpc/metadata"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/client"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	userproto "github.com/bucketeer-io/bucketeer/v2/proto/user"
)

const (
	ofrepVersion = "0.3.0"

	ofrepReasonStatic         = "STATIC"
	ofrepReasonTargetingMatch = "TARGETING_MATCH"
	ofrepReasonSplit          = "SPLIT"
	ofrepReasonDisabled       = "DISABLED"
	ofrepReasonUnknown        = "UNKNOWN"

	ofrepErrorParse               = "PARSE_ERROR"
	ofrepErrorTargetingKeyMissing = "TARGETING_KEY_MISSING"
	ofrepErrorInvalidContext      = "INVALID_CONTEXT"
	ofrepErrorGeneral             = "GENERAL"
	ofrepErrorFlagNotFound        = "FLAG_NOT_FOUND"
	ofrepSingleEvaluationPath     = "/ofrep/v1/evaluate/flags/{key}"
	ofrepBulkEvaluationPath       = "/ofrep/v1/evaluate/flags"
	ofrepInternalErrorDetails     = "An internal server error occurred while processing the request"
	ofrepSingleEvaluationSpan     = "bucketeerGRPCGatewayService.OFREPEvaluateFlag"
	ofrepBulkEvaluationSpan       = "bucketeerGRPCGatewayService.OFREPEvaluateFlags"
)

type ofrepEvaluationRequest struct {
	Context json.RawMessage `json:"context"`
}

type ofrepEvaluationSuccess struct {
	Key      string         `json:"key"`
	Value    any            `json:"value"`
	Reason   string         `json:"reason"`
	Variant  string         `json:"variant"`
	Metadata map[string]any `json:"metadata"`
}

type ofrepEvaluationFailure struct {
	Key          string `json:"key"`
	ErrorCode    string `json:"errorCode"`
	ErrorDetails string `json:"errorDetails,omitempty"`
}

type ofrepBulkEvaluationSuccess struct {
	Flags []any `json:"flags"`
}

type ofrepBulkEvaluationFailure struct {
	ErrorCode    string `json:"errorCode"`
	ErrorDetails string `json:"errorDetails,omitempty"`
}

type ofrepGeneralErrorResponse struct {
	ErrorDetails string `json:"errorDetails,omitempty"`
}

// GrpcGatewayService serves Bucketeer's gRPC API and direct HTTP routes.
type GrpcGatewayService interface {
	rpc.Service
	RegisterOFREPHandlers(*runtime.ServeMux) error
}

// RegisterOFREPHandlers installs OFREP on the same ServeMux as Bucketeer's
// generated gRPC-gateway routes.
func (s *grpcGatewayService) RegisterOFREPHandlers(mux *runtime.ServeMux) error {
	if err := mux.HandlePath(http.MethodPost, ofrepSingleEvaluationPath, s.handleOFREPEvaluateFlag); err != nil {
		return err
	}
	return mux.HandlePath(http.MethodPost, ofrepBulkEvaluationPath, s.handleOFREPEvaluateFlags)
}

func (s *grpcGatewayService) handleOFREPEvaluateFlag(
	w http.ResponseWriter,
	r *http.Request,
	pathParams map[string]string,
) {
	ctx, span := ofrepIncomingContext(r, ofrepSingleEvaluationSpan)
	defer span.End()
	envAPIKey, ok := s.authorizeOFREP(w, ctx)
	if !ok {
		return
	}
	startTime := time.Now()
	s.observeOFREPRequest(envAPIKey, methodOFREPEvaluateFlag)
	defer s.observeOFREPDuration(envAPIKey, methodOFREPEvaluateFlag, startTime)

	key := pathParams["key"]
	user, failure := decodeOFREPUser(r.Body, key)
	if failure != nil {
		s.writeOFREPEvaluationFailure(w, envAPIKey, methodOFREPEvaluateFlag, http.StatusBadRequest, *failure)
		return
	}

	features, err := s.loadOFREPFeatures(ctx, envAPIKey.Environment.Id)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	targetFeatures, err := s.getTargetFeatures(features, key)
	if err != nil {
		if errors.Is(err, ErrFeatureNotFound) {
			s.writeOFREPEvaluationFailure(w, envAPIKey, methodOFREPEvaluateFlag, http.StatusNotFound, ofrepEvaluationFailure{
				Key:          key,
				ErrorCode:    ofrepErrorFlagNotFound,
				ErrorDetails: fmt.Sprintf("Flag %q was not found", key),
			})
			return
		}
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}

	eval, err := s.evaluateOFREPSingle(ctx, envAPIKey, targetFeatures, user, key)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	feature, err := s.findFeature(targetFeatures, key)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	response, err := newOFREPEvaluationSuccess(feature, eval)
	if err != nil {
		s.writeOFREPEvaluationFailure(w, envAPIKey, methodOFREPEvaluateFlag, http.StatusBadRequest, ofrepEvaluationFailure{
			Key:          key,
			ErrorCode:    ofrepErrorParse,
			ErrorDetails: err.Error(),
		})
		return
	}

	s.publishOFREPEvaluationEvent(ctx, envAPIKey.Environment.Id, user, eval)
	writeOFREPJSON(w, http.StatusOK, response)
}

func (s *grpcGatewayService) handleOFREPEvaluateFlags(
	w http.ResponseWriter,
	r *http.Request,
	_ map[string]string,
) {
	ctx, span := ofrepIncomingContext(r, ofrepBulkEvaluationSpan)
	defer span.End()
	envAPIKey, ok := s.authorizeOFREP(w, ctx)
	if !ok {
		return
	}
	startTime := time.Now()
	s.observeOFREPRequest(envAPIKey, methodOFREPEvaluateFlags)
	defer s.observeOFREPDuration(envAPIKey, methodOFREPEvaluateFlags, startTime)

	user, failure := decodeOFREPUser(r.Body, "")
	if failure != nil {
		s.writeOFREPBulkFailure(w, envAPIKey, methodOFREPEvaluateFlags, http.StatusBadRequest, ofrepBulkEvaluationFailure{
			ErrorCode:    failure.ErrorCode,
			ErrorDetails: failure.ErrorDetails,
		})
		return
	}

	features, err := s.loadOFREPFeatures(ctx, envAPIKey.Environment.Id)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}
	segmentUsers, segments, err := s.getSegmentUsersMap(ctx, features, envAPIKey.Environment.Id)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}
	evaluations, err := evaluation.NewEvaluator().EvaluateFeatures(features, user, segmentUsers, segments, "")
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}

	evaluationsByKey := make(map[string]*featureproto.Evaluation, len(evaluations.Evaluations))
	for _, eval := range evaluations.Evaluations {
		evaluationsByKey[eval.FeatureId] = eval
	}
	sortedFeatures := append([]*featureproto.Feature(nil), features...)
	sort.Slice(sortedFeatures, func(i, j int) bool { return sortedFeatures[i].Id < sortedFeatures[j].Id })
	flags := make([]any, 0, len(sortedFeatures))
	for _, feature := range sortedFeatures {
		eval, found := evaluationsByKey[feature.Id]
		if !found {
			flags = append(flags, ofrepEvaluationFailure{
				Key:          feature.Id,
				ErrorCode:    ofrepErrorGeneral,
				ErrorDetails: "Evaluation result was not found",
			})
			continue
		}
		result, err := newOFREPEvaluationSuccess(feature, eval)
		if err != nil {
			flags = append(flags, ofrepEvaluationFailure{
				Key:          feature.Id,
				ErrorCode:    ofrepErrorParse,
				ErrorDetails: err.Error(),
			})
			continue
		}
		flags = append(flags, result)
	}

	body, err := json.Marshal(ofrepBulkEvaluationSuccess{Flags: flags})
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}
	digest := sha256.Sum256(body)
	etag := fmt.Sprintf("\"%x\"", digest)
	w.Header().Set("ETag", etag)
	if ofrepETagMatches(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func ofrepIncomingContext(r *http.Request, spanName string) (context.Context, *trace.Span) {
	ctx, span := startOFREPSpan(r, spanName)
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = rpcmetadata.GenerateXRequestID()
	}
	return gmetadata.NewIncomingContext(ctx, gmetadata.Pairs(
		"authorization", ofrepAPIKey(r.Header),
		"x-request-id", requestID,
	)), span
}

func startOFREPSpan(r *http.Request, spanName string) (context.Context, *trace.Span) {
	if parent, ok := (&tracecontext.HTTPFormat{}).SpanContextFromRequest(r); ok {
		return trace.StartSpanWithRemoteParent(
			r.Context(), spanName, parent, trace.WithSpanKind(trace.SpanKindServer),
		)
	}
	if parent, ok := (&b3.HTTPFormat{}).SpanContextFromRequest(r); ok {
		return trace.StartSpanWithRemoteParent(
			r.Context(), spanName, parent, trace.WithSpanKind(trace.SpanKindServer),
		)
	}
	return trace.StartSpan(r.Context(), spanName, trace.WithSpanKind(trace.SpanKindServer))
}

// ofrepAPIKey accepts OFREP's standard authentication schemes and Bucketeer's
// legacy raw Authorization value. Multiple credentials must identify the same
// key; otherwise authentication fails closed.
func ofrepAPIKey(header http.Header) string {
	credentials := make([]string, 0, len(header.Values("Authorization"))+len(header.Values("X-API-Key")))
	for _, value := range header.Values("Authorization") {
		credential, ok := ofrepAuthorizationCredential(value)
		if !ok {
			return ""
		}
		if credential != "" {
			credentials = append(credentials, credential)
		}
	}
	for _, value := range header.Values("X-API-Key") {
		if credential := strings.TrimSpace(value); credential != "" {
			credentials = append(credentials, credential)
		}
	}
	if len(credentials) == 0 {
		return ""
	}
	for _, credential := range credentials[1:] {
		if credential != credentials[0] {
			return ""
		}
	}
	return credentials[0]
}

func ofrepAuthorizationCredential(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", true
	}
	fields := strings.Fields(value)
	if !strings.EqualFold(fields[0], "Bearer") {
		return value, true
	}
	if len(fields) != 2 {
		return "", false
	}
	return fields[1], true
}

func (s *grpcGatewayService) authorizeOFREP(
	w http.ResponseWriter,
	ctx context.Context,
) (*accountproto.EnvironmentAPIKey, bool) {
	envAPIKey, err := s.checkRequest(ctx, []accountproto.APIKey_Role{accountproto.APIKey_SDK_SERVER})
	if err == nil {
		return envAPIKey, true
	}
	grpcStatus := status.Convert(err)
	code := grpcStatus.Code()
	errorDetails := grpcStatus.Message()
	switch code {
	case codes.Unauthenticated, codes.PermissionDenied, codes.Canceled, codes.DeadlineExceeded:
	default:
		errorDetails = ofrepInternalErrorDetails
	}
	writeOFREPJSON(w, runtime.HTTPStatusFromCode(code), ofrepGeneralErrorResponse{ErrorDetails: errorDetails})
	return nil, false
}

func (s *grpcGatewayService) observeOFREPRequest(envAPIKey *accountproto.EnvironmentAPIKey, method string) {
	source := eventproto.SourceId_OPEN_FEATURE_OFREP.String()
	requestTotal.WithLabelValues(
		envAPIKey.Environment.OrganizationId,
		envAPIKey.ProjectId,
		envAPIKey.ProjectUrlCode,
		envAPIKey.Environment.Id,
		envAPIKey.Environment.UrlCode,
		method,
		source,
	).Inc()
}

func (s *grpcGatewayService) observeOFREPDuration(
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	start time.Time,
) {
	handledSecondsHistogram.WithLabelValues(
		envAPIKey.Environment.Id,
		eventproto.SourceId_OPEN_FEATURE_OFREP.String(),
		method,
	).Observe(time.Since(start).Seconds())
}

func (s *grpcGatewayService) loadOFREPFeatures(
	ctx context.Context,
	environmentID string,
) ([]*featureproto.Feature, error) {
	result, err := s.singleflightFetch(ctx, environmentID, func(ctx context.Context) (interface{}, error) {
		return s.getFeatures(ctx, environmentID)
	})
	if err != nil {
		if isCallerContextErr(err) {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, err
	}
	return s.filterOutArchivedFeatures(result.([]*featureproto.Feature)), nil
}

func (s *grpcGatewayService) evaluateOFREPSingle(
	ctx context.Context,
	envAPIKey *accountproto.EnvironmentAPIKey,
	features []*featureproto.Feature,
	user *userproto.User,
	key string,
) (*featureproto.Evaluation, error) {
	segmentUsers, segments, err := s.getSegmentUsersMap(ctx, features, envAPIKey.Environment.Id)
	if err != nil {
		return nil, err
	}
	evaluations, err := evaluation.NewEvaluator().EvaluateFeatures(features, user, segmentUsers, segments, "")
	if err != nil {
		return nil, err
	}
	return s.findEvaluation(evaluations.Evaluations, key)
}

func decodeOFREPUser(body io.Reader, key string) (*userproto.User, *ofrepEvaluationFailure) {
	request := &ofrepEvaluationRequest{}
	decoder := json.NewDecoder(body)
	decoder.UseNumber()
	if err := decoder.Decode(request); err != nil {
		return nil, &ofrepEvaluationFailure{Key: key, ErrorCode: ofrepErrorParse, ErrorDetails: err.Error()}
	}
	if err := ensureOFREPEOF(decoder); err != nil {
		return nil, &ofrepEvaluationFailure{Key: key, ErrorCode: ofrepErrorParse, ErrorDetails: err.Error()}
	}
	if len(request.Context) == 0 {
		return nil, &ofrepEvaluationFailure{
			Key: key, ErrorCode: ofrepErrorInvalidContext, ErrorDetails: "Context must be an object",
		}
	}

	contextDecoder := json.NewDecoder(bytes.NewReader(request.Context))
	contextDecoder.UseNumber()
	attributes := make(map[string]any)
	if err := contextDecoder.Decode(&attributes); err != nil || attributes == nil {
		return nil, &ofrepEvaluationFailure{
			Key: key, ErrorCode: ofrepErrorInvalidContext, ErrorDetails: "Context must be an object",
		}
	}
	targetingKey, ok := attributes["targetingKey"].(string)
	if !ok || strings.TrimSpace(targetingKey) == "" {
		return nil, &ofrepEvaluationFailure{
			Key: key, ErrorCode: ofrepErrorTargetingKeyMissing,
			ErrorDetails: "Context is missing a non-empty targetingKey",
		}
	}
	delete(attributes, "targetingKey")

	data := make(map[string]string, len(attributes))
	for name, value := range attributes {
		if stringValue, ok := value.(string); ok {
			data[name] = stringValue
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, &ofrepEvaluationFailure{
				Key: key, ErrorCode: ofrepErrorInvalidContext, ErrorDetails: "Context contains an invalid attribute",
			}
		}
		data[name] = string(encoded)
	}
	return &userproto.User{Id: targetingKey, Data: data}, nil
}

func ensureOFREPEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func newOFREPEvaluationSuccess(
	feature *featureproto.Feature,
	eval *featureproto.Evaluation,
) (ofrepEvaluationSuccess, error) {
	if eval.Reason == nil {
		return ofrepEvaluationSuccess{}, errors.New("evaluation reason is missing")
	}
	value, err := ofrepTypedValue(feature.VariationType, eval.VariationValue)
	if err != nil {
		return ofrepEvaluationSuccess{}, err
	}
	metadata := map[string]any{
		"featureVersion":  eval.FeatureVersion,
		"bucketeerReason": eval.Reason.Type.String(),
	}
	if eval.Reason.RuleId != "" {
		metadata["ruleId"] = eval.Reason.RuleId
	}
	return ofrepEvaluationSuccess{
		Key:      feature.Id,
		Value:    value,
		Reason:   ofrepReason(feature, eval.Reason),
		Variant:  eval.VariationId,
		Metadata: metadata,
	}, nil
}

func ofrepTypedValue(variationType featureproto.Feature_VariationType, value string) (any, error) {
	switch variationType {
	case featureproto.Feature_STRING:
		return value, nil
	case featureproto.Feature_BOOLEAN:
		var boolean bool
		if err := json.Unmarshal([]byte(value), &boolean); err != nil {
			return nil, fmt.Errorf("invalid boolean variation: %w", err)
		}
		return boolean, nil
	case featureproto.Feature_NUMBER:
		decoder := json.NewDecoder(strings.NewReader(value))
		decoder.UseNumber()
		var number any
		if err := decoder.Decode(&number); err != nil {
			return nil, fmt.Errorf("invalid number variation: %w", err)
		}
		if err := ensureOFREPEOF(decoder); err != nil {
			return nil, fmt.Errorf("invalid number variation: %w", err)
		}
		jsonNumber, ok := number.(json.Number)
		if !ok {
			return nil, errors.New("invalid number variation")
		}
		if _, err := strconv.ParseFloat(jsonNumber.String(), 64); err != nil {
			return nil, fmt.Errorf("invalid number variation: %w", err)
		}
		return jsonNumber, nil
	case featureproto.Feature_JSON, featureproto.Feature_YAML:
		decoder := json.NewDecoder(strings.NewReader(value))
		decoder.UseNumber()
		object := make(map[string]any)
		if err := decoder.Decode(&object); err != nil || object == nil {
			return nil, errors.New("variation value must be a JSON object")
		}
		if err := ensureOFREPEOF(decoder); err != nil {
			return nil, fmt.Errorf("invalid object variation: %w", err)
		}
		return object, nil
	default:
		return nil, errors.New("unknown variation type")
	}
}

func ofrepReason(feature *featureproto.Feature, reason *featureproto.Reason) string {
	if reason == nil {
		return ofrepReasonUnknown
	}
	switch reason.Type {
	case featureproto.Reason_TARGET, featureproto.Reason_PREREQUISITE:
		return ofrepReasonTargetingMatch
	case featureproto.Reason_OFF_VARIATION:
		return ofrepReasonDisabled
	case featureproto.Reason_RULE:
		for _, rule := range feature.Rules {
			if rule.Id == reason.RuleId && rule.Strategy != nil {
				if rule.Strategy.Type == featureproto.Strategy_ROLLOUT {
					return ofrepReasonSplit
				}
				return ofrepReasonTargetingMatch
			}
		}
		return ofrepReasonUnknown
	case featureproto.Reason_DEFAULT:
		if feature.DefaultStrategy == nil {
			return ofrepReasonUnknown
		}
		if feature.DefaultStrategy.Type == featureproto.Strategy_ROLLOUT {
			return ofrepReasonSplit
		}
		return ofrepReasonStatic
	default:
		return ofrepReasonUnknown
	}
}

func (s *grpcGatewayService) publishOFREPEvaluationEvent(
	ctx context.Context,
	environmentID string,
	user *userproto.User,
	eval *featureproto.Evaluation,
) {
	id, err := uuid.NewUUID()
	if err != nil {
		s.logger.Error("Failed to create OFREP evaluation event ID", zap.Error(err))
		return
	}
	evaluationEvent := &eventproto.EvaluationEvent{
		Timestamp:      time.Now().Unix(),
		FeatureId:      eval.FeatureId,
		FeatureVersion: eval.FeatureVersion,
		UserId:         user.Id,
		VariationId:    eval.VariationId,
		User:           user,
		Reason:         eval.Reason,
		SourceId:       eventproto.SourceId_OPEN_FEATURE_OFREP,
		SdkVersion:     ofrepVersion,
	}
	wrapped, err := anypb.New(evaluationEvent)
	if err != nil {
		s.logger.Error("Failed to marshal OFREP evaluation event", zap.Error(err))
		return
	}
	eventID := id.String()
	err = s.evaluationPublisher.Publish(ctx, &eventproto.Event{
		Id:            eventID,
		Event:         wrapped,
		EnvironmentId: environmentID,
	})
	if err == nil {
		eventCounter.WithLabelValues(callerGatewayService, typeEvaluation, codeOK).Inc()
		return
	}
	code := codeRepeatableError
	if errors.Is(err, publisher.ErrBadMessage) {
		code = codeNonRepeatableError
	}
	eventCounter.WithLabelValues(callerGatewayService, typeEvaluation, code).Inc()
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.logger.Error(
			"Failed to publish OFREP evaluation event",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentID", environmentID),
				zap.String("eventID", eventID),
			)...,
		)
	}
}

func (s *grpcGatewayService) writeOFREPEvaluationFailure(
	w http.ResponseWriter,
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	statusCode int,
	failure ofrepEvaluationFailure,
) {
	apiErrorCounter.WithLabelValues(
		envAPIKey.Environment.Id,
		eventproto.SourceId_OPEN_FEATURE_OFREP.String(),
		method,
	).Inc()
	writeOFREPJSON(w, statusCode, failure)
}

func (s *grpcGatewayService) writeOFREPBulkFailure(
	w http.ResponseWriter,
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	statusCode int,
	failure ofrepBulkEvaluationFailure,
) {
	apiErrorCounter.WithLabelValues(
		envAPIKey.Environment.Id,
		eventproto.SourceId_OPEN_FEATURE_OFREP.String(),
		method,
	).Inc()
	writeOFREPJSON(w, statusCode, failure)
}

func (s *grpcGatewayService) writeOFREPInternalError(
	w http.ResponseWriter,
	ctx context.Context,
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	err error,
) {
	apiErrorCounter.WithLabelValues(
		envAPIKey.Environment.Id,
		eventproto.SourceId_OPEN_FEATURE_OFREP.String(),
		method,
	).Inc()
	s.logger.Error(
		"Failed to evaluate OFREP request",
		log.FieldsFromIncomingContext(ctx).AddFields(
			zap.Error(err),
			zap.String("environmentID", envAPIKey.Environment.Id),
			zap.String("method", method),
		)...,
	)
	writeOFREPJSON(w, http.StatusInternalServerError, ofrepGeneralErrorResponse{
		ErrorDetails: ofrepInternalErrorDetails,
	})
}

func writeOFREPJSON(w http.ResponseWriter, statusCode int, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}

func ofrepETagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}
