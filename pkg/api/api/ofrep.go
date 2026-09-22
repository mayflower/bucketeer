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
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opencensus.io/plugin/ochttp/propagation/b3"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"go.uber.org/zap"
	gmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	evaluation "github.com/bucketeer-io/bucketeer/v2/evaluation/go"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	"github.com/bucketeer-io/bucketeer/v2/pkg/rpc"
	rpcmetadata "github.com/bucketeer-io/bucketeer/v2/pkg/rpc/metadata"
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
	ofrepSpanPrefix               = "bucketeerGRPCGatewayService."
	ofrepAuthenticateChallenge    = `Bearer realm="bucketeer-ofrep"`
	// ofrepMaxBodyBytes matches the gRPC server's default receive limit that
	// bounds the generated routes.
	ofrepMaxBodyBytes = 4 << 20
)

var (
	ofrepSourceID = eventproto.SourceId_OPEN_FEATURE_OFREP.String()

	ofrepSpanPropagators = []propagation.HTTPFormat{&tracecontext.HTTPFormat{}, &b3.HTTPFormat{}}
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

// ofrepEvaluationFailure is the per-flag error body. Without a key it is also
// the bulk evaluation error body.
type ofrepEvaluationFailure struct {
	Key          string `json:"key,omitempty"`
	ErrorCode    string `json:"errorCode"`
	ErrorDetails string `json:"errorDetails,omitempty"`
	// status overrides the 400 a request failure normally maps to.
	status int
}

func (f ofrepEvaluationFailure) httpStatus() int {
	if f.status != 0 {
		return f.status
	}
	return http.StatusBadRequest
}

type ofrepBulkEvaluationSuccess struct {
	Flags []any `json:"flags"`
}

type ofrepGeneralErrorResponse struct {
	ErrorDetails string `json:"errorDetails,omitempty"`
}

// GrpcGatewayService serves Bucketeer's gRPC API and direct HTTP routes.
type GrpcGatewayService interface {
	rpc.Service
	RegisterOFREPHandlers(*runtime.ServeMux) error
	// ShutdownOFREPExposures drains queued OFREP exposure events until ctx
	// is done, then stops the publication workers.
	ShutdownOFREPExposures(context.Context)
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
	ctx, span := ofrepIncomingContext(r, methodOFREPEvaluateFlag)
	defer span.End()
	envAPIKey, ok := s.authorizeOFREP(w, ctx)
	if !ok {
		return
	}
	startTime := time.Now()
	s.observeOFREPRequest(envAPIKey, methodOFREPEvaluateFlag)
	defer s.observeOFREPDuration(envAPIKey, methodOFREPEvaluateFlag, startTime)

	key := pathParams["key"]
	user, failure := decodeOFREPUser(http.MaxBytesReader(w, r.Body, ofrepMaxBodyBytes))
	if failure != nil {
		failure.Key = key
		s.writeOFREPFailure(w, envAPIKey, methodOFREPEvaluateFlag, failure.httpStatus(), *failure)
		return
	}

	features, err := s.loadOFREPFeatures(ctx, envAPIKey.Environment.Id)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	feature, err := s.findFeature(features, key)
	if err != nil {
		s.writeOFREPFailure(w, envAPIKey, methodOFREPEvaluateFlag, http.StatusNotFound, ofrepEvaluationFailure{
			Key:          key,
			ErrorCode:    ofrepErrorFlagNotFound,
			ErrorDetails: fmt.Sprintf("Flag %q was not found", key),
		})
		return
	}
	targetFeatures, err := s.getTargetFeatures(features, key)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	evaluations, err := s.evaluateOFREPFeatures(ctx, envAPIKey.Environment.Id, targetFeatures, user)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	eval, err := s.findEvaluation(evaluations, key)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlag, err)
		return
	}
	response, err := newOFREPEvaluationSuccess(feature, eval)
	if err != nil {
		s.writeOFREPFailure(w, envAPIKey, methodOFREPEvaluateFlag, http.StatusBadRequest, ofrepEvaluationFailure{
			Key:          key,
			ErrorCode:    ofrepErrorParse,
			ErrorDetails: err.Error(),
		})
		return
	}

	// Exposure delivery is best effort and never delays the response.
	if event, err := newOFREPExposureEvent(envAPIKey.Environment.Id, user, eval); err != nil {
		s.logger.Error("Failed to build OFREP exposure event", zap.Error(err))
	} else {
		s.enqueueOFREPExposure(event)
	}
	writeOFREPJSON(w, http.StatusOK, response)
}

func (s *grpcGatewayService) handleOFREPEvaluateFlags(
	w http.ResponseWriter,
	r *http.Request,
	_ map[string]string,
) {
	ctx, span := ofrepIncomingContext(r, methodOFREPEvaluateFlags)
	defer span.End()
	envAPIKey, ok := s.authorizeOFREP(w, ctx)
	if !ok {
		return
	}
	startTime := time.Now()
	s.observeOFREPRequest(envAPIKey, methodOFREPEvaluateFlags)
	defer s.observeOFREPDuration(envAPIKey, methodOFREPEvaluateFlags, startTime)

	user, failure := decodeOFREPUser(http.MaxBytesReader(w, r.Body, ofrepMaxBodyBytes))
	if failure != nil {
		s.writeOFREPFailure(w, envAPIKey, methodOFREPEvaluateFlags, failure.httpStatus(), *failure)
		return
	}

	features, err := s.loadOFREPFeatures(ctx, envAPIKey.Environment.Id)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}
	evaluations, err := s.evaluateOFREPFeatures(ctx, envAPIKey.Environment.Id, features, user)
	if err != nil {
		s.writeOFREPInternalError(w, ctx, envAPIKey, methodOFREPEvaluateFlags, err)
		return
	}

	evaluationsByKey := make(map[string]*featureproto.Evaluation, len(evaluations))
	for _, eval := range evaluations {
		evaluationsByKey[eval.FeatureId] = eval
	}
	// loadOFREPFeatures returns a private copy, so it can be sorted in place.
	sort.Slice(features, func(i, j int) bool { return features[i].Id < features[j].Id })
	flags := make([]any, 0, len(features))
	for _, feature := range features {
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

func ofrepIncomingContext(r *http.Request, method string) (context.Context, *trace.Span) {
	ctx, span := startOFREPSpan(r, ofrepSpanPrefix+method)
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
	for _, propagator := range ofrepSpanPropagators {
		if parent, ok := propagator.SpanContextFromRequest(r); ok {
			return trace.StartSpanWithRemoteParent(
				r.Context(), spanName, parent, trace.WithSpanKind(trace.SpanKindServer),
			)
		}
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
	statusCode, errorDetails := ofrepAuthorizationError(err)
	if statusCode == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", ofrepAuthenticateChallenge)
	}
	writeOFREPJSON(w, statusCode, ofrepGeneralErrorResponse{ErrorDetails: errorDetails})
	return nil, false
}

// ofrepAuthorizationError maps the shared request checker's result onto
// OFREP's HTTP contract without changing the checker or its gRPC codes:
// 401 for a credential Bucketeer cannot use, 403 for a usable key without
// the required role. ErrDisabledAPIKey also covers a disabled environment;
// OFREP treats both as an unusable environment-bound credential.
func ofrepAuthorizationError(err error) (int, string) {
	switch {
	case errors.Is(err, ErrMissingAPIKey), errors.Is(err, ErrInvalidAPIKey), errors.Is(err, ErrDisabledAPIKey):
		return http.StatusUnauthorized, status.Convert(err).Message()
	case errors.Is(err, ErrBadRole):
		return http.StatusForbidden, status.Convert(err).Message()
	case isCallerContextErr(err):
		grpcStatus := status.Convert(err)
		return runtime.HTTPStatusFromCode(grpcStatus.Code()), grpcStatus.Message()
	default:
		return http.StatusInternalServerError, ofrepInternalErrorDetails
	}
}

func (s *grpcGatewayService) observeOFREPRequest(envAPIKey *accountproto.EnvironmentAPIKey, method string) {
	requestTotal.WithLabelValues(
		envAPIKey.Environment.OrganizationId,
		envAPIKey.ProjectId,
		envAPIKey.ProjectUrlCode,
		envAPIKey.Environment.Id,
		envAPIKey.Environment.UrlCode,
		method,
		ofrepSourceID,
	).Inc()
}

func (s *grpcGatewayService) observeOFREPDuration(
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	start time.Time,
) {
	handledSecondsHistogram.WithLabelValues(
		envAPIKey.Environment.Id,
		ofrepSourceID,
		method,
	).Observe(time.Since(start).Seconds())
}

func (s *grpcGatewayService) observeOFREPError(envAPIKey *accountproto.EnvironmentAPIKey, method string) {
	apiErrorCounter.WithLabelValues(envAPIKey.Environment.Id, ofrepSourceID, method).Inc()
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
			return nil, translateCallerCanceledErr(ctx, err)
		}
		return nil, err
	}
	return s.filterOutArchivedFeatures(result.([]*featureproto.Feature)), nil
}

func (s *grpcGatewayService) evaluateOFREPFeatures(
	ctx context.Context,
	environmentID string,
	features []*featureproto.Feature,
	user *userproto.User,
) ([]*featureproto.Evaluation, error) {
	segmentUsers, segments, err := s.getSegmentUsersMap(ctx, features, environmentID)
	if err != nil {
		return nil, err
	}
	evaluations, err := evaluation.NewEvaluator().EvaluateFeatures(features, user, segmentUsers, segments, "")
	if err != nil {
		return nil, err
	}
	return evaluations.Evaluations, nil
}

// decodeOFREPUser converts an OFREP evaluation context into a Bucketeer user.
// The returned failure carries no flag key; the caller adds it when the
// response format requires one.
func decodeOFREPUser(body io.Reader) (*userproto.User, *ofrepEvaluationFailure) {
	request := &ofrepEvaluationRequest{}
	if err := decodeOFREPJSON(body, request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &ofrepEvaluationFailure{
				ErrorCode:    ofrepErrorGeneral,
				ErrorDetails: fmt.Sprintf("Request body exceeds %d bytes", tooLarge.Limit),
				status:       http.StatusRequestEntityTooLarge,
			}
		}
		return nil, &ofrepEvaluationFailure{ErrorCode: ofrepErrorParse, ErrorDetails: err.Error()}
	}
	invalidContext := &ofrepEvaluationFailure{
		ErrorCode: ofrepErrorInvalidContext, ErrorDetails: "Context must be an object",
	}
	if len(request.Context) == 0 {
		return nil, invalidContext
	}
	attributes := make(map[string]any)
	if err := decodeOFREPJSON(bytes.NewReader(request.Context), &attributes); err != nil || attributes == nil {
		return nil, invalidContext
	}
	targetingKey, ok := attributes["targetingKey"].(string)
	if !ok || strings.TrimSpace(targetingKey) == "" {
		return nil, &ofrepEvaluationFailure{
			ErrorCode: ofrepErrorTargetingKeyMissing, ErrorDetails: "Context is missing a non-empty targetingKey",
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
				ErrorCode: ofrepErrorInvalidContext, ErrorDetails: "Context contains an invalid attribute",
			}
		}
		data[name] = string(encoded)
	}
	return &userproto.User{Id: targetingKey, Data: data}, nil
}

// decodeOFREPJSON strictly decodes exactly one JSON value, keeping numbers as
// json.Number so they are re-emitted with their original precision.
func decodeOFREPJSON(r io.Reader, v any) error {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	var extra any
	switch err := decoder.Decode(&extra); err {
	case io.EOF:
		return nil
	case nil:
		return errors.New("request body must contain exactly one JSON value")
	default:
		return err
	}
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
		switch value {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, errors.New("invalid boolean variation")
		}
	case featureproto.Feature_NUMBER:
		return ofrepNumber(value)
	case featureproto.Feature_JSON, featureproto.Feature_YAML:
		object := make(map[string]any)
		if err := decodeOFREPJSON(strings.NewReader(value), &object); err != nil || object == nil {
			return nil, errors.New("variation value must be a JSON object")
		}
		return object, nil
	default:
		return nil, errors.New("unknown variation type")
	}
}

func ofrepNumber(value string) (json.Number, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return "", errors.New("invalid number variation")
	}
	if isOFREPJSONNumber(value) {
		return json.Number(value), nil
	}
	if normalized, ok := normalizeOFREPDecimal(value); ok && isOFREPJSONNumber(normalized) {
		return json.Number(normalized), nil
	}
	// ParseFloat also accepts hexadecimal floating-point syntax. JSON does not,
	// so emit its finite numeric value using a valid JSON representation.
	return json.Number(strconv.FormatFloat(parsed, 'g', -1, 64)), nil
}

// isOFREPJSONNumber reports whether value is a JSON number literal. Callers
// pass values that strconv.ParseFloat accepted, so surrounding whitespace is
// already excluded.
func isOFREPJSONNumber(value string) bool {
	if value == "" {
		return false
	}
	if first := value[0]; first != '-' && (first < '0' || first > '9') {
		return false
	}
	return json.Valid([]byte(value))
}

// normalizeOFREPDecimal converts the additional finite decimal syntax accepted
// by strconv.ParseFloat into JSON number syntax without passing through float64.
func normalizeOFREPDecimal(value string) (string, bool) {
	value = strings.ReplaceAll(value, "_", "")
	sign := ""
	if strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") {
		sign = value[:1]
		value = value[1:]
	}
	if strings.HasPrefix(strings.ToLower(value), "0x") {
		return "", false
	}
	if sign == "+" {
		sign = ""
	}

	mantissa := value
	exponent := ""
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		mantissa = value[:index]
		exponent = value[index:]
	}
	if strings.HasPrefix(mantissa, ".") {
		mantissa = "0" + mantissa
	}
	if strings.HasSuffix(mantissa, ".") {
		mantissa += "0"
	}

	integer := mantissa
	fraction := ""
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		integer = mantissa[:index]
		fraction = mantissa[index:]
	}
	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}
	return sign + integer + fraction + exponent, true
}

func ofrepReason(feature *featureproto.Feature, reason *featureproto.Reason) string {
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

func (s *grpcGatewayService) writeOFREPFailure(
	w http.ResponseWriter,
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	statusCode int,
	failure ofrepEvaluationFailure,
) {
	s.observeOFREPError(envAPIKey, method)
	writeOFREPJSON(w, statusCode, failure)
}

func (s *grpcGatewayService) writeOFREPInternalError(
	w http.ResponseWriter,
	ctx context.Context,
	envAPIKey *accountproto.EnvironmentAPIKey,
	method string,
	err error,
) {
	if contextErr := ctxAlreadyDoneErr(ctx); contextErr != nil {
		grpcStatus := status.Convert(contextErr)
		writeOFREPJSON(w, runtime.HTTPStatusFromCode(grpcStatus.Code()), ofrepGeneralErrorResponse{
			ErrorDetails: grpcStatus.Message(),
		})
		return
	}
	s.observeOFREPError(envAPIKey, method)
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

// ofrepETagMatches compares a strong server ETag against an If-None-Match
// header, treating a weak client validator as a match on the same value.
func ofrepETagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}
