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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	cachev3mock "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3/mock"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	environmentproto "github.com/bucketeer-io/bucketeer/v2/proto/environment"
	gwproto "github.com/bucketeer-io/bucketeer/v2/proto/gateway"
)

const exportTestAPIKeySecret = "super-secret-api-key-value"

func exportTestEnvironmentAPIKey(role accountproto.APIKey_Role, apiKeyDisabled, envDisabled bool) *accountproto.EnvironmentAPIKey {
	return &accountproto.EnvironmentAPIKey{
		Environment: &environmentproto.EnvironmentV2{
			Id:             "env-id-0",
			Name:           "Production",
			UrlCode:        "production",
			ProjectId:      "project-id-0",
			OrganizationId: "org-id-0",
		},
		ProjectUrlCode:      "project-url-code-0",
		EnvironmentDisabled: envDisabled,
		ApiKey: &accountproto.APIKey{
			Id:       "api-key-id-0",
			ApiKey:   exportTestAPIKeySecret,
			Role:     role,
			Disabled: apiKeyDisabled,
		},
	}
}

func TestGrpcGetExportContext(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	patterns := []struct {
		desc        string
		setup       func(*grpcGatewayService)
		expected    *gwproto.GetExportContextResponse
		expectedErr error
	}{
		{
			desc: "fails: SDK client key is rejected",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_SDK_CLIENT, false, false), nil)
			},
			expected:    nil,
			expectedErr: ErrBadRole,
		},
		{
			desc: "fails: SDK server key is rejected",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_SDK_SERVER, false, false), nil)
			},
			expected:    nil,
			expectedErr: ErrBadRole,
		},
		{
			desc: "fails: disabled API key is rejected",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, true, false), nil)
			},
			expected:    nil,
			expectedErr: ErrDisabledAPIKey,
		},
		{
			desc: "fails: disabled environment is rejected",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, true), nil)
			},
			expected:    nil,
			expectedErr: ErrDisabledAPIKey,
		},
		{
			desc: "success: read-only key returns the bound environment context",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)
			},
			expected: &gwproto.GetExportContextResponse{
				ContractVersion:    "1",
				CredentialScope:    "environment",
				OrganizationId:     "org-id-0",
				ProjectId:          "project-id-0",
				ProjectUrlCode:     "project-url-code-0",
				EnvironmentId:      "env-id-0",
				EnvironmentName:    "Production",
				EnvironmentUrlCode: "production",
				Capabilities: []string{
					"feature_flags", "segments", "experiments", "goals", "audit_logs", "code_references",
				},
			},
			expectedErr: nil,
		},
		{
			desc: "success: write key is also allowed",
			setup: func(gs *grpcGatewayService) {
				gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
					exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_WRITE, false, false), nil)
			},
			expected: &gwproto.GetExportContextResponse{
				ContractVersion:    "1",
				CredentialScope:    "environment",
				OrganizationId:     "org-id-0",
				ProjectId:          "project-id-0",
				ProjectUrlCode:     "project-url-code-0",
				EnvironmentId:      "env-id-0",
				EnvironmentName:    "Production",
				EnvironmentUrlCode: "production",
				Capabilities: []string{
					"feature_flags", "segments", "experiments", "goals", "audit_logs", "code_references",
				},
			},
			expectedErr: nil,
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			gs := newGrpcGatewayServiceWithMock(t, mockController)
			p.setup(gs)
			ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
				"authorization": []string{"test-key"},
			})
			actual, err := gs.GetExportContext(ctx, &gwproto.GetExportContextRequest{})
			assert.Equal(t, p.expectedErr, err, "%s", p.desc)
			if p.expected == nil {
				assert.Nil(t, actual, "%s", p.desc)
				return
			}
			assert.Equal(t, p.expected, actual, "%s", p.desc)
		})
	}
}

// The response is what a connector stores as its environment lineage, so nothing
// derived from the credential itself may appear in it.
func TestGrpcGetExportContextLeaksNoSecret(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	gs := newGrpcGatewayServiceWithMock(t, mockController)
	gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
		exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)

	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
		"authorization": []string{exportTestAPIKeySecret},
	})
	resp, err := gs.GetExportContext(ctx, &gwproto.GetExportContextRequest{})
	require.NoError(t, err)

	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), exportTestAPIKeySecret)
	assert.NotContains(t, string(encoded), "api-key-id-0")
}

// A connector trusts capabilities as permission to walk an endpoint, so an entry
// added here without a tested public list endpoint would send it at a 404.
func TestExportCapabilitiesAreStable(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{
		"feature_flags", "segments", "experiments", "goals", "audit_logs", "code_references",
	}, exportCapabilities)
}
