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

	"go.uber.org/zap"

	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	gwproto "github.com/bucketeer-io/bucketeer/v2/proto/gateway"
)

const (
	// exportContractVersion is the version of the analytics export contract this
	// server implements. Bump it only for a breaking change: connectors gate on it.
	exportContractVersion = "1"

	// exportCredentialScopeEnvironment is reported for an EnvironmentAPIKey, which is
	// structurally bound to exactly one environment.
	exportCredentialScopeEnvironment = "environment"
)

// exportCapabilities lists the resources a connector may export. Add an entry only
// when the matching public list endpoint exists and its pagination contract is tested,
// because connectors treat this list as permission to walk that endpoint.
var exportCapabilities = []string{
	"feature_flags",
	"segments",
	"experiments",
	"goals",
	"audit_logs",
	"code_references",
}

func (s *grpcGatewayService) GetExportContext(
	ctx context.Context,
	_ *gwproto.GetExportContextRequest,
) (*gwproto.GetExportContextResponse, error) {
	envAPIKey, err := s.checkRequest(ctx, []accountproto.APIKey_Role{
		accountproto.APIKey_PUBLIC_API_READ_ONLY,
		accountproto.APIKey_PUBLIC_API_WRITE,
		accountproto.APIKey_PUBLIC_API_ADMIN,
	})
	if err != nil {
		s.logger.Error("Failed to check GetExportContext request",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
			)...,
		)
		return nil, err
	}

	requestTotal.WithLabelValues(
		envAPIKey.Environment.OrganizationId, envAPIKey.ProjectId, envAPIKey.ProjectUrlCode,
		envAPIKey.Environment.Id, envAPIKey.Environment.UrlCode, methodGetExportContext, "").Inc()

	// Every value below comes from the environment the presented key is bound to, never
	// from the request, so a caller cannot widen its own scope.
	return &gwproto.GetExportContextResponse{
		ContractVersion:    exportContractVersion,
		CredentialScope:    exportCredentialScopeEnvironment,
		OrganizationId:     envAPIKey.Environment.OrganizationId,
		ProjectId:          envAPIKey.Environment.ProjectId,
		ProjectUrlCode:     envAPIKey.ProjectUrlCode,
		EnvironmentId:      envAPIKey.Environment.Id,
		EnvironmentName:    envAPIKey.Environment.Name,
		EnvironmentUrlCode: envAPIKey.Environment.UrlCode,
		Capabilities:       exportCapabilities,
	}, nil
}
