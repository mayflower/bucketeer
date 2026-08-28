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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	featureclientmock "github.com/bucketeer-io/bucketeer/v2/pkg/feature/client/mock"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
	gwproto "github.com/bucketeer-io/bucketeer/v2/proto/gateway"

	cachev3mock "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3/mock"
)

// Before the export contract, the public envelope dropped the internal service's
// cursor, so a connector could not walk past the first page.
func TestGrpcListFeaturesReturnsCursorEnvelope(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	patterns := []struct {
		desc     string
		internal *featureproto.ListFeaturesResponse
		expected *gwproto.ListFeaturesResponse
	}{
		{
			desc: "single page: empty cursor ends the walk",
			internal: &featureproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}},
				Cursor:     "",
				TotalCount: 1,
			},
			expected: &gwproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}},
				Cursor:     "",
				TotalCount: 1,
			},
		},
		{
			desc: "first of several pages: cursor is passed through",
			internal: &featureproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}, {Id: "id-1"}},
				Cursor:     "2",
				TotalCount: 5,
			},
			expected: &gwproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}, {Id: "id-1"}},
				Cursor:     "2",
				TotalCount: 5,
			},
		},
		{
			desc: "final page: cursor empties even though rows were returned",
			internal: &featureproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-4"}},
				Cursor:     "",
				TotalCount: 5,
			},
			expected: &gwproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-4"}},
				Cursor:     "",
				TotalCount: 5,
			},
		},
		{
			desc: "page exactly the size of the page limit still carries a cursor",
			internal: &featureproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}, {Id: "id-1"}},
				Cursor:     "2",
				TotalCount: 2,
			},
			expected: &gwproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{{Id: "id-0"}, {Id: "id-1"}},
				Cursor:     "2",
				TotalCount: 2,
			},
		},
		{
			desc: "empty page",
			internal: &featureproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{},
				Cursor:     "",
				TotalCount: 0,
			},
			expected: &gwproto.ListFeaturesResponse{
				Features:   []*featureproto.Feature{},
				Cursor:     "",
				TotalCount: 0,
			},
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			gs := newGrpcGatewayServiceWithMock(t, mockController)
			gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
				exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)
			gs.featureClient.(*featureclientmock.MockClient).EXPECT().ListFeatures(
				gomock.Any(), gomock.Any(),
			).Return(p.internal, nil)

			ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
				"authorization": []string{"test-key"},
			})
			actual, err := gs.ListFeatures(ctx, &gwproto.ListFeaturesRequest{PageSize: 2})
			require.NoError(t, err)
			assert.Equal(t, p.expected, actual, "%s", p.desc)
		})
	}
}

// A connector walks pages by feeding the previous response's cursor back in. This
// asserts the request parameters survive the gateway hop, not just the response.
func TestGrpcListFeaturesForwardsCursorAndFilters(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	gs := newGrpcGatewayServiceWithMock(t, mockController)
	gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
		exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)

	var got *featureproto.ListFeaturesRequest
	gs.featureClient.(*featureclientmock.MockClient).EXPECT().ListFeatures(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(_ context.Context, req *featureproto.ListFeaturesRequest, _ ...interface{}) (*featureproto.ListFeaturesResponse, error) {
		got = req
		return &featureproto.ListFeaturesResponse{Cursor: "", TotalCount: 0}, nil
	})

	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
		"authorization": []string{"test-key"},
	})
	_, err := gs.ListFeatures(ctx, &gwproto.ListFeaturesRequest{
		PageSize:       50,
		Cursor:         "100",
		OrderBy:        featureproto.ListFeaturesRequest_NAME,
		OrderDirection: featureproto.ListFeaturesRequest_DESC,
	})
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, int64(50), got.PageSize)
	assert.Equal(t, "100", got.Cursor)
	assert.Equal(t, featureproto.ListFeaturesRequest_NAME, got.OrderBy)
	assert.Equal(t, featureproto.ListFeaturesRequest_DESC, got.OrderDirection)
	// The environment is taken from the authenticated key, never from the request.
	assert.Equal(t, "env-id-0", got.EnvironmentId)
}

// Adding fields 2 and 3 must not disturb field 1 for clients that only read it.
func TestGrpcListFeaturesRemainsCompatibleForFeaturesOnlyClients(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	gs := newGrpcGatewayServiceWithMock(t, mockController)
	gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
		exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)
	gs.featureClient.(*featureclientmock.MockClient).EXPECT().ListFeatures(
		gomock.Any(), gomock.Any(),
	).Return(&featureproto.ListFeaturesResponse{
		Features:   []*featureproto.Feature{{Id: "id-0", Enabled: true}, {Id: "id-1", Archived: true}},
		Cursor:     "2",
		TotalCount: 2,
	}, nil)

	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
		"authorization": []string{"test-key"},
	})
	resp, err := gs.ListFeatures(ctx, &gwproto.ListFeaturesRequest{})
	require.NoError(t, err)

	require.Len(t, resp.Features, 2)
	assert.Equal(t, "id-0", resp.Features[0].Id)
	assert.True(t, resp.Features[0].Enabled)
	assert.Equal(t, "id-1", resp.Features[1].Id)
	assert.True(t, resp.Features[1].Archived)
}

// A nil response from the internal service must not surface as a nil-pointer panic.
func TestGrpcListFeaturesNilResponse(t *testing.T) {
	t.Parallel()
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	gs := newGrpcGatewayServiceWithMock(t, mockController)
	gs.environmentAPIKeyCache.(*cachev3mock.MockEnvironmentAPIKeyCache).EXPECT().Get(gomock.Any()).Return(
		exportTestEnvironmentAPIKey(accountproto.APIKey_PUBLIC_API_READ_ONLY, false, false), nil)
	gs.featureClient.(*featureclientmock.MockClient).EXPECT().ListFeatures(
		gomock.Any(), gomock.Any(),
	).Return(nil, nil)

	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD{
		"authorization": []string{"test-key"},
	})
	resp, err := gs.ListFeatures(ctx, &gwproto.ListFeaturesRequest{})
	assert.Nil(t, resp)
	assert.Equal(t, errInternal, err)
}
