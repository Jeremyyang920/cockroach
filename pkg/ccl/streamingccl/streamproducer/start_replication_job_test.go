// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamproducer

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartReplicationJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	source := tc.Server(0)
	registry := source.JobRegistry().(*jobs.Registry)

	jr := jobs.Record{
		Description:   "Start Replication Job",
		Username:      username.MakeSQLUsernameFromPreNormalizedString("user"),
		DescriptorIDs: []descpb.ID{3, 2, 1},
		Details:       jobspb.ActiveReplicationDetails{TargetClusterConnStr: "posgres://root@127.0.0.1:26257/defaultdb"},
		Progress:      jobspb.ActiveReplicationProgress{},
	}
	job, err := registry.CreateAdoptableJobWithTxn(ctx, jr, registry.MakeJobID(), nil)
	require.NoError(t, err)
	assert.Equal(t, job.Status(), jobs.StatusRunning)
	// err = registry.PauseRequested(ctx, nil, job.ID(), "test")
	// registry.NotifyToResume(ctx, job.ID())
	require.NoError(t, job.NoTxn().CheckStatus(ctx))
	require.NoError(t, job.NoTxn().CancelRequested(ctx))
}
