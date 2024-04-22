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
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type activeReplicationResumer struct {
	job    *jobs.Job
	server *http.Server
}

var _ jobs.Resumer = (*activeReplicationResumer)(nil)

func hello(w http.ResponseWriter, req *http.Request) {
	// Functions serving as handlers take a
	// `http.ResponseWriter` and a `http.Request` as
	// arguments. The response writer is used to fill in the
	// HTTP response. Here our simple response is just
	// "hello\n".
	log.Infof(req.Context(), "inside hello handler")
	fmt.Fprintf(w, "hello\n")
}

// Resume is part of the jobs.Resumer interface.
func (h *activeReplicationResumer) Resume(ctx context.Context, execCtx interface{}) error {
	details := h.job.Details().(jobspb.ActiveReplicationDetails)
	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	jobExecCtx := execCtx.(sql.JobExecContext)
	distSQLPlanner := jobExecCtx.DistSQLPlanner()
	evalCtx := jobExecCtx.ExtendedEvalContext()

	group := ctxgroup.WithContext(ctx)
	err := func() error {
		// Setup the active listner on a go routine
		// group.GoCtx(func(ctx context.Context) error {
		// 	server := &http.Server{
		// 		Addr: ":36573",
		// 	}
		// 	h.server = server
		// 	targetConnStr := details.TargetClusterConnStr
		// 	log.Infof(ctx, "starting replication from %s", targetConnStr)

		// 	http.HandleFunc("/hello", hello)
		// 	if err := server.ListenAndServe(); err != nil {
		// 		log.Errorf(ctx, "error during listen and serve: %#v", err.Error())
		// 	}
		// 	return nil
		// })

		// Alter the source table to add the hidden column.
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, t isql.Txn) error {
			_, err := t.Exec(ctx, "add-hidden-column", t.KV(), "ALTER TABLE movr.test ADD COLUMN IF NOT EXISTS clusterID UUID NOT VISIBLE")
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		planCtx, nodes, err := distSQLPlanner.SetupAllNodesPlanning(ctx, evalCtx, execCfg)
		if err != nil {
			return err
		}
		// Setup a one-stage plan with one proc per input spec.
		processorCorePlacements := make([]physicalplan.ProcessorCorePlacement, len(nodes))
		jobID := h.job.ID()
		for i := range nodes {
			processorCorePlacements[i].SQLInstanceID = nodes[i]
			processorCorePlacements[i].Core.Replication = &execinfrapb.ReplicationSpec{
				JobID:                    jobID,
				ActiveReplicationDetails: details,
			}
		}

		physicalPlan := planCtx.NewPhysicalPlan()
		physicalPlan.AddNoInputStage(
			processorCorePlacements,
			execinfrapb.PostProcessSpec{},
			[]*types.T{},
			execinfrapb.Ordering{},
		)
		physicalPlan.PlanToStreamColMap = []int{}
		metadataCallbackWriter := sql.NewMetadataOnlyMetadataCallbackWriter()

		sql.FinalizePlan(ctx, planCtx, physicalPlan)
		distSQLReceiver := sql.MakeDistSQLReceiver(
			ctx,
			metadataCallbackWriter,
			tree.Rows,
			execCfg.RangeDescriptorCache,
			nil, /* txn */
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer distSQLReceiver.Release()
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		distSQLPlanner.Run(
			ctx,
			planCtx,
			nil, /* txn */
			physicalPlan,
			distSQLReceiver,
			&evalCtxCopy,
			nil, /* finishedSetupFn */
		)

		return metadataCallbackWriter.Err()
	}()
	if err != nil {
		return err
	}
	return group.Wait()
}

// OnFailOrCancel implements jobs.Resumer interface
func (h *activeReplicationResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	if h.server != nil {
		return h.server.Shutdown(ctx)
	}
	return nil
}

// CollectProfile implements jobs.Resumer interface
func (h *activeReplicationResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeActiveReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &activeReplicationResumer{
				job: job,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
