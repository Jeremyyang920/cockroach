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
	"encoding/json"
	"fmt"
	"go/constant"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/jackc/pgx/v5"
)

type replicationProcessor struct {
	execinfra.ProcessorBase
	replicationSpec execinfrapb.ReplicationSpec
}

var _ execinfra.RowSource = (*replicationProcessor)(nil)

func (t *replicationProcessor) Start(ctx context.Context) {
	ctx = t.StartInternal(ctx, "replication")
	err := t.work(ctx)
	t.MoveToDraining(err)
}

type changeRecord struct {
	After    map[string]interface{}
	Before   map[string]interface{}
	Updated  string
	Resolved string
}

func (t *replicationProcessor) work(ctx context.Context) error {
	// time.Sleep(10 * time.Second)
	// log.Info(ctx, "making get request to server")
	// resp, err := http.Get("http://127.0.0.1:36573/hello")
	// if err != nil {
	// 	return err
	// }
	// log.Info(ctx, "after request")
	// body, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	return err
	// }
	// log.Infof(ctx, "body:%s", body)
	// return nil

	//internal executor
	//ie := t.FlowCtx.Cfg.DB
	conn, err := pgx.Connect(ctx, t.replicationSpec.ActiveReplicationDetails.TargetClusterConnStr)
	if err != nil {
		return err
	}
	if err := conn.Ping(ctx); err != nil {
		return err
	}
	log.Infof(ctx, "creating changefeed for table %s", t.replicationSpec.ActiveReplicationDetails.TableName)
	rows, err := conn.Query(ctx, fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s WITH updated, resolved, diff", t.replicationSpec.ActiveReplicationDetails.TableName))
	if err != nil {
		return err
	}

	defer func() { go rows.Close() }()
	var tableNameBytes []byte
	var changeJSON []byte
	var primaryKeyValuesJSON []byte
	if _, err = pgx.ForEachRow(rows, []any{&tableNameBytes, &primaryKeyValuesJSON, &changeJSON}, func() error {
		log.Infof(ctx, "received changefeed record: table_name:%s, changeJSON:%s, pk_value:%s", string(tableNameBytes), string(changeJSON), string(primaryKeyValuesJSON))
		record := changeRecord{}
		// Insert:
		// table_name:‹test›, changeJSON:‹{"after": {"a": 2, "b": "bcd"}, "updated": "1713975737350593000.0000000000"}›, pk_value:‹[2]›

		// Delete:
		// table_name:‹test›, changeJSON:‹{"after": null, "updated": "1713976192015763000.0000000000"}›, pk_value:‹[1]›

		// Update:
		// table_name:‹test›, changeJSON:‹{"after": {"a": 2, "b": "def"}, "updated": "1713976632922191000.0000000000"}›, pk_value:‹[2]›

		// Phantom Delete: (Maybe we don’t differentiate for now????)
		// table_name:‹test›, changeJSON:‹{"after": null, "updated": "1713978277822732000.0000000000"}›, pk_value:‹[1]›

		err := json.Unmarshal(changeJSON, &record)
		if err != nil {
			return err
		}
		log.Infof(ctx, "record:%#v", record)
		// If resolved isint filled in then we have some record, and if the clusterID isint already
		// filled in.
		if record.Resolved == "" {
			clusterID := t.EvalCtx.ClusterID

			q := generateQuery(string(tableNameBytes), string(primaryKeyValuesJSON), clusterID, record)
			if q == "" {
				// no op and do nothing. Assumes that it was a data loop and the record should be dropped.
				return nil
			}
			log.Infof(ctx, "query:%s", q)
			if err := t.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, t isql.Txn) error {
				// Hack, we need to know the db name. For now, fill it in since we can control it
				_, err := t.ExecEx(ctx, "replication-update", t.KV(), sessiondata.InternalExecutorOverride{Database: "movr"}, q)
				if err != nil {
					log.Infof(ctx, "error from exec:%s", err.Error())
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

// This is just to get up and running quickly. Ideally we maybe already have all the
// table descriptors and know what type is supposed to be unmarshalled into what
// so we don't have to do this dance of type conversions.
func generateQuery(tableName, pkValue string, clusterID uuid.UUID, record changeRecord) string {
	// if the after payload is filled in, it was either a fresh insert or update
	// if the clusterID is not filled in, then we know it came from the target side.
	if record.After != nil && record.After["clusterid"] == nil {
		query := &tree.Insert{}
		query.Table = tree.NewUnqualifiedTableName(tree.Name(tableName))

		query.Columns = make(tree.NameList, len(record.After))
		i := 0

		// Populate the columns and values clause
		valuesClause := &tree.ValuesClause{
			Rows: make([]tree.Exprs, 1),
		}
		exprs := make(tree.Exprs, len(record.After))
		i = 0
		for key, val := range record.After {
			query.Columns[i] = tree.Name(tree.NameString(key))
			// Hack for now.
			switch v := val.(type) {
			case int64:
				exprs[i] = tree.NewNumVal(constant.MakeInt64(v), strconv.Itoa(int(v)), false)
			case float64:
				exprs[i] = tree.NewNumVal(constant.MakeInt64(int64(v)), strconv.Itoa(int(v)), false)
			case string:
				exprs[i] = tree.NewStrVal(v)
			case nil:
				// hack as well. This won't work with columns that may actually be of nil value.
				// Probably would need to check for the hidden column name.
				exprs[i] = tree.NewDUuid(tree.DUuid{UUID: clusterID})
			}
			i++
		}

		valuesClause.Rows[0] = exprs
		query.Rows = &tree.Select{
			Select: valuesClause,
		}
		query.OnConflict = &tree.OnConflict{}
		query.Returning = &tree.NoReturningClause{}
		return query.String()
	}
	// Indicates a delete
	// We shouldnt delete again if the payload of before has a clusterID
	if record.Before != nil && record.After == nil && record.Before["clusterid"] == nil {
		query := &tree.Delete{
			Returning: &tree.NoReturningClause{},
		}
		query.Table = tree.NewUnqualifiedTableName(tree.Name(tableName))
		trimmedPk, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(pkValue, "["), "]"))
		if err != nil {
			return ""
		}
		query.Where = &tree.Where{
			Type: tree.AstWhere,
			Expr: &tree.ComparisonExpr{
				Operator: treecmp.MakeComparisonOperator(treecmp.EQ),
				Left:     tree.NewUnresolvedName(string(*tree.NewDString("a"))),
				Right:    tree.NewDInt(tree.DInt(trimmedPk)),
			},
		}
		return query.String()
	}
	return ""
}

func (t *replicationProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, t.DrainHelper()
}

func newReplicationProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ReplicationSpec,
) (execinfra.Processor, error) {
	replicationProcessor := &replicationProcessor{
		replicationSpec: spec,
	}
	if err := replicationProcessor.Init(
		ctx,
		replicationProcessor,
		&execinfrapb.PostProcessSpec{},
		[]*types.T{},
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return replicationProcessor, nil
}

func init() {
	rowexec.NewReplicationProcessor = newReplicationProcessor
}
