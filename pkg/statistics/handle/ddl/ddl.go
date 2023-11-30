// Copyright 2023 PingCAP, Inc.
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

package ddl

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

type ddlHandlerImpl struct {
	ddlEventCh         chan *util.DDLEvent
	statsWriter        types.StatsReadWriter
	statsHandler       types.StatsHandle
	globalStatsHandler types.StatsGlobal
}

// NewDDLHandler creates a new ddl handler.
func NewDDLHandler(
	statsWriter types.StatsReadWriter,
	statsHandler types.StatsHandle,
	globalStatsHandler types.StatsGlobal,
) types.DDL {
	return &ddlHandlerImpl{
		ddlEventCh:         make(chan *util.DDLEvent, 1000),
		statsWriter:        statsWriter,
		statsHandler:       statsHandler,
		globalStatsHandler: globalStatsHandler,
	}
}

// HandleDDLEvent begins to process a ddl task.
func (h *ddlHandlerImpl) HandleDDLEvent(t *util.DDLEvent) error {
	switch t.GetType() {
	case model.ActionCreateTable:
		newTableInfo := t.GetCreateTableInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionTruncateTable:
		newTableInfo, _ := t.GetTruncateTableInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertTableStats2KV(newTableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionDropTable:
		droppedTableInfo := t.GetDropTableInfo()
		ids, err := h.getInitStateTableIDs(droppedTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.ResetTableStats2KVForDrop(id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn:
		newTableInfo, newColumnInfo := t.GetAddColumnInfo()
		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertColStats2KV(id, newColumnInfo); err != nil {
				return err
			}
		}
	case model.ActionModifyColumn:
		newTableInfo, modifiedColumnInfo := t.GetModifyColumnInfo()

		ids, err := h.getInitStateTableIDs(newTableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.statsWriter.InsertColStats2KV(id, modifiedColumnInfo); err != nil {
				return err
			}
		}
	case model.ActionAddTablePartition:
		globalTableInfo, addedPartitionInfo := t.GetAddPartitionInfo()
		for _, def := range addedPartitionInfo.Definitions {
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionTruncateTablePartition:
		globalTableInfo, addedPartInfo, _ := t.GetTruncatePartitionInfo()
		for _, def := range addedPartInfo.Definitions {
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionDropTablePartition:
		globalTableInfo, droppedPartitionInfo := t.GetDropPartitionInfo()

		count := int64(0)
		for _, def := range droppedPartitionInfo.Definitions {
			// Get the count and modify count of the partition.
			stats := h.statsHandler.GetPartitionStats(globalTableInfo, def.ID)
			if stats.Pseudo {
				se, err := h.statsHandler.SPool().Get()
				if err != nil {
					return errors.Trace(err)
				}
				sctx := se.(sessionctx.Context)
				is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
				schema, _ := is.SchemaByTable(globalTableInfo)
				logutil.StatsLogger().Warn(
					"drop partition with pseudo stats, "+
						"usually it won't happen because we always load stats when initializing the handle",
					zap.String("schema", schema.Name.O),
					zap.String("table", globalTableInfo.Name.O),
					zap.String("partition", def.Name.O),
				)
			} else {
				count += stats.RealtimeCount
			}
			// Always reset the partition stats.
			if err := h.statsWriter.ResetTableStats2KVForDrop(def.ID); err != nil {
				return err
			}
		}
		if count != 0 {
			// Because we drop the partition, we should subtract the count from the global stats.
			delta := -count
			if err := h.statsWriter.UpdateStatsMetaDelta(
				globalTableInfo.ID, count, delta,
			); err != nil {
				return err
			}
		}
	case model.ActionExchangeTablePartition:
		if err := h.onExchangeAPartition(t); err != nil {
			return err
		}
	case model.ActionReorganizePartition:
		globalTableInfo, addedPartInfo, _ := t.GetReorganizePartitionInfo()
		for _, def := range addedPartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
			// Do not update global stats, since the data have not changed!
		}
	case model.ActionAlterTablePartitioning:
		globalTableInfo, addedPartInfo := t.GetAddPartitioningInfo()
		// Add partitioning
		for _, def := range addedPartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
				return err
			}
		}
		// Change id for global stats, since the data has not changed!
		// Note that globalTableInfo is the new table info
		// and addedPartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		return h.statsWriter.ChangeGlobalStatsID(addedPartInfo.NewTableID, globalTableInfo.ID)
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note that newSingleTableInfo is the new table info
		// and droppedPartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		newSingleTableInfo, droppedPartInfo := t.GetRemovePartitioningInfo()
		return h.statsWriter.ChangeGlobalStatsID(droppedPartInfo.NewTableID, newSingleTableInfo.ID)
	case model.ActionFlashbackCluster:
		return h.statsWriter.UpdateStatsVersion()
	}
	return nil
}

func (h *ddlHandlerImpl) onExchangeAPartition(t *util.DDLEvent) error {
	globalTableInfo, originalPartInfo, originalTableInfo := t.GetExchangePartitionInfo()
	// Get the count of the partition.
	partCount, partModifyCount, err := h.statsWriter.StatsMetaCountAndModifyCount(
		// You only can exchange one partition at a time.
		originalPartInfo.Definitions[0].ID,
	)
	if err != nil {
		return err
	}
	// Get the count of the table.
	tableCount, tableModifyCount, err := h.statsWriter.StatsMetaCountAndModifyCount(
		originalTableInfo.ID,
	)
	if err != nil {
		return err
	}

	// Compute the difference between the partition and the table.
	// For instance, consider the following scenario:
	//
	// | Entity    | modify_count                       | count |
	// |-----------|------------------------------------|-------|
	// | Partition | 10                                 | 100   |
	// | Table     | 20                                 | 200   |
	// | Global    | 30 (20 from other partitions + 10) | 300   |
	//
	// After the partition exchange, the partition becomes the table and vice versa:
	//
	// | Entity    | modify_count | count |
	// |-----------|--------------|-------|
	// | Partition | 20           | 200   |
	// | Table     | 10           | 100   |
	// | Global    | 30           | 300   |
	//
	// The count difference is 200 - 100 = 100, which is also considered as the table's delta.
	delta := tableCount - partCount
	count := int64(math.Abs(float64(delta)))

	// Adjust the delta to account for the modify count of the partition.
	// For example, if the partition's modify_count is 10 and the count difference is 100,
	// the actual delta after the exchange is 100 - 10 = 90.
	// When updating the global stats, the count increases by 100 and the modify_count increases by 90.
	// The final modify_count becomes 30 (original) + 90 (delta) = 120, which is the correct value.
	//
	// | Entity    | modify_count                             | count |
	// |-----------|------------------------------------------|-------|
	// | Partition | 20                                       | 200   |
	// | Table     | 10                                       | 100   |
	// | Global    | 120 (20 from other partitions + 10 + 90) | 400   |
	// If we do not do this, the modify_count will be 30 (original) + 100 (delta) = 130, which is incorrect.
	if delta > 0 {
		delta -= partModifyCount
	} else {
		delta += partModifyCount
	}
	// Adjust the delta to account for the modify count of the table.
	// For example, if the table's modify_count is 20 and the count difference is 100,
	// the actual delta after the exchange is 100 + 20 = 120.
	// When updating the global stats, the count increases by 100 and the modify_count increases by 120.
	// The final modify_count becomes 20 (original) + 120 (delta) = 140, which is the correct value.
	//
	// | Entity    | modify_count                                     | count |
	// |-----------|--------------------------------------------------|-------|
	// | Partition | 20                                               | 200   |
	// | Table     | 10                                               | 100   |
	// | Global    | 140 (20 from other partitions + 20 + 10 + 90)    | 400   |
	// If we do not do this, the modify_count will be 20 (original) + 10 + 90 (delta) = 120, which is incorrect.
	if delta > 0 {
		delta += tableModifyCount
	} else {
		delta -= tableModifyCount
	}
	// Update the global stats.
	if count != 0 {
		if err := h.statsWriter.UpdateStatsMetaDelta(
			globalTableInfo.ID, count, delta,
		); err != nil {
			return err
		}
		logutil.StatsLogger.Info(
			"Update global stats after exchange partition",
			zap.Int64("tableID", globalTableInfo.ID),
			zap.Int64("count", count),
			zap.Int64("delta", delta),
			zap.Int64("partitionID", originalPartInfo.Definitions[0].ID),
			zap.String("partitionName", originalPartInfo.Definitions[0].Name.O),
			zap.Int64("partitionCount", partCount),
			zap.Int64("tableID", originalTableInfo.ID),
			zap.String("tableName", originalTableInfo.Name.O),
			zap.Int64("tableCount", tableCount),
		)
	}
	return nil
}

func (h *ddlHandlerImpl) getInitStateTableIDs(tblInfo *model.TableInfo) (ids []int64, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}, nil
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	pruneMode, err := util.GetCurrentPruneMode(h.statsHandler.SPool())
	if err != nil {
		return nil, err
	}
	if variable.PartitionPruneMode(pruneMode) == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids, nil
}

// DDLEventCh returns ddl events channel in handle.
func (h *ddlHandlerImpl) DDLEventCh() chan *util.DDLEvent {
	return h.ddlEventCh
}
