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

package util

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/model"
)

// Event contains the information of a ddl event that is used to update stats.
type Event struct {
	// For different ddl types, the following fields are used.
	// They have different meanings for different ddl types.
	// Please do **not** use these fields directly, use the corresponding
	// NewXXXEvent functions instead.
	tableInfo    *model.TableInfo
	partInfo     *model.PartitionInfo
	oldTableInfo *model.TableInfo
	oldPartInfo  *model.PartitionInfo
	columnInfos  []*model.ColumnInfo

	// We expose the action type field to the outside, because some ddl events
	// do not need other fields.
	// If your ddl event needs other fields, please add them with the
	// corresponding NewXXXEvent function and give them clear names.
	Tp model.ActionType
}

// NewCreateTableEvent creates a new ddl event that creates a table.
func NewCreateTableEvent(
	newTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:        model.ActionCreateTable,
		tableInfo: newTableInfo,
	}
}

// GetCreateTableInfo gets the table info of the table that is created.
func (e *Event) GetCreateTableInfo() (newTableInfo *model.TableInfo) {
	return e.tableInfo
}

// NewTruncateTableEvent creates a new ddl event that truncates a table.
func NewTruncateTableEvent(
	newTableInfo *model.TableInfo,
	droppedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionTruncateTable,
		tableInfo:    newTableInfo,
		oldTableInfo: droppedTableInfo,
	}
}

// GetTruncateTableInfo gets the table info of the table that is truncated.
func (e *Event) GetTruncateTableInfo() (newTableInfo *model.TableInfo, droppedTableInfo *model.TableInfo) {
	return e.tableInfo, e.oldTableInfo
}

// NewDropTableEvent creates a new ddl event that drops a table.
func NewDropTableEvent(
	droppedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionDropTable,
		oldTableInfo: droppedTableInfo,
	}
}

// GetDropTableInfo gets the table info of the table that is dropped.
func (e *Event) GetDropTableInfo() (newTableInfo *model.TableInfo) {
	return e.oldTableInfo
}

// NewAddColumnEvent creates a new ddl event that
// adds a column.
func NewAddColumnEvent(
	newTableInfo *model.TableInfo,
	newColumnInfo []*model.ColumnInfo,
) *Event {
	return &Event{
		Tp:          model.ActionAddColumn,
		tableInfo:   newTableInfo,
		columnInfos: newColumnInfo,
	}
}

// GetAddColumnInfo gets the table info of the table that is added a column.
func (e *Event) GetAddColumnInfo() (newTableInfo *model.TableInfo, newColumnInfo []*model.ColumnInfo) {
	return e.tableInfo, e.columnInfos
}

// NewModifyColumnEvent creates a new ddl event that
// modifies a column.
func NewModifyColumnEvent(
	newTableInfo *model.TableInfo,
	modifiedColumnInfo []*model.ColumnInfo,
) *Event {
	return &Event{
		Tp:          model.ActionModifyColumn,
		tableInfo:   newTableInfo,
		columnInfos: modifiedColumnInfo,
	}
}

// GetModifyColumnInfo gets the table info of the table that is modified a column.
func (e *Event) GetModifyColumnInfo() (newTableInfo *model.TableInfo, modifiedColumnInfo []*model.ColumnInfo) {
	return e.tableInfo, e.columnInfos
}

// NewAddTablePartitionEvent creates a new ddl event that adds partitions.
func NewAddTablePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:        model.ActionAddTablePartition,
		tableInfo: globalTableInfo,
		partInfo:  addedPartInfo,
	}
}

// GetAddTablePartitionInfo gets the table info of the table that is added partitions.
func (e *Event) GetAddTablePartitionInfo() (globalTableInfo *model.TableInfo, addedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.partInfo
}

// NewDropPartitionEvent creates a new ddl event that drops partitions.
func NewDropPartitionEvent(
	globalTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionDropTablePartition,
		tableInfo:   globalTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetDropPartitionInfo gets the table info of the table that is dropped partitions.
func (e *Event) GetDropPartitionInfo() (globalTableInfo *model.TableInfo, droppedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.oldPartInfo
}

// NewExchangePartitionEvent creates a new ddl event that exchanges a partition.
func NewExchangePartitionEvent(
	globalTableInfo *model.TableInfo,
	exchangedPartInfo *model.PartitionInfo,
	exchangedTableInfo *model.TableInfo,
) *Event {
	return &Event{
		Tp:           model.ActionExchangeTablePartition,
		tableInfo:    globalTableInfo,
		partInfo:     exchangedPartInfo,
		oldTableInfo: exchangedTableInfo,
	}
}

func (e *Event) getExchangePartitionInfo() (
	globalTableInfo *model.TableInfo,
	exchangedPartInfo *model.PartitionInfo,
	exchangedTableInfo *model.TableInfo,
) {
	return e.tableInfo, e.partInfo, e.oldTableInfo
}

// NewReorganizePartitionEvent creates a new ddl event that reorganizes partitions.
// We also use it for increasing or decreasing the number of hash partitions.
func NewReorganizePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionReorganizePartition,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetReorganizePartitionInfo gets the table info of the table that is reorganized partitions.
func (e *Event) GetReorganizePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewTruncatePartitionEvent creates a new ddl event that truncates partitions.
func NewTruncatePartitionEvent(
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionTruncateTablePartition,
		tableInfo:   globalTableInfo,
		partInfo:    addedPartInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetTruncatePartitionInfo gets the table info of the table that is truncated partitions.
func (e *Event) GetTruncatePartitionInfo() (
	globalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.partInfo, e.oldPartInfo
}

// NewAddPartitioningEvent creates a new ddl event that converts a single table to a partitioned table.
// For example, `alter table t partition by range (c1) (partition p1 values less than (10))`.
func NewAddPartitioningEvent(
	newGlobalTableInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:        model.ActionAlterTablePartitioning,
		tableInfo: newGlobalTableInfo,
		partInfo:  addedPartInfo,
	}
}

// GetAddPartitioningInfo gets the table info of the table that is converted to a partitioned table.
func (e *Event) GetAddPartitioningInfo() (newGlobalTableInfo *model.TableInfo, addedPartInfo *model.PartitionInfo) {
	return e.tableInfo, e.partInfo
}

// NewRemovePartitioningEvent creates a new ddl event that converts a partitioned table to a single table.
// For example, `alter table t remove partitioning`.
func NewRemovePartitioningEvent(
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) *Event {
	return &Event{
		Tp:          model.ActionRemovePartitioning,
		tableInfo:   newSingleTableInfo,
		oldPartInfo: droppedPartInfo,
	}
}

// GetRemovePartitioningInfo gets the table info of the table that is converted to a single table.
func (e *Event) GetRemovePartitioningInfo() (
	newSingleTableInfo *model.TableInfo,
	droppedPartInfo *model.PartitionInfo,
) {
	return e.tableInfo, e.oldPartInfo
}

// String implements fmt.Stringer interface.
func (e *Event) String() string {
	ret := fmt.Sprintf("(Event Type: %s", e.Tp)
	if e.tableInfo != nil {
		ret += fmt.Sprintf(", Table ID: %d, Table Name: %s", e.tableInfo.ID, e.tableInfo.Name)
	}
	if e.partInfo != nil {
		ids := make([]int64, 0, len(e.partInfo.Definitions))
		for _, def := range e.partInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Partition IDs: %v", ids)
	}
	if e.oldTableInfo != nil {
		ret += fmt.Sprintf(", Old Table ID: %d, Old Table Name: %s", e.oldTableInfo.ID, e.oldTableInfo.Name)
	}
	if e.oldPartInfo != nil {
		ids := make([]int64, 0, len(e.oldPartInfo.Definitions))
		for _, def := range e.oldPartInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Old Partition IDs: %v", ids)
	}
	for _, columnInfo := range e.columnInfos {
		ret += fmt.Sprintf(", Column ID: %d, Column Name: %s", columnInfo.ID, columnInfo.Name)
	}

	return ret
}
