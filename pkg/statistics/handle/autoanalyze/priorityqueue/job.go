// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

import (
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
)

// TableAnalysisJob defines the structure for table analysis job information.
type TableAnalysisJob struct {
	TableID     int64
	TableSchema string
	TableName   string
	// Only set when table's indexes need to be analyzed.
	Indexes       []string
	TableStatsVer int
	// Only set when table's partitions need to be analyzed.
	Partitions []string
	// Only set when partitions's indexes need to be analyzed.
	PartitionIndexes map[string][]string
	ChangePercentage float64
	Weight           float64
}

func (j *TableAnalysisJob) Execute(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) error {
	se, err := statsHandle.SPool().Get()
	if err != nil {
		return err
	}
	defer statsHandle.SPool().Put(se)

	sctx := se.(sessionctx.Context)
	if len(j.PartitionIndexes) > 0 {
		j.analyzePartitionIndexes(sctx, statsHandle, sysProcTracker)
		return nil
	}
	if len(j.Partitions) > 0 {
		j.analyzePartitions(sctx, statsHandle, sysProcTracker)
		return nil
	}
	if len(j.Indexes) > 0 {
		for _, index := range j.Indexes {
			sql, params := j.genSQLForAnalyzeIndex(index)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
		}
		return nil
	}
	sql, params := j.genSQLForAnalyzeTable()
	exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	return nil
}

func (j *TableAnalysisJob) analyzePartitions(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := j.Partitions
	for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
		start := i
		end := start + analyzePartitionBatchSize
		if end >= len(needAnalyzePartitionNames) {
			end = len(needAnalyzePartitionNames)
		}

		sql := getPartitionSQL("analyze table %n.%n partition", "", end-start)
		params := append([]interface{}{j.TableSchema, j.TableName}, []interface{}{needAnalyzePartitionNames[start:end]}...)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
	}
}

func (j *TableAnalysisJob) analyzePartitionIndexes(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) {
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())

	for indexName, partitionNames := range j.PartitionIndexes {
		for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(partitionNames) {
				end = len(partitionNames)
			}

			sql := getPartitionSQL("analyze table %n.%n partition", " index %n", end-start)
			params := append([]interface{}{j.TableSchema, j.TableName}, []interface{}{partitionNames[start:end]}...)
			params = append(params, indexName)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, j.TableStatsVer, sql, params...)
		}
	}
}

func getPartitionSQL(prefix, suffix string, numPartitions int) string {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(prefix)
	for i := 0; i < numPartitions; i++ {
		if i != 0 {
			sqlBuilder.WriteString(",")
		}
		sqlBuilder.WriteString(" %n")
	}
	sqlBuilder.WriteString(suffix)
	return sqlBuilder.String()
}

// genSQLForAnalyzeTable generates the SQL for analyzing the specified table.
func (j *TableAnalysisJob) genSQLForAnalyzeTable() (string, []interface{}) {
	sql := "analyze table %n.%n"
	params := []interface{}{j.TableSchema, j.TableName}

	return sql, params
}

// genSQLForAnalyzeIndex generates the SQL for analyzing the specified index.
func (j *TableAnalysisJob) genSQLForAnalyzeIndex(index string) (string, []interface{}) {
	sql := "analyze table %n.%n index %n"
	params := []interface{}{j.TableSchema, j.TableName, index}

	return sql, params
}
