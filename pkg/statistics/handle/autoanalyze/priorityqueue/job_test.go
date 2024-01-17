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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGenSQLForAnalyzeTable(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	expectedSQL := "analyze table %n.%n"
	expectedParams := []interface{}{"test_schema", "test_table"}

	sql, params := job.genSQLForAnalyzeTable()

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestGenSQLForAnalyzeIndex(t *testing.T) {
	job := &TableAnalysisJob{
		TableSchema: "test_schema",
		TableName:   "test_table",
	}

	index := "test_index"

	expectedSQL := "analyze table %n.%n index %n"
	expectedParams := []interface{}{"test_schema", "test_table", index}

	sql, params := job.genSQLForAnalyzeIndex(index)

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedParams, params)
}

func TestAnalyzeTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	job := &TableAnalysisJob{
		TableSchema:   "test",
		TableName:     "t",
		TableStatsVer: 2,
	}

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()
	job.analyzeTable(sctx, handle, dom.SysProcTracker())
	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetTableStats(tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
}
