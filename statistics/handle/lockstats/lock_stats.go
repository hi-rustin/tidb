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

package lockstats

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	lockAction     = "locking"
	unlockAction   = "unlocking"
	lockedStatus   = "locked"
	unlockedStatus = "unlocked"
)

var (
	// Stats logger.
	statsLogger = logutil.BgLogger().With(zap.String("category", "stats"))
	// useCurrentSession to make sure the sql is executed in current session.
	useCurrentSession = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}
	insertSQL         = "INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?"
)

// AddLockedTables add locked tables id to store.
// - exec: sql executor.
// - tids: table ids of which will be locked.
// - pids: partition ids of which will be locked.
// - tables: table names of which will be locked.
// Return the message of skipped tables and error.
func AddLockedTables(exec sqlexec.RestrictedSQLExecutor, tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	err := startTransaction(ctx, exec)
	if err != nil {
		return "", err
	}
	defer func() {
		// Commit transaction.
		err = finishTransaction(ctx, exec, err)
	}()

	// Load tables to check duplicate before insert.
	lockedTables, err := QueryLockedTables(exec)
	if err != nil {
		return "", err
	}

	skippedTables := make([]string, 0, len(tables))
	statsLogger.Info("lock table", zap.Int64s("tableIDs", tids))

	// Insert locked tables.
	checkedTables := GetLockedTables(lockedTables, tids...)
	for i, tid := range tids {
		if _, ok := checkedTables[tid]; !ok {
			if err := insertIntoStatsTableLocked(ctx, exec, tid); err != nil {
				return "", err
			}
		} else {
			skippedTables = append(skippedTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}

	// Insert related partitions while don't warning duplicate partitions.
	checkedTables = GetLockedTables(lockedTables, pids...)
	for _, pid := range pids {
		if _, ok := checkedTables[pid]; !ok {
			if err := insertIntoStatsTableLocked(ctx, exec, pid); err != nil {
				return "", err
			}
		}
	}

	msg := generateSkippedTablesMessage(tids, skippedTables, lockAction, lockedStatus)
	// Note: defer commit transaction, so we can't use `return nil` here.
	return msg, err
}

func generateSkippedTablesMessage(tids []int64, dupTables []string, action, status string) string {
	if len(dupTables) > 0 {
		tables := strings.Join(dupTables, ", ")
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(dupTables) {
				msg = fmt.Sprintf("skip %s %s tables: %s, other tables %s successfully", action, status, tables, status)
			} else {
				msg = fmt.Sprintf("skip %s %s tables: %s", action, status, tables)
			}
		} else {
			msg = fmt.Sprintf("skip %s %s table: %s", action, status, tables)
		}
		return msg
	}

	return ""
}

func insertIntoStatsTableLocked(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, tid int64) error {
	_, _, err := exec.ExecRestrictedSQL(
		ctx,
		useCurrentSession,
		insertSQL, tid, tid,
	)
	if err != nil {
		logutil.BgLogger().Error("error occurred when insert mysql.stats_table_locked", zap.String("category", "stats"), zap.Error(err))
		return err
	}
	return nil
}
