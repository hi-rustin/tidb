// Copyright 2021 PingCAP, Inc.
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

package handle

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

type statsWrapper struct {
	col *statistics.Column
	idx *statistics.Index
}

// StatsLoad is used to load stats concurrently
type StatsLoad struct {
	NeededItemsCh  chan *NeededItemTask
	TimeoutItemsCh chan *NeededItemTask
	Singleflight   singleflight.Group
	SubCtxs        []sessionctx.Context
	sync.Mutex
}

// NeededItemTask represents one needed column/indices with expire time.
type NeededItemTask struct {
	ToTimeout   time.Time
	ResultCh    chan stmtctx.StatsLoadResult
	TableItemID model.TableItemID
}

// SendLoadRequests send neededColumns requests
func (h *Handle) SendLoadRequests(sc *stmtctx.StatementContext, neededHistItems []model.TableItemID, timeout time.Duration) error {
	remainedItems := h.removeHistLoadedColumns(neededHistItems)

	failpoint.Inject("assertSyncLoadItems", func(val failpoint.Value) {
		if sc.OptimizeTracer != nil {
			count := val.(int)
			if len(remainedItems) != count {
				panic("remained items count wrong")
			}
		}
	})

	if len(remainedItems) <= 0 {
		return nil
	}
	sc.StatsLoad.Timeout = timeout
	sc.StatsLoad.NeededItems = remainedItems
	sc.StatsLoad.ResultCh = make(chan stmtctx.StatsLoadResult, len(remainedItems))
	tasks := make([]*NeededItemTask, 0)
	for _, item := range remainedItems {
		task := &NeededItemTask{
			TableItemID: item,
			ToTimeout:   time.Now().Local().Add(timeout),
			ResultCh:    sc.StatsLoad.ResultCh,
		}
		tasks = append(tasks, task)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for _, task := range tasks {
		select {
		case h.StatsLoad.NeededItemsCh <- task:
			continue
		case <-timer.C:
			return errors.New("sync load stats channel is full and timeout sending task to channel")
		}
	}
	sc.StatsLoad.LoadStartTime = time.Now()
	return nil
}

// SyncWaitStatsLoad sync waits loading of neededColumns and return false if timeout
func (*Handle) SyncWaitStatsLoad(sc *stmtctx.StatementContext) error {
	if len(sc.StatsLoad.NeededItems) <= 0 {
		return nil
	}
	var errorMsgs []string
	defer func() {
		if len(errorMsgs) > 0 {
			logutil.BgLogger().Warn("SyncWaitStatsLoad meets error",
				zap.Strings("errors", errorMsgs))
		}
		sc.StatsLoad.NeededItems = nil
	}()
	resultCheckMap := map[model.TableItemID]struct{}{}
	for _, col := range sc.StatsLoad.NeededItems {
		resultCheckMap[col] = struct{}{}
	}
	metrics.SyncLoadCounter.Inc()
	timer := time.NewTimer(sc.StatsLoad.Timeout)
	defer timer.Stop()
	for {
		select {
		case result, ok := <-sc.StatsLoad.ResultCh:
			if !ok {
				return errors.New("sync load stats channel closed unexpectedly")
			}
			if result.HasError() {
				errorMsgs = append(errorMsgs, result.ErrorMsg())
			}
			delete(resultCheckMap, result.Item)
			if len(resultCheckMap) == 0 {
				metrics.SyncLoadHistogram.Observe(float64(time.Since(sc.StatsLoad.LoadStartTime).Milliseconds()))
				return nil
			}
		case <-timer.C:
			metrics.SyncLoadTimeoutCounter.Inc()
			return errors.New("sync load stats timeout")
		}
	}
}

// removeHistLoadedColumns removed having-hist columns based on neededColumns and statsCache.
func (h *Handle) removeHistLoadedColumns(neededItems []model.TableItemID) []model.TableItemID {
	remainedItems := make([]model.TableItemID, 0, len(neededItems))
	for _, item := range neededItems {
		tbl, ok := h.Get(item.TableID)
		if !ok {
			continue
		}
		if item.IsIndex {
			remainedItems = append(remainedItems, item)
			continue
		}
		colHist, ok := tbl.Columns[item.ID]
		if ok && colHist.IsStatsInitialized() && !colHist.IsFullLoad() {
			remainedItems = append(remainedItems, item)
		}
	}
	return remainedItems
}

// AppendNeededItem appends needed columns/indices to ch, it is only used for test
func (h *Handle) AppendNeededItem(task *NeededItemTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case h.StatsLoad.NeededItemsCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

var errExit = errors.New("Stop loading since domain is closed")

// SubLoadWorker loads hist data for each column
func (h *Handle) SubLoadWorker(sctx sessionctx.Context, exit chan struct{}, exitWg *util.WaitGroupEnhancedWrapper) {
	defer func() {
		exitWg.Done()
		logutil.BgLogger().Info("SubLoadWorker exited.")
	}()
	// if the last task is not successfully handled in last round for error or panic, pass it to this round to retry
	var lastTask *NeededItemTask
	for {
		task, err := h.HandleOneTask(sctx, lastTask, exit)
		lastTask = task
		if err != nil {
			switch err {
			case errExit:
				return
			default:
				// To avoid the thundering herd effect
				// thundering herd effect: Everyone tries to retry a large number of requests simultaneously when a problem occurs.
				r := rand.Intn(500)
				time.Sleep(h.Lease()/10 + time.Duration(r)*time.Microsecond)
				continue
			}
		}
	}
}

// HandleOneTask handles last task if not nil, else handle a new task from chan, and return current task if fail somewhere.
func (h *Handle) HandleOneTask(sctx sessionctx.Context, lastTask *NeededItemTask, exit chan struct{}) (task *NeededItemTask, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			logutil.BgLogger().Error("stats loading panicked", zap.Any("error", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	if lastTask == nil {
		task, err = h.drainColTask(exit)
		if err != nil {
			if err != errExit {
				logutil.BgLogger().Error("Fail to drain task for stats loading.", zap.Error(err))
			}
			return task, err
		}
	} else {
		task = lastTask
	}
	resultChan := h.StatsLoad.Singleflight.DoChan(task.TableItemID.Key(), func() (any, error) {
		return h.handleOneItemTask(sctx, task)
	})
	timeout := time.Until(task.ToTimeout)
	select {
	case result := <-resultChan:
		if result.Err == nil {
			slr := result.Val.(*stmtctx.StatsLoadResult)
			if slr.Error != nil {
				return task, slr.Error
			}
			task.ResultCh <- *slr
			return nil, nil
		}
		return task, result.Err
	case <-time.After(timeout):
		return task, nil
	}
}

func (h *Handle) handleOneItemTask(sctx sessionctx.Context, task *NeededItemTask) (result *stmtctx.StatsLoadResult, err error) {
	defer func() {
		// recover for each task, worker keeps working
		if r := recover(); r != nil {
			logutil.BgLogger().Error("handleOneItemTask panicked", zap.Any("recover", r), zap.Stack("stack"))
			err = errors.Errorf("stats loading panicked: %v", r)
		}
	}()
	result = &stmtctx.StatsLoadResult{Item: task.TableItemID}
	item := result.Item
	tbl, ok := h.Get(item.TableID)
	if !ok {
		return result, nil
	}
	wrapper := &statsWrapper{}
	if item.IsIndex {
		index, ok := tbl.Indices[item.ID]
		if !ok || index.IsFullLoad() {
			return result, nil
		}
		wrapper.idx = index
	} else {
		col, ok := tbl.Columns[item.ID]
		if !ok || col.IsFullLoad() {
			return result, nil
		}
		wrapper.col = col
	}
	t := time.Now()
	needUpdate := false
	wrapper, err = h.readStatsForOneItem(sctx, item, wrapper)
	if err != nil {
		result.Error = err
		return result, err
	}
	if item.IsIndex {
		if wrapper.idx != nil {
			needUpdate = true
		}
	} else {
		if wrapper.col != nil {
			needUpdate = true
		}
	}
	metrics.ReadStatsHistogram.Observe(float64(time.Since(t).Milliseconds()))
	if needUpdate && h.updateCachedItem(item, wrapper.col, wrapper.idx) {
		return result, nil
	}
	return nil, nil
}

// readStatsForOneItem reads hist for one column/index, TODO load data via kv-get asynchronously
func (*Handle) readStatsForOneItem(sctx sessionctx.Context, item model.TableItemID, w *statsWrapper) (*statsWrapper, error) {
	failpoint.Inject("mockReadStatsForOnePanic", nil)
	failpoint.Inject("mockReadStatsForOneFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail ReadStatsForOne error"))
		}
	})
	c := w.col
	index := w.idx
	loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
	var hg *statistics.Histogram
	var err error
	isIndexFlag := int64(0)
	if item.IsIndex {
		isIndexFlag = 1
	}
	if item.IsIndex {
		hg, err = storage.HistogramFromStorage(sctx, item.TableID, item.ID, types.NewFieldType(mysql.TypeBlob), index.Histogram.NDV, int(isIndexFlag), index.LastUpdateVersion, index.NullCount, index.TotColSize, index.Correlation)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		hg, err = storage.HistogramFromStorage(sctx, item.TableID, item.ID, &c.Info.FieldType, c.Histogram.NDV, int(isIndexFlag), c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var cms *statistics.CMSketch
	var topN *statistics.TopN
	cms, topN, err = storage.CMSketchAndTopNFromStorage(sctx, item.TableID, isIndexFlag, item.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = storage.FMSketchFromStorage(sctx, item.TableID, isIndexFlag, item.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	rows, _, err := utilstats.ExecRows(sctx, "select stats_ver from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?", item.TableID, item.ID, int(isIndexFlag))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", item.TableID),
			zap.Int64("hist_id", item.ID), zap.Bool("is_index", item.IsIndex))
		return nil, errors.Trace(fmt.Errorf("fail to get stats version for this histogram, table_id:%v, hist_id:%v, is_index:%v", item.TableID, item.ID, item.IsIndex))
	}
	statsVer := rows[0].GetInt64(0)
	if item.IsIndex {
		idxHist := &statistics.Index{
			Histogram:  *hg,
			CMSketch:   cms,
			TopN:       topN,
			FMSketch:   fms,
			Info:       index.Info,
			StatsVer:   statsVer,
			Flag:       index.Flag,
			PhysicalID: index.PhysicalID,
		}
		if statsVer != statistics.Version0 {
			idxHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
		}
		index.LastAnalyzePos.Copy(&idxHist.LastAnalyzePos)
		w.idx = idxHist
	} else {
		colHist := &statistics.Column{
			PhysicalID: item.TableID,
			Histogram:  *hg,
			Info:       c.Info,
			CMSketch:   cms,
			TopN:       topN,
			FMSketch:   fms,
			IsHandle:   c.IsHandle,
			StatsVer:   statsVer,
		}
		if colHist.StatsAvailable() {
			colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
		}
		w.col = colHist
	}
	return w, nil
}

// drainColTask will hang until a column task can return, and either task or error will be returned.
func (h *Handle) drainColTask(exit chan struct{}) (*NeededItemTask, error) {
	// select NeededColumnsCh firstly, if no task, then select TimeoutColumnsCh
	for {
		select {
		case <-exit:
			return nil, errExit
		case task, ok := <-h.StatsLoad.NeededItemsCh:
			if !ok {
				return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
			}
			// if the task has already timeout, no sql is sync-waiting for it,
			// so do not handle it just now, put it to another channel with lower priority
			if time.Now().After(task.ToTimeout) {
				h.writeToTimeoutChan(h.StatsLoad.TimeoutItemsCh, task)
				continue
			}
			return task, nil
		case task, ok := <-h.StatsLoad.TimeoutItemsCh:
			select {
			case <-exit:
				return nil, errExit
			case task0, ok0 := <-h.StatsLoad.NeededItemsCh:
				if !ok0 {
					return nil, errors.New("drainColTask: cannot read from NeededColumnsCh, maybe the chan is closed")
				}
				// send task back to TimeoutColumnsCh and return the task drained from NeededColumnsCh
				h.writeToTimeoutChan(h.StatsLoad.TimeoutItemsCh, task)
				return task0, nil
			default:
				if !ok {
					return nil, errors.New("drainColTask: cannot read from TimeoutColumnsCh, maybe the chan is closed")
				}
				// NeededColumnsCh is empty now, handle task from TimeoutColumnsCh
				return task, nil
			}
		}
	}
}

// writeToTimeoutChan writes in a nonblocking way, and if the channel queue is full, it's ok to drop the task.
func (*Handle) writeToTimeoutChan(taskCh chan *NeededItemTask, task *NeededItemTask) {
	select {
	case taskCh <- task:
	default:
	}
}

// writeToChanWithTimeout writes a task to a channel and blocks until timeout.
func (*Handle) writeToChanWithTimeout(taskCh chan *NeededItemTask, task *NeededItemTask, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case taskCh <- task:
	case <-timer.C:
		return errors.New("Channel is full and timeout writing to channel")
	}
	return nil
}

// writeToResultChan safe-writes with panic-recover so one write-fail will not have big impact.
func (*Handle) writeToResultChan(resultCh chan stmtctx.StatsLoadResult, rs stmtctx.StatsLoadResult) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("writeToResultChan panicked", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	select {
	case resultCh <- rs:
	default:
	}
}

// updateCachedItem updates the column/index hist to global statsCache.
func (h *Handle) updateCachedItem(item model.TableItemID, colHist *statistics.Column, idxHist *statistics.Index) (updated bool) {
	h.StatsLoad.Lock()
	defer h.StatsLoad.Unlock()
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok := h.Get(item.TableID)
	if !ok {
		return true
	}
	if !item.IsIndex && colHist != nil {
		c, ok := tbl.Columns[item.ID]
		if !ok || c.IsFullLoad() {
			return true
		}
		tbl = tbl.Copy()
		tbl.Columns[c.ID] = colHist
	} else if item.IsIndex && idxHist != nil {
		index, ok := tbl.Indices[item.ID]
		if !ok || index.IsFullLoad() {
			return true
		}
		tbl = tbl.Copy()
		tbl.Indices[item.ID] = idxHist
	}
	h.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	return true
}