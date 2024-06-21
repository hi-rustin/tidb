package refresher

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

var jobs sync.Map

type Worker struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	jobChan        chan priorityqueue.AnalysisJob
	exitChan       chan struct{}
	wg             util.WaitGroupWrapper
	jobCnt         atomic.Int64
}

func NewWorker(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker, jobChan chan priorityqueue.AnalysisJob, concurrency int) *Worker {
	result := &Worker{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		jobChan:        jobChan,
		exitChan:       make(chan struct{}),
	}
	result.start(concurrency)
	return result
}

func (w *Worker) start(concurrency int) {
	for i := 0; i < concurrency; i++ {
		w.wg.Run(w.task)
	}
}

func (w *Worker) task() {
	for {
		select {
		case job := <-w.jobChan:
			func() {
				defer func() {
					if r := recover(); r != nil {
						statslogutil.StatsLogger().Error(
							"Recovered from panic in auto analyze job",
							zap.Stringer("job", job),
							zap.Any("panic", r),
						)
					}
				}()

				statslogutil.StatsLogger().Info(
					"Auto analyze triggered",
					zap.Stringer("job", job),
				)
				err := job.Analyze(
					w.statsHandle,
					w.sysProcTracker,
				)
				if err != nil {
					statslogutil.StatsLogger().Error(
						"Execute auto analyze job failed",
						zap.Stringer("job", job),
						zap.Error(err),
					)
				}
				statslogutil.StatsLogger().Info(
					"Auto analyze finished",
					zap.Stringer("job", job),
				)
				jobs.Delete(job.ID())
				w.jobCnt.Add(1)
			}()
		case <-w.exitChan:
			return
		}
	}
}

func (w *Worker) close() {
	close(w.exitChan)
	w.wg.Wait()
}

func (w *Worker) CompletedJobsCnt() int64 {
	return w.jobCnt.Load()
}
