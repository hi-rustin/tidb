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

import "container/heap"

// A priority queue for TableAnalysisJobs.
type AnalysisPriorityQueue struct {
	inner *analysisQueueInner
}

// NewAnalysisPriorityQueue creates a new AnalysisPriorityQueue.
func NewAnalysisPriorityQueue() *AnalysisPriorityQueue {
	return &AnalysisPriorityQueue{
		inner: &analysisQueueInner{},
	}
}

// Push adds a job to the priority queue with the given weight.
func (apq *AnalysisPriorityQueue) Push(job *TableAnalysisJob) {
	heap.Push(apq.inner, job)
}

// Pop removes the highest priority job from the queue.
func (apq *AnalysisPriorityQueue) Pop() *TableAnalysisJob {
	return heap.Pop(apq.inner).(*TableAnalysisJob)
}

// Len returns the number of jobs in the queue.
func (apq *AnalysisPriorityQueue) Len() int {
	return apq.inner.Len()
}

// An analysisQueueInner implements heap.Interface and holds TableAnalysisJobs.
type analysisQueueInner []*TableAnalysisJob

// Implement the sort.Interface methods for the priority queue.

func (aq analysisQueueInner) Len() int { return len(aq) }
func (aq analysisQueueInner) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority, so we use greater than here.
	return aq[i].Weight > aq[j].Weight
}
func (aq analysisQueueInner) Swap(i, j int) {
	aq[i], aq[j] = aq[j], aq[i]
}

// Push adds an item to the priority queue.
func (aq *analysisQueueInner) Push(x any) {
	item := x.(*TableAnalysisJob)
	*aq = append(*aq, item)
}

// Pop removes the highest priority item from the queue.
func (aq *analysisQueueInner) Pop() any {
	old := *aq
	n := len(old)
	item := old[n-1]
	*aq = old[0 : n-1]
	return item
}
