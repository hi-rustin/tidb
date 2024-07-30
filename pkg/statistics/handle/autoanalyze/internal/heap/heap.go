// Copyright 2017 The Kubernetes Authors.
// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Modifications:
// 1. Use the `errors` package from PingCAP.
// 2. Use generics to define the `heapData` struct.

package heap

import (
	"container/heap"
	"sync"

	"github.com/pingcap/errors"
)

const (
	closedMsg = "heap is closed"
)

// LessFunc is used to compare two objects in the heap.
type LessFunc[T any] func(T, T) bool

// KeyFunc is used to generate a key for an object.
type KeyFunc[T any] func(T) (string, error)

type heapItem[T any] struct {
	obj   T   // The object which is stored in the heap.
	index int // The index of the object's key in the Heap.queue.
}

type itemKeyValue[T any] struct {
	key string
	obj T
}

// heapData is an internal struct that implements the standard heap interface
// and keeps the data stored in the heap.
type heapData[T any] struct {
	items    map[string]*heapItem[T]
	queue    []string
	keyFunc  KeyFunc[T]
	lessFunc LessFunc[T]
}

var (
	_ = heap.Interface(&heapData[any]{}) // heapData is a standard heap
)

func (h *heapData[T]) Less(i, j int) bool {
	if i >= len(h.queue) || j >= len(h.queue) {
		return false
	}
	itemi, ok := h.items[h.queue[i]]
	if !ok {
		return false
	}
	itemj, ok := h.items[h.queue[j]]
	if !ok {
		return false
	}
	return h.lessFunc(itemi.obj, itemj.obj)
}

func (h *heapData[T]) Len() int { return len(h.queue) }

func (h *heapData[T]) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

func (h *heapData[T]) Push(kv interface{}) {
	keyValue := kv.(*itemKeyValue[T])
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem[T]{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

func (h *heapData[T]) Pop() interface{} {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[:len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		return nil
	}
	delete(h.items, key)
	return item.obj
}

// Heap is a thread-safe producer/consumer queue that implements a heap data structure.
type Heap[T any] struct {
	lock   sync.RWMutex
	cond   sync.Cond
	data   *heapData[T]
	closed bool
}

func (h *Heap[T]) Close() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.closed = true
	h.cond.Broadcast()
}

func (h *Heap[T]) Add(obj T) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.Errorf(closedMsg)
	}
	if _, exists := h.data.items[key]; exists {
		h.data.items[key].obj = obj
		heap.Fix(h.data, h.data.items[key].index)
	} else {
		h.addIfNotPresentLocked(key, obj)
	}
	h.cond.Broadcast()
	return nil
}

func (h *Heap[T]) BulkAdd(list []T) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.Errorf(closedMsg)
	}
	for _, obj := range list {
		key, err := h.data.keyFunc(obj)
		if err != nil {
			return errors.Errorf("key error: %v", err)
		}
		if _, exists := h.data.items[key]; exists {
			h.data.items[key].obj = obj
			heap.Fix(h.data, h.data.items[key].index)
		} else {
			h.addIfNotPresentLocked(key, obj)
		}
	}
	h.cond.Broadcast()
	return nil
}

func (h *Heap[T]) AddIfNotPresent(obj T) error {
	id, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.Errorf(closedMsg)
	}
	h.addIfNotPresentLocked(id, obj)
	h.cond.Broadcast()
	return nil
}

func (h *Heap[T]) addIfNotPresentLocked(key string, obj T) {
	if _, exists := h.data.items[key]; exists {
		return
	}
	heap.Push(h.data, &itemKeyValue[T]{key, obj})
}

func (h *Heap[T]) Update(obj T) error {
	return h.Add(obj)
}

func (h *Heap[T]) Delete(obj T) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if item, ok := h.data.items[key]; ok {
		heap.Remove(h.data, item.index)
		return nil
	}
	return errors.Errorf("object not found")
}

func (h *Heap[T]) Pop() (T, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for len(h.data.queue) == 0 {
		if h.closed {
			var zero T
			return zero, errors.Errorf("heap is closed")
		}
		h.cond.Wait()
	}
	obj := heap.Pop(h.data)
	if obj == nil {
		var zero T
		return zero, errors.Errorf("object was removed from heap data")
	}
	return obj.(T), nil
}

func (h *Heap[T]) List() []T {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]T, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

func (h *Heap[T]) ListKeys() []string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]string, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}
	return list
}

func (h *Heap[T]) Get(obj T) (T, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		var zero T
		return zero, false, errors.Errorf("key error: %v", err)
	}
	return h.GetByKey(key)
}

func (h *Heap[T]) GetByKey(key string) (T, bool, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	item, exists := h.data.items[key]
	if !exists {
		var zero T
		return zero, false, nil
	}
	return item.obj, true, nil
}

func (h *Heap[T]) IsClosed() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.closed
}

// NewHeap returns a Heap which can be used to queue up items to process.
func NewHeap[T any](keyFn KeyFunc[T], lessFn LessFunc[T]) *Heap[T] {
	h := &Heap[T]{
		data: &heapData[T]{
			items:    map[string]*heapItem[T]{},
			queue:    []string{},
			keyFunc:  keyFn,
			lessFunc: lessFn,
		},
	}
	h.cond.L = &h.lock
	return h
}
