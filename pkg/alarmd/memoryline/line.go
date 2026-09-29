// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package memoryline is the one line the process's observation memory grows
// under. Detection has its memory budgets - the Slots' retained bytes, the
// control caches - and the Go runtime its soft limit. Observation - the cost
// summary, the cost projection, the criterion samples, the late-data
// lookback - takes no share of its own: it may grow while the live heap after
// the last collection, plus what the detection budgets may still take, plus
// what observation was granted since that collection, stays within the soft
// limit. At the line every consumer stops taking more and keeps what it has,
// and each refusal is counted under the consumer it refused.
//
// There is no ratio, no order among the consumers and no count they are held
// to: a deployment far from its limit never refuses, and one near it refuses
// whichever consumer asks next.
package memoryline

import (
	"math"
	runtimemetrics "runtime/metrics"
	"sync"
	"sync/atomic"
)

// Consumer is one observation reader of memory. Closed.
type Consumer string

const (
	// ConsumerCostSummary is the per-group cost summary, sized by the Query
	// Groups this replica owns.
	ConsumerCostSummary Consumer = "cost_summary"
	// ConsumerCostProjection is the cross-replica cost projection: what one
	// refresh reads of the other replicas' published rankings.
	ConsumerCostProjection Consumer = "cost_projection"
	// ConsumerSeriesSampler is the criterion samples' encode buffers, one per
	// open sample window.
	ConsumerSeriesSampler Consumer = "series_sampler"
	// ConsumerLookback is the late-data lookback's per-series tables.
	ConsumerLookback Consumer = "lookback"
)

// Consumers is every consumer, in the order they are reported.
var Consumers = []Consumer{ConsumerCostSummary, ConsumerCostProjection, ConsumerSeriesSampler, ConsumerLookback}

// Reserve is what one detection budget may still take: its size less what
// it holds now. The line leaves that room to detection.
type Reserve func() uint64

// heap is the runtime's reading the line is drawn from.
type heap struct {
	limit, live, cycles uint64
}

// Line is the process's observation memory line. The zero of a nil *Line
// admits everything.
type Line struct {
	read func() heap

	mu       sync.Mutex
	reserves []Reserve
	// cycle is the collection the grants below were made after: from the
	// next one on, what they took is in the live heap. reserved is what the
	// detection budgets could still take when the line first read after
	// that collection: detection that grows before the next one takes room
	// the live heap does not show yet, so the room stays detection's until
	// then.
	cycle    uint64
	granted  uint64
	reserved uint64

	// refused and admitted are by consumer, in the order of Consumers.
	refused  []atomic.Uint64
	admitted []atomic.Uint64
}

// New is the line over the running process's heap.
func New() *Line {
	samples := []runtimemetrics.Sample{{Name: "/gc/gomemlimit:bytes"}, {Name: "/gc/heap/live:bytes"}, {Name: "/gc/cycles/total:gc-cycles"}}
	var mu sync.Mutex
	return newLine(func() heap {
		mu.Lock()
		defer mu.Unlock()
		runtimemetrics.Read(samples)
		return heap{limit: sampleValue(samples[0]), live: sampleValue(samples[1]), cycles: sampleValue(samples[2])}
	})
}

func newLine(read func() heap) *Line {
	return &Line{read: read, refused: make([]atomic.Uint64, len(Consumers)), admitted: make([]atomic.Uint64, len(Consumers))}
}

func sampleValue(sample runtimemetrics.Sample) uint64 {
	if sample.Value.Kind() != runtimemetrics.KindUint64 {
		return 0
	}
	return sample.Value.Uint64()
}

// Reserve leaves room for one more detection budget.
func (line *Line) Reserve(reserve Reserve) {
	if line == nil || reserve == nil {
		return
	}
	line.mu.Lock()
	line.reserves = append(line.reserves, reserve)
	line.mu.Unlock()
}

// Admit answers whether consumer may take bytes more. Admitted, the bytes
// count against the line until the next collection, whose live heap holds
// them from then on; nothing is given back. Refused, the consumer keeps
// what it has and takes no more, and the refusal is counted.
func (line *Line) Admit(consumer Consumer, bytes uint64) bool {
	if line == nil {
		return true
	}
	index := consumerIndex(consumer)
	reading := line.read()
	line.mu.Lock()
	reserved := line.reservedLocked(reading.cycles)
	taken := saturatingAdd(saturatingAdd(reading.live, reserved), line.granted)
	admitted := reading.limit > 0 && taken <= reading.limit && bytes <= reading.limit-taken
	if admitted {
		line.granted = saturatingAdd(line.granted, bytes)
	}
	line.mu.Unlock()
	if index >= 0 {
		if admitted {
			line.admitted[index].Add(bytes)
		} else {
			line.refused[index].Add(1)
		}
	}
	return admitted
}

// reservedLocked is the room left to detection at collection cycle: what
// its budgets could take at the first reading after the collection, or
// more if they could take more now. A new collection forgets the grants,
// whose bytes its live heap holds.
func (line *Line) reservedLocked(cycle uint64) uint64 {
	unused := line.unusedLocked()
	if cycle != line.cycle {
		line.cycle, line.granted, line.reserved = cycle, 0, unused
	}
	return max(line.reserved, unused)
}

// unusedLocked is what the detection budgets may still take.
func (line *Line) unusedLocked() uint64 {
	var unused uint64
	for _, reserve := range line.reserves {
		unused = saturatingAdd(unused, reserve())
	}
	return unused
}

// Reading is the line as a reader sees it: the soft limit, the live heap
// after the last collection, what the detection budgets may still take,
// Headroom - the limit less the other two, negative past the line - and what
// observation was granted since that collection, which admission also takes
// out of the headroom.
type Reading struct {
	LimitBytes     uint64
	LiveBytes      uint64
	ReservedBytes  uint64
	HeadroomBytes  int64
	GrantedBytes   uint64
	RefusedTotal   map[Consumer]uint64
	AdmittedBytes  map[Consumer]uint64
	LimitUnlimited bool
}

// Read is the line now.
func (line *Line) Read() Reading {
	reading := Reading{RefusedTotal: map[Consumer]uint64{}, AdmittedBytes: map[Consumer]uint64{}}
	for _, consumer := range Consumers {
		reading.RefusedTotal[consumer], reading.AdmittedBytes[consumer] = 0, 0
	}
	if line == nil {
		return reading
	}
	heap := line.read()
	line.mu.Lock()
	reserved := line.reservedLocked(heap.cycles)
	granted := line.granted
	line.mu.Unlock()
	reading.LimitBytes, reading.LiveBytes, reading.ReservedBytes, reading.GrantedBytes = heap.limit, heap.live, reserved, granted
	reading.LimitUnlimited = heap.limit == math.MaxInt64
	reading.HeadroomBytes = signedDifference(heap.limit, saturatingAdd(heap.live, reserved))
	for index, consumer := range Consumers {
		reading.RefusedTotal[consumer] = line.refused[index].Load()
		reading.AdmittedBytes[consumer] = line.admitted[index].Load()
	}
	return reading
}

func consumerIndex(consumer Consumer) int {
	for index, known := range Consumers {
		if known == consumer {
			return index
		}
	}
	return -1
}

func saturatingAdd(left, right uint64) uint64 {
	if left > math.MaxUint64-right {
		return math.MaxUint64
	}
	return left + right
}

func signedDifference(left, right uint64) int64 {
	if left >= right {
		return int64(min(left-right, math.MaxInt64))
	}
	return -int64(min(right-left, math.MaxInt64))
}
