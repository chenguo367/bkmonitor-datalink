// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"testing"
	"time"
)

// The memory's reading counts what start's sweep can reach apart from what it
// cannot: a Query Group remembered without a Slot started here, and one whose
// period is not known, are both kept by every sweep. The entries are summed
// over every memory, reached or not.
func TestTheSliceMemoryReadingCountsWhatTheSweepCannotReach(t *testing.T) {
	now := time.Unix(1_788_000_000, 0)
	var trees sliceTrees
	trees.start("walked", now, time.Minute)
	trees.start("period unknown", now, 0)
	trees.remember("walked", slicedReading{leafSeries: map[string]uint64{"a": 1, "b": 2}, deferred: map[string]bool{"a": true}})
	trees.remember("period unknown", slicedReading{leafSeries: map[string]uint64{"c": 3}})
	trees.remember("never started", slicedReading{leafSeries: map[string]uint64{"d": 4, "e": 5, "f": 6}})
	reading := trees.reading()
	// Three memories, each a shape of one leaf (none kept a split); two of
	// them out of the sweep's reach; six per-slice series entries; one
	// deferred slice; the largest shape, one leaf.
	want := SliceMemoryReading{Remembered: 3, Unswept: 2, Leaves: 3, LeafSeries: 6, Deferred: 1, LeavesMax: 1}
	if reading != want {
		t.Fatalf("reading = %+v, want %+v", reading, want)
	}
	// Idle past its bound, the walked one goes at the next sweep, the two the
	// sweep cannot reach stay: the reading follows.
	trees.start("another", now.Add(20*time.Minute), time.Minute)
	if got := trees.reading(); got.Remembered != 2 || got.Unswept != 2 || got.LeafSeries != 4 {
		t.Fatalf("after the sweep reading = %+v, want the two unswept memories and their four entries", got)
	}
}
