// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "time"

// sliceSeams times the seams of a Slot's reads, on the Slot's goroutine: a
// read's tail, from its last series reaching the Slot to the read ending,
// and the change into the next read, from that end to the next read's first
// series reaching the Slot. Between the reads of a Slot read in slices sits
// the time a sliced Slot spends besides reading and evaluating: the tail is
// its answer's end - the footer checked, the read finished - and the change
// is the slice completing and the next one asked, or waited for when it was
// read ahead. Summed, and the longest of each (SlotBudgetUsage).
type sliceSeams struct {
	// first and last are when the read in hand's first and last series
	// reached the Slot, zero before its first; ended is when the last read
	// kept ended, zero before one.
	first, last, ended time.Time
	tail, tailMax      time.Duration
	change, changeMax  time.Duration
	// handoff and handoffMax time each read's end from the read before's,
	// series or none (015 O1): change and tail leave a read of no series
	// out, and a Query Group whose slices answer nothing has no seam to
	// read without these.
	handoff, handoffMax time.Duration
}

// fold adds other's seams to these: their sums, and the longest of each.
func (seams *sliceSeams) fold(other sliceSeams) {
	seams.tail, seams.tailMax = seams.tail+other.tail, max(seams.tailMax, other.tailMax)
	seams.change, seams.changeMax = seams.change+other.change, max(seams.changeMax, other.changeMax)
	seams.handoff, seams.handoffMax = seams.handoff+other.handoff, max(seams.handoffMax, other.handoffMax)
}

// seriesCame notes a series of the read in hand reaching the Slot at at.
func (seams *sliceSeams) seriesCame(at time.Time) {
	if seams.first.IsZero() {
		seams.first = at
	}
	seams.last = at
}

// readEnded notes the read in hand ending at at, kept: its tail, after its
// last series, and the change into it, from the read before's end to its
// first series. A read of no series has neither, and the next change runs
// from its end.
func (seams *sliceSeams) readEnded(at time.Time) {
	if !seams.last.IsZero() {
		tail := max(at.Sub(seams.last), 0)
		seams.tail, seams.tailMax = seams.tail+tail, max(seams.tailMax, tail)
	}
	if !seams.first.IsZero() && !seams.ended.IsZero() {
		change := max(seams.first.Sub(seams.ended), 0)
		seams.change, seams.changeMax = seams.change+change, max(seams.changeMax, change)
	}
	if !seams.ended.IsZero() {
		handoff := max(at.Sub(seams.ended), 0)
		seams.handoff, seams.handoffMax = seams.handoff+handoff, max(seams.handoffMax, handoff)
	}
	seams.first, seams.last, seams.ended = time.Time{}, time.Time{}, at
}

// readLetGo notes the read in hand let go, to be read again in its
// children: it did not end, and its series no longer count - the change
// runs on to the first series of the read that comes back.
func (seams *sliceSeams) readLetGo() {
	seams.first, seams.last = time.Time{}, time.Time{}
}
