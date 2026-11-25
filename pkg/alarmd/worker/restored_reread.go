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
	"context"
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A Slot its previous owner handed over is read again by the lookback here
// (014 B4 H1) as the previous owner would have read it: in the slices this
// process reads the Query Group in now, and keeping only the series its
// supplement would not tell decided by their stable segments.

// RestoredSlices is the slices a Slot of the Query Group read now would
// start in, as its last read here left it (sliceReadingFor): the shape it
// was last read in, else its slices counted from its last read's size; one
// slice, the zero one, for a Query Group read whole. False while no Slot of
// the Query Group has completed here (or its last found its target missing,
// which leaves none): the reread waits for one. Reads the memory and changes
// none of it - a Slot's own start does. A function of the coordinator's, not
// a method: it executes nothing, and Execute stays its one way in.
func RestoredSlices(coordinator *SlotExecutionCoordinator, request execution.SlotExecutionRequest) ([]execution.InputSlice, bool) {
	if coordinator == nil {
		return nil, false
	}
	last, held := coordinator.slicedInput.heldInput(request.Contract.Slot.QueryGroup)
	if !held {
		return nil, false
	}
	whole := []execution.InputSlice{{}}
	dimensions := last.dimensions
	if len(dimensions) == 0 {
		return whole, true
	}
	var tree *inputslice.Tree
	if !last.shape.IsZero() && last.revision == request.Contract.QueryRevision && slices.Equal(last.shape.Dimensions(), dimensions) &&
		last.shape.Leaves() <= inputslice.MaxLeaves-3 {
		if shaped, err := inputslice.FromShape(dimensions, last.shape); err == nil {
			tree = shaped
		}
	}
	if tree == nil {
		leaves := max(slicesBySeries(last.series), slicesByTime(last.timedOutMillis), last.slices)
		if limit := coordinator.wholeSliceLimit(); limit < ^uint64(0) && last.bytes > 0 {
			leaves = max(leaves, int((last.bytes+limit-1)/limit))
		}
		on := last.cut
		if !slices.Contains(dimensions, on.Dimension) {
			on = inputslice.Cut{}
			if last.series == 0 && last.bytes == 0 {
				on = inputslice.FixedCut(dimensions[0])
			}
		}
		if on.IsZero() || leaves <= 1 {
			return whole, true
		}
		built, err := inputslice.NewCut(dimensions, min(leaves, inputslice.MaxLeaves-3), on)
		if err != nil {
			return whole, true
		}
		tree = built
	}
	pending := tree.Pending()
	out := make([]execution.InputSlice, 0, len(pending))
	for _, leaf := range pending {
		out = append(out, leaf.Slice)
	}
	return out, true
}

// RestoredShapeKnown is whether a Slot of the Query Group has completed
// here, so RestoredSlices knows the slices it is read in: the memory alone,
// no store read, cheap enough to ask every Step a restored Slot waits.
func RestoredShapeKnown(coordinator *SlotExecutionCoordinator, queryGroup execution.QueryGroupIdentity) bool {
	if coordinator == nil {
		return false
	}
	_, held := coordinator.slicedInput.heldInput(queryGroup)
	return held
}

// RestoredCoverage is whether a supplement of the Slot request is frozen as
// would tell a series decided by its stable segments before reading its
// State (supplementCovered): every due Plan's book, as read now, covering
// its last point there. Nil when the store keeps no segments or they did
// not read: every series is kept. Like RestoredSlices, a function, not a
// method of the coordinator.
func RestoredCoverage(ctx context.Context, coordinator *SlotExecutionCoordinator, request execution.SlotExecutionRequest,
	header execution.InternalExecutionHeader) func(execution.SeriesIdentityDigest, int64) bool {
	if coordinator == nil || len(header.DuePlans) == 0 {
		return nil
	}
	store, ok := coordinator.ports.State.(segment.Store)
	if !ok {
		return nil
	}
	slot := loadSegmentSlot(ctx, store, request, coordinator.segmentDues(header))
	if slot == nil || len(slot.unread) > 0 {
		return nil
	}
	plans := append([]execution.DuePlan(nil), header.DuePlans...)
	return func(series execution.SeriesIdentityDigest, last int64) bool {
		for _, due := range plans {
			coverage, member := slot.Coverage(execution.StateKeyIdentity{Plan: due.Identity, StateGeneration: due.StateGeneration,
				SeriesIdentityDigest: series})
			if !member || !coveredAt(coverage, last) {
				return false
			}
		}
		return true
	}
}

// coveredAt is whether a member's segment covers a point at last: an open
// member's mark at or past it, a closed one's close.
func coveredAt(coverage segment.Coverage, last int64) bool {
	end := coverage.Position
	if !coverage.Entry.Open() {
		end = min(end, coverage.Entry.Closed)
	}
	return end >= last
}
