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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// readSliceOf is a slice read whole whose series came from the physical
// queries given, each delivering its count of series, through the stream's
// own count (noteSliceInput), and its sample as the PRIMARY gave it: the
// state noteLeafRead and landSliceRead see once the slice has landed.
func readSliceOf(t *testing.T, sample inputslice.Sample, perQuery map[execution.PhysicalQueryDigest]uint64) (*sliceReading, *sliceRead, inputslice.Leaf) {
	t.Helper()
	tree, err := inputslice.New([]string{"host"}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	reading := &sliceReading{tree: tree, dimensions: []string{"host"}}
	stream := &streamedExecution{slotStream: &slotStream{slices: reading}}
	for query, series := range perQuery {
		for range series {
			stream.noteSliceInput(execution.SeriesExecutionBatch{PhysicalQuery: query,
				Delivery: execution.SeriesDelivery{PhysicalQuery: query, Series: 1}}, 0)
		}
	}
	stream.slice.sample = sample
	return reading, &stream.slice, tree.Pending()[0]
}

// A slice of a Plan whose two physical queries each read the same 80,000
// series - a ring ratio's PRIMARY and its "previous" dependency, sent side by
// side in one slice - holds 80,000 series a request, within the 87,000 a
// slice is cut to: it is not split for the next Slot, and the Slot remembers
// 80,000 series read, not their sum. The provider answers each request on its
// own and at once (access.Source runs a slice's queries side by side), so the
// sum is no request's size, and sizing by it cuts such a Query Group into
// twice the slices its requests need.
//
// The same two queries at 100,000 series each are past a slice's series, and
// are split: the size a request answers is what is read, not the first query
// alone.
func TestTheSeriesOfASliceAreTheMostOneOfItsQueriesRead(t *testing.T) {
	ringRatio := map[execution.PhysicalQueryDigest]uint64{"primary": 80_000, "previous": 80_000}
	reading, slice, leaf := readSliceOf(t, hostSample(80, 1000), ringRatio)
	reading.noteLeafRead(slice, leaf, 10_000, false)
	if !reading.tree.Shape().IsZero() {
		t.Fatal("two queries of 80,000 series each, within a slice's 87,000 a request, were split for the next Slot")
	}
	if got := reading.leafSeries[leaf.Slice.Digest()]; got != 80_000 {
		t.Fatalf("the slice is planned at %d series, want the 80,000 one request read", got)
	}
	reading.landSliceRead(slice, leaf)
	if reading.seriesRead != 80_000 {
		t.Fatalf("the Slot remembers %d series read, want the 80,000 one request read", reading.seriesRead)
	}

	past := map[execution.PhysicalQueryDigest]uint64{"primary": 100_000, "previous": 100_000}
	reading, slice, leaf = readSliceOf(t, hostSample(100, 1000), past)
	reading.noteLeafRead(slice, leaf, 20_000, false)
	if reading.tree.Shape().IsZero() {
		t.Fatal("two queries of 100,000 series each, past a slice's 87,000 a request, were not split for the next Slot")
	}
}

// A slice abandoned for its children gives back every series it reserved of
// the pool - the series of all its queries, 160,000 for two queries of 80,000
// each - and leaves the stream's series where they were before it began: the
// size it is judged by is one request's, what it holds of the pool is all
// of its requests'.
func TestAnAbandonedSliceGivesBackTheSeriesOfAllItsQueries(t *testing.T) {
	coordinator := &SlotExecutionCoordinator{}
	tree, err := inputslice.New([]string{"host"}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	reading := &sliceReading{tree: tree, dimensions: []string{"host"}}
	stream := &streamedExecution{slotStream: &slotStream{coordinator: coordinator, slices: reading, series: 5}}
	coordinator.reservations.series = 5
	for _, query := range []execution.PhysicalQueryDigest{"primary", "previous"} {
		for range 80_000 {
			coordinator.reservations.series++
			stream.series++
			stream.noteSliceInput(execution.SeriesExecutionBatch{PhysicalQuery: query,
				Delivery: execution.SeriesDelivery{PhysicalQuery: query, Series: 1}}, 0)
		}
	}
	if stream.slice.series != 80_000 {
		t.Fatalf("the slice is judged at %d series, want one request's 80,000", stream.slice.series)
	}
	stream.abandonSlice()
	if coordinator.reservations.series != 5 || stream.series != 5 {
		t.Fatalf("after the slice was abandoned the pool holds %d series and the stream %d, want the 5 held before it",
			coordinator.reservations.series, stream.series)
	}
}
