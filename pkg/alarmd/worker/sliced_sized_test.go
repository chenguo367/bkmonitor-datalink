// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// The fleet of TestAFirstDimensionOfOneValueStartsInEvenSlices, read a third
// time. The second Slot started in the slices its root cut made, let the rest
// go for its size - a slice it started in - and read it in four. That split
// is the Query Group's next start: the third Slot starts in the eight slices
// the second landed and lets nothing go. Before, the third let the rest go
// again, and every Slot after it, one more request a round for a slice whose
// split no round kept.
func TestASliceLetGoForItsSizeIsStartedSplitTheRoundAfter(t *testing.T) {
	fixture := largeSlicedFixture(t)
	for index := range fixture.source.series {
		fixture.source.series[index].cloud = "0"
	}
	fixture.source.facts = cloudHostFacts()
	request := windowedRequest()
	for round := 1; round <= 2; round++ {
		if result, err := fixture.coordinator.Execute(context.Background(), request); err != nil || !result.Completed {
			t.Fatalf("round %d: result=%+v err=%v", round, result, err)
		}
	}
	first := len(fixture.source.calls)
	result, err := fixture.coordinator.Execute(context.Background(), request)
	if err != nil || !result.Completed {
		t.Fatalf("third round: result=%+v err=%v, want it completed", result, err)
	}
	if result.Usage.InputSlicesDiscarded != 0 || result.Usage.InputSliceStartDiscarded != 0 {
		t.Fatalf("third round usage %+v, want no slice let go: it starts in the split the round before kept", result.Usage)
	}
	landed := map[string]bool{}
	for _, call := range fixture.source.calls[first:] {
		if call.completed {
			landed[call.slice.Digest()] = true
		}
	}
	if len(landed) != 8 {
		t.Fatalf("third round landed %d slices, want the eight the second landed", len(landed))
	}
}

// A slice the Slot started in, let go for its size, is cut by the sample of
// the part of it read before it went - a coarse cut - and a part of it that
// still holds past a slice's series is kept split again once read whole
// (KeepSizedSplit): the two converge within two rounds of the first cut,
// and the rounds after let nothing go, split nothing and land the same
// slices, each within a slice's series. A slice's series are four here, as
// a Slot of 87,000, read whole, is past the fixture's budget; the 24 hosts
// of 45 KiB of TestTheSlicesAQueryGroupIsReadInDoNotGrowRoundByRound. The
// bound is lowered for the splits kept for series only: A5's cut is made for
// the bytes of a slice past its limit, and slicesBySeries still counts by
// sliceSeries.
func TestACoarseCutForSizeIsRefinedForSeriesWithinTwoRounds(t *testing.T) {
	const series = 4
	worker.SetSizedSplitSeriesForTest(t, series)
	fixture := newSlicedFixture(t, slicedHosts(24, 45<<10), sliceableFacts(), 200)
	fixture.source.queryMillis = 10_000
	request := windowedRequest()
	landed := func(round int) []string {
		t.Helper()
		before := len(fixture.source.calls)
		result, err := fixture.coordinator.Execute(context.Background(), request)
		if err != nil || !result.Completed {
			t.Fatalf("round %d: result=%+v err=%v, want it completed", round, result, err)
		}
		if round > 2 && result.Usage.InputSlicesDiscarded != 0 {
			t.Fatalf("round %d let %d slices go, want none after the first cut's round", round, result.Usage.InputSlicesDiscarded)
		}
		var slices []string
		for _, call := range fixture.source.calls[before:] {
			if !call.completed {
				continue
			}
			slices = append(slices, call.slice.Digest())
			if round > 2 && len(call.delivered) > series {
				t.Fatalf("round %d slice %s held %d series, want within a slice's %d", round, call.slice.Digest(), len(call.delivered), series)
			}
		}
		sort.Strings(slices)
		t.Logf("round %d landed %d slices", round, len(slices))
		return slices
	}
	for round := 1; round <= 2; round++ {
		landed(round)
	}
	settled := landed(3)
	for round := 4; round <= 7; round++ {
		if got := landed(round); !reflect.DeepEqual(got, settled) {
			t.Fatalf("round %d landed %d slices, round 3 %d: want the cut settled from round 3", round, len(got), len(settled))
		}
	}
}
