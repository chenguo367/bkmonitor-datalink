// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A read of the open Segments hands each to its caller as its timeline is
// decoded, and keeps none itself: when the first Segment is visited, one
// batch of timelines has been read, however many Query Groups the read is
// for. Doubling the Query Groups doubles the batches read, not what is held
// at once; and each Segment visited is its timeline's open one.
func TestOpenSegmentsAreVisitedBatchByBatch(t *testing.T) {
	for _, count := range []int{openSegmentReadBatch * 2, openSegmentReadBatch * 4} {
		groups := make([]execution.QueryGroupIdentity, count)
		for index := range groups {
			groups[index] = execution.QueryGroupIdentity(fmt.Sprintf("qg-visit-%04d", index))
		}
		repository, client := timelineCacheFixture(t, groups, 2)
		client.gets = 0
		visited, firstAt := 0, -1
		err := repository.readOpenSegments(context.Background(), groups, controlVersion{},
			func(identity execution.QueryGroupIdentity, segment persistedScheduleSegment) error {
				if visited == 0 {
					firstAt = client.gets
				}
				visited++
				timeline, err := decodeScheduleTimeline(identity, []byte(client.values[repository.scheduleTimelineKey(identity)]))
				if err != nil {
					return err
				}
				if open := timeline.Segments[len(timeline.Segments)-1]; segment.Schedule.Segment.ObjectDigest != open.Schedule.Segment.ObjectDigest ||
					len(segment.Plans) != len(open.Plans) {
					t.Fatalf("%s visited with %+v, want its open Segment", identity, segment.Schedule.Segment)
				}
				return nil
			})
		if err != nil || visited != count {
			t.Fatalf("%d groups: visited %d, %v", count, visited, err)
		}
		if firstAt != openSegmentReadBatch {
			t.Fatalf("%d groups: the first Segment was visited after %d reads, want one batch of %d", count, firstAt, openSegmentReadBatch)
		}
	}
}

// A visit that fails stops the read with its error: the caller's refusal -
// a Plan open in two Query Groups - is not read past.
func TestAFailedVisitStopsTheRead(t *testing.T) {
	groups := []execution.QueryGroupIdentity{"qg-stop-a", "qg-stop-b", "qg-stop-c"}
	repository, _ := timelineCacheFixture(t, groups, 2)
	stop := fmt.Errorf("refused")
	visited := 0
	err := repository.readOpenSegments(context.Background(), groups, controlVersion{},
		func(execution.QueryGroupIdentity, persistedScheduleSegment) error {
			visited++
			return stop
		})
	if err != stop || visited != 1 {
		t.Fatalf("read = %v after %d visits, want the visit's error after the first", err, visited)
	}
}
