// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package readhold

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A group's record keeps how long its query takes, as its owner measured it,
// for the process that takes the group over: a new owner has no reading of
// its own until a round of the group has run there, and a round it judges by
// a fallback that is shorter than the query runs past its deadline instead of
// going to replay. The reading is the owner's whole first read - from the
// request sent to the last series handed on - rounded up to the second.
//
// It is written only into a record that is kept for the hold anyway: a group
// whose record is missing has never been measured, and a record for the
// reading alone would tell its successor more than that the group holds
// nothing. A group none of whose first reads completed in time has no
// reading either, and its successor judges it by the fallback, as before.

// Writes of the reading, closed: changed is a new reading, refreshed the
// same one rewritten so it does not go stale (QueryDurationStaleAfter) while
// it still holds.
const (
	QueryDurationWriteChanged   = "changed"
	QueryDurationWriteRefreshed = "refreshed"
)

// QueryDurationWriteReasons is every write of the reading.
var QueryDurationWriteReasons = []string{QueryDurationWriteChanged, QueryDurationWriteRefreshed}

const (
	// queryDurationWriteEvery is the least time between two writes of a
	// group's reading: a reading that moves a second every round is written
	// once a minute.
	queryDurationWriteEvery = time.Minute
	// QueryDurationStaleAfter is how old a reading is no longer one: a query
	// changes over a day as its data does, and the hold is renewed as often.
	QueryDurationStaleAfter = RenewInterval
	// queryDurationRefreshAfter is how old a reading that still holds is
	// rewritten, so a successor never finds it stale while the owner keeps
	// measuring it: twice a day at most.
	queryDurationRefreshAfter = QueryDurationStaleAfter / 2
)

// queryDurationBucket is a measured duration rounded up to the second, in
// milliseconds: a reading moves the record only when it moves a second.
func queryDurationBucket(measured time.Duration) int64 {
	return int64((measured + time.Second - 1) / time.Second * 1000)
}

// MeasureQueryDuration gives the controller the group's query duration as
// this process measured it, for the record the group's next Slot freezes to
// keep (noteQueryDurationLocked). It is given outside every lock: the
// measuring side calls into the controller under its own.
func (controller *Controller) MeasureQueryDuration(qg execution.QueryGroupIdentity, measured time.Duration) {
	if measured <= 0 {
		return
	}
	state := controller.group(qg)
	state.mu.Lock()
	state.measuredQueryDuration = measured
	state.mu.Unlock()
}

// noteQueryDurationLocked keeps the group's measured duration on next when it
// moved by a second and the last write is a minute old, or when the reading
// it keeps is half way to stale: the reason written, or "" for none. Called
// with the group's lock held.
func (controller *Controller) noteQueryDurationLocked(state *entry, next *Record) string {
	if state.measuredQueryDuration <= 0 {
		return ""
	}
	now := controller.options.Now().UnixMilli()
	bucket, age := queryDurationBucket(state.measuredQueryDuration), now-next.QueryDurationAtMillis
	reason := ""
	switch {
	case bucket != next.QueryDurationMillis && (next.QueryDurationAtMillis == 0 || age >= queryDurationWriteEvery.Milliseconds()):
		reason = QueryDurationWriteChanged
	case bucket == next.QueryDurationMillis && age >= queryDurationRefreshAfter.Milliseconds():
		reason = QueryDurationWriteRefreshed
	default:
		return ""
	}
	next.QueryDurationMillis, next.QueryDurationAtMillis = bucket, now
	return reason
}

// InheritedQueryDuration is how long the group's query took as the record
// says - its last owner's reading, or this process's once written - and
// false when the record keeps none, is not read, or keeps one older than
// QueryDurationStaleAfter.
func (controller *Controller) InheritedQueryDuration(qg execution.QueryGroupIdentity) (time.Duration, bool) {
	state := controller.group(qg)
	state.mu.Lock()
	defer state.mu.Unlock()
	record := state.record
	if !state.loaded || record.QueryDurationMillis <= 0 ||
		controller.options.Now().UnixMilli()-record.QueryDurationAtMillis >= QueryDurationStaleAfter.Milliseconds() {
		return 0, false
	}
	return time.Duration(record.QueryDurationMillis) * time.Millisecond, true
}
