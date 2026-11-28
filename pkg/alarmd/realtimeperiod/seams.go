// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package realtimeperiod is a realtime Query Group's periods (016 section
// 3): the buckets its points are filed in as they are read, the watermark a
// period closes by, and the source its Slot reads a closed period's records
// from. This file holds the seams other parts are written against - the
// point stream files points here (Buckets), and this reads the reader's
// partitions (TopicProgress) and the channel records (ChannelStates) - so
// each side can be built and tested apart.
package realtimeperiod

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeinput"
)

// Buckets is where the point stream files the points of the realtime Query
// Groups this worker holds.
type Buckets interface {
	// Add files point, a point the stream read for queryGroup that its
	// owner holds, whose metric the group references and whose series
	// passed the conditions of each of plans (none: not called), in its
	// series' bucket of the period its event time falls in. The goroutine
	// of every partition the stream reads calls it at once: it is safe for
	// that, and it does no blocking I/O - a slow write in it would hold that
	// partition's reads and with them every period's watermark. A point that
	// is the first of its series in its period to cross a Level (016 section
	// 3.4) is handed to a bounded queue whose writer judges it and writes its
	// event outside Add; a point that finds the queue full is still filed,
	// and Add still says accepted - its period's close judges it and writes
	// its event as it would have - and the early event not made is counted
	// by the buckets by name.
	Add(queryGroup execution.QueryGroupIdentity, plans []execution.PlanIdentity, point realtimeinput.Point) AddResult
}

// AddResult is what Add did with a point.
type AddResult string

// AddResult's values, closed.
const (
	// AddAccepted: filed in its series' bucket of an open period.
	AddAccepted AddResult = "accepted"
	// AddLateAfterClose: its period was closed and committed; dropped, the
	// committed period left as it was (016 section 3.5).
	AddLateAfterClose AddResult = "late_after_close"
	// AddReplaySkipped: of a period committed before the group was taken
	// over, read again by the replay that placed the reader; dropped (016
	// section 4).
	AddReplaySkipped AddResult = "replay_skipped"
	// AddOverQuota: a series with no bucket yet in a group whose share the
	// buckets have taken; dropped, and the group's period read PARTIAL (016
	// section 3.2).
	AddOverQuota AddResult = "over_quota"
)

// AddResults is the closed set of AddResult.
func AddResults() []AddResult {
	return []AddResult{AddAccepted, AddLateAfterClose, AddReplaySkipped, AddOverQuota}
}

// TopicProgress is how far the reader of a topic has read each of its
// partitions: what a period's watermark is taken over (016 section 3.3).
type TopicProgress interface {
	// Progress is every partition of cluster's topic as Kafka's metadata
	// last listed it, by partition, as read now: one listed and not yet
	// placed, or placed and failed or closed, is there and not Ready. known
	// is false while the metadata has not been read: no partition, and no
	// watermark.
	Progress(cluster, topic string) (partitions map[int32]realtimeinput.PartitionProgress, known bool)
}

// ChannelStates is what the channel records say of a realtime item as a
// period closes: whether its channel has taken effect, so its absence is
// judged (016 section 5.4, REALTIME_CHANNEL_PENDING).
type ChannelStates interface {
	// NoDataFrom is when, in Unix milliseconds, item of the tenant's result
	// table is judged for no data from: its channel's applied_at and 90 s.
	// known is false while it has not taken effect - no record for the
	// table, the item not among its applied items, its source not resolved
	// - and err is not nil when the record could not be read; either way no
	// data is judged, and the two are counted apart. It reads the channel
	// record from the store: ctx is the closing period's deadline, and the
	// read is the control plane's (redisfailure.CallerControlPlane). The
	// item is the Plan's own (contract.RealtimeItemV2.Item).
	NoDataFrom(ctx context.Context, tenant, resultTableID string, item contract.RealtimeItemKey) (fromMillis int64, known bool, err error)
}
