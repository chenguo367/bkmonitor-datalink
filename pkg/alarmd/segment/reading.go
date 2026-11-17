// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package segment

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Query Group's Plans' segments as a reader of the object sees them: each
// Plan's mark as Redis holds it - its round, position, page count and page
// renewals, never a member of a page - and what the process answering last
// did with the Plan's segments. Read on request only, never on a tick.

// ReadingRenewals bounds the page renewals a Plan's reading lists: the pages
// of a Query Group at its largest (150 000 series at PageFill, rounded up to
// a power of two, is 64). A mark naming more lists the first ReadingRenewals
// and their total.
const ReadingRenewals = 64

// How a Plan's mark read for a reader, closed: found; absent, Redis holds no
// mark for the Plan's generation (before its first round, or past its keys'
// life); unreadable, bytes that do not read as a mark.
const (
	MarkFound      = "found"
	MarkAbsent     = "absent"
	MarkUnreadable = "unreadable"
)

// PlansReading is a Query Group's Plans' segments: one PlanReading for each
// Plan of its open schedule Segment, in Plan order, at most the bound the
// Plans are read by; Total is the Plans of the Segment. LastRecorded is
// whether the answering process holds anything it did with the Query
// Group's segments: when false, the replica that runs the Query Group does,
// and every Plan's Last is empty here.
type PlansReading struct {
	QueryGroup   execution.QueryGroupIdentity `json:"query_group"`
	Plans        []PlanReading                `json:"plans"`
	Total        int                          `json:"total"`
	LastRecorded bool                         `json:"last_recorded"`
}

// PlanReading is one Plan's segments, Shard the piece of a split strategy
// it is (absent for a Plan not split). Mark says how its mark read; for a
// found mark, Round is the evaluation time of the round that wrote it,
// Position the source time its open segments are read up to, Pages its
// page count and Renewed each page's last renewal (at most ReadingRenewals,
// RenewedTotal of them in all), At and TTL in milliseconds. Unreadable says
// why a mark did not read. Last is what the answering process last did with
// the Plan's segments, nil when it did nothing with this generation's.
type PlanReading struct {
	Plan         execution.PlanIdentity    `json:"plan"`
	Shard        *execution.ShardRef       `json:"shard,omitempty"`
	Generation   execution.StateGeneration `json:"state_generation"`
	Mark         string                    `json:"mark"`
	Unreadable   string                    `json:"unreadable,omitempty"`
	Round        execution.EvaluationTime  `json:"round,omitempty"`
	Position     int64                     `json:"position,omitempty"`
	Pages        int                       `json:"pages,omitempty"`
	Renewed      []Renewal                 `json:"renewed,omitempty"`
	RenewedTotal int                       `json:"renewed_total,omitempty"`
	Last         *LastUse                  `json:"last,omitempty"`
}

// LastUse is what a process last did with a Plan's segments: the source and
// reason its last read came by (cache and hit, the cache answered; redis and
// why it did not) and when, and the result its last write came back with
// (WriteResults) and when. A part not done is empty.
type LastUse struct {
	ReadSource  string     `json:"read_source,omitempty"`
	ReadReason  string     `json:"read_reason,omitempty"`
	ReadAt      *time.Time `json:"read_at,omitempty"`
	WriteResult string     `json:"write_result,omitempty"`
	WriteAt     *time.Time `json:"write_at,omitempty"`
}

// ReadMark is a Plan's reading of the mark bytes Redis holds for it: absent
// for none, unreadable with why for bytes that are not a mark, else found
// with its round, position, pages and the first ReadingRenewals renewals.
func ReadMark(key PlanKey, raw []byte) PlanReading {
	reading := PlanReading{Plan: key.Plan, Generation: key.Generation, Mark: MarkAbsent}
	if !key.Shard.IsZero() {
		shard := key.Shard
		reading.Shard = &shard
	}
	if raw == nil {
		return reading
	}
	mark, err := DecodeMark(raw)
	if err != nil {
		reading.Mark, reading.Unreadable = MarkUnreadable, err.Error()
		return reading
	}
	reading.Mark = MarkFound
	reading.Round, reading.Position, reading.Pages = mark.Version.EvaluationTime, mark.Position, mark.Pages
	reading.RenewedTotal = len(mark.Renewed)
	reading.Renewed = append([]Renewal(nil), mark.Renewed[:min(len(mark.Renewed), ReadingRenewals)]...)
	return reading
}
