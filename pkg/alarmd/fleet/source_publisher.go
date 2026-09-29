// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import "time"

// SourcePublisherReport is the strategy cache publisher's own record of its
// last run, as one replica last read it beside the active set. It is the
// publisher's word and is carried as that: it answers why the publisher
// published the way it did -- published, or blocked and for what reason --
// which nothing on the platform side can see, and nothing decides on it.
// What the publisher runs is read from the platform side (k8s.workloads);
// the version here is what the publisher says, best effort.
//
// A publisher that leaves no record is neither an error nor an empty
// report: State is not_provided and Label says so.
type SourcePublisherReport struct {
	// State is provided, not_provided or unreadable, and Label the words a
	// reader shows it under; see SourcePublisherLabels. Detail names why an
	// unreadable record was: too_large, not_json, field_type or
	// schema_version.
	State  string `json:"state"`
	Label  string `json:"label"`
	Detail string `json:"detail,omitempty"`
	// ReadAt is when the replica read it. WrittenAgeSeconds is how old the
	// record was then, by its own written_at; absent when it does not say.
	ReadAt            time.Time `json:"read_at"`
	WrittenAgeSeconds *int64    `json:"written_age_seconds,omitempty"`
	// The record's fields, bounded. The counts are absent where the record
	// wrote null: the publisher writes null for what it could not tell.
	Writer     string                   `json:"writer,omitempty"`
	Version    string                   `json:"version,omitempty"`
	Outcome    string                   `json:"outcome,omitempty"`
	Reason     string                   `json:"reason,omitempty"`
	Strategies *int64                   `json:"strategies,omitempty"`
	Rejected   *int64                   `json:"rejected,omitempty"`
	Generation *int64                   `json:"generation,omitempty"`
	Backfill   *SourcePublisherBackfill `json:"backfill,omitempty"`
}

// SourcePublisherBackfill is the publisher's copy of the last backfill of
// its records: where it came from, how many it wrote and failed, the first
// record identifier it used and the largest old identifier it started
// above, and its one-line summary.
type SourcePublisherBackfill struct {
	Source       string `json:"source,omitempty"`
	StrategySets *int64 `json:"strategy_sets,omitempty"`
	Written      *int64 `json:"written,omitempty"`
	Failed       *int64 `json:"failed,omitempty"`
	Start        *int64 `json:"start,omitempty"`
	LegacyMax    *int64 `json:"legacy_max,omitempty"`
	Summary      string `json:"summary,omitempty"`
	// UpdatedAgeSeconds is how old the backfill was when the record was
	// read; absent when the record does not say.
	UpdatedAgeSeconds *int64 `json:"updated_age_seconds,omitempty"`
}

// SourcePublisherLabels is the words each state is shown under, closed.
var SourcePublisherLabels = map[string]string{
	"provided":     "写方自述",
	"not_provided": "写方未提供",
	"unreadable":   "写方自述读不出",
}

// newerSourcePublisher says whether candidate was read after current.
func newerSourcePublisher(current, candidate *SourcePublisherReport) bool {
	return candidate != nil && (current == nil || candidate.ReadAt.After(current.ReadAt))
}
