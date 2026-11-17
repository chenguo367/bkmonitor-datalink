// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// Why the normalization of a query's answer let a point go rather than hand
// it on, closed. Every place the normalization skips a point is one of these;
// an answer it cannot read is refused whole and is no drop.
//
//   - outside_range_before, outside_range_after: a point the provider
//     returned before the range the query accepts, or at or after its end -
//     the provider looks back further than asked, or answered another
//     window. The series stays only if some point of it was inside.
//   - row_missing_columns: a range query's row (the trends read) without the
//     time or the value column.
const (
	PointDroppedOutsideRangeBefore = "outside_range_before"
	PointDroppedOutsideRangeAfter  = "outside_range_after"
	PointDroppedRowMissingColumns  = "row_missing_columns"
)

// PointDropReasons is every reason, in the order the metric is built in.
var PointDropReasons = []string{PointDroppedOutsideRangeBefore, PointDroppedOutsideRangeAfter, PointDroppedRowMissingColumns}

// PointDrops is the points let go, by reason. Zero is none let go, and is a
// reading.
type PointDrops struct {
	OutsideRangeBefore uint64 `json:"outside_range_before,omitempty"`
	OutsideRangeAfter  uint64 `json:"outside_range_after,omitempty"`
	RowMissingColumns  uint64 `json:"row_missing_columns,omitempty"`
}

// Add is the two together.
func (drops PointDrops) Add(other PointDrops) PointDrops {
	return PointDrops{OutsideRangeBefore: drops.OutsideRangeBefore + other.OutsideRangeBefore,
		OutsideRangeAfter: drops.OutsideRangeAfter + other.OutsideRangeAfter,
		RowMissingColumns: drops.RowMissingColumns + other.RowMissingColumns}
}

// Of is the points let go for one reason; zero for a reason not in the set.
func (drops PointDrops) Of(reason string) uint64 {
	switch reason {
	case PointDroppedOutsideRangeBefore:
		return drops.OutsideRangeBefore
	case PointDroppedOutsideRangeAfter:
		return drops.OutsideRangeAfter
	case PointDroppedRowMissingColumns:
		return drops.RowMissingColumns
	}
	return 0
}

// Total is every point let go.
func (drops PointDrops) Total() uint64 {
	return drops.OutsideRangeBefore + drops.OutsideRangeAfter + drops.RowMissingColumns
}

// Facts is the drops for a line, nil when none was let go.
func (drops PointDrops) Facts() *PointDrops {
	if drops.Total() == 0 {
		return nil
	}
	return &drops
}
