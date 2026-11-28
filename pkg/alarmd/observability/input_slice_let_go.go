// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// A slice let go as it streamed, its input past the limit the slice being
// read may hold, is said on a line of its own: what the slice held and the
// batch that took it past, the limit, the limit it would have with nothing
// beside it, and what the limit counted beside it - the reads ahead and what
// the dispatch holds, each the 1/k a consumer counts, and the stream's own
// phases beside the slice less what its windows in flight give back. A
// reading tells a slice let go for its own size from one let go for what was
// held beside it, which no split mends (015 C1, the 1.0 next release gate).

// StageInputSliceLetGo is a slice let go past its limit
// (InputSliceLetGoFacts).
const StageInputSliceLetGo = "input_slice_let_go"

// InputSliceLetGoFacts is what one slice let go held, and what its limit
// counted beside it. Consumers is k, the streams consuming the Slot's
// slices side by side, one for a Slot's own stream; ShareBytes the Query
// Group's share; OwnLimitBytes the limit with nothing held beside the slice,
// a quarter of the share over k. ForItsSize is the slice and its batch past
// OwnLimitBytes: a slice let go for its own size, which the next Slot starts
// split for (InputSliceStartDiscarded); one that is not was let go for what
// was held beside it, and its split is the Slot's alone. Dispatch is what a
// consumer counts of the dispatch, by phase (input, gap, output, state), 1/k
// of it; Own the stream's own phases beside the slice's input, less what its
// windows in flight give back.
type InputSliceLetGoFacts struct {
	Consumers          uint64    `json:"consumers"`
	ShareBytes         uint64    `json:"share_bytes"`
	LimitBytes         uint64    `json:"limit_bytes"`
	OwnLimitBytes      uint64    `json:"own_limit_bytes"`
	SliceBytes         uint64    `json:"slice_bytes"`
	BatchBytes         uint64    `json:"batch_bytes"`
	LargestSeriesBytes uint64    `json:"largest_series_bytes"`
	AheadBytes         uint64    `json:"ahead_bytes"`
	Dispatch           [4]uint64 `json:"dispatch_bytes"`
	Own                [4]uint64 `json:"own_bytes"`
	ApplyHeldBytes     uint64    `json:"apply_held_bytes"`
	ForItsSize         bool      `json:"for_its_size"`
}

// DispatchBytes is what a consumer counted of the dispatch, every phase.
func (facts *InputSliceLetGoFacts) DispatchBytes() uint64 {
	return facts.Dispatch[0] + facts.Dispatch[1] + facts.Dispatch[2] + facts.Dispatch[3]
}
