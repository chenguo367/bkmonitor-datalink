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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// SegmentCoverageAfterFold is how many coverages were asked of a Slot's
// segment books after its round folded into them, in this process.
func SegmentCoverageAfterFold() uint64 { return segmentCoverageAfterFold.Load() }

// SegmentSampledRound is whether the round at at is the sampled round of
// the day of the Plan's piece under generation (sampleRound), every member
// written whole.
func SegmentSampledRound(plan execution.PlanIdentity, generation execution.StateGeneration, at, interval int64) bool {
	return sampleRound(segment.PlanKey{Plan: plan, Generation: generation}, at, interval)
}

// SegmentSampledRoundOf is SegmentSampledRound of the family's piece.
func SegmentSampledRoundOf(plan execution.PlanIdentity, generation execution.StateGeneration, family segment.Family, at, interval int64) bool {
	return sampleRound(segment.PlanKey{Plan: plan, Generation: generation, Family: family}, at, interval)
}
