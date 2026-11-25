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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// RememberSliceShapeForTest has coordinator remember of group what its
// completed Slots leave once slices they started in were let go and kept
// split (inputslice.Tree.SplitSized): a last read of bytes and series at
// revision, sliced on dimensions and cut on the first of them, in slices
// counted slices, and shape, the leaves the next Slot starts in. It is for a
// case to start a Slot in a shape as large as a Slot may start in without
// reading the rounds that grow one.
func RememberSliceShapeForTest(coordinator *SlotExecutionCoordinator, group execution.QueryGroupIdentity,
	revision execution.QueryRevision, dimensions []string, shape inputslice.Shape, slices int, bytes, series uint64) {
	coordinator.slicedInput.remember(group, slicedReading{bytes: bytes, series: series, dimensions: dimensions,
		slices: slices, shape: shape, revision: revision, cut: inputslice.FixedCut(dimensions[0]),
		cutFrom: observability.InputSliceCutOnRootSplit})
}
