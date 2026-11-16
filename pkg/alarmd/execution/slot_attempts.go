// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// SlotBeginResults is every word a Slot attempt's BeginSlot is counted under
// (slot_begin_total): its status, in lower case.
var SlotBeginResults = []string{
	SlotBeginResult(ProgressCommitted), SlotBeginResult(ProgressConflict), SlotBeginResult(ProgressStaleOwner),
	SlotBeginResult(ProgressRetryableIO), SlotBeginResult(ProgressContentMoved),
}

// SlotBeginResult is the word a BeginSlot that returned status is counted
// under.
func SlotBeginResult(status ProgressCommitStatus) string {
	return strings.ToLower(string(status))
}

// Why a Slot attempt that began its Slot returned without writing it down
// (slot_not_committed_total): its data was not in yet (a readiness
// deferral), its finalization asked for the attempt again (SNAPSHOT_RETRY,
// EXACT_SET_BLOCKED), or anything else it returned on.
const (
	SlotNotCommittedQueryNotReady   = contract.ReasonQueryNotReady
	SlotNotCommittedSnapshotRetry   = string(FinalizationSnapshotRetry)
	SlotNotCommittedExactSetBlocked = string(FinalizationExactSetBlocked)
	SlotNotCommittedOther           = "other"
)

// SlotNotCommittedReasons lists them.
var SlotNotCommittedReasons = []string{SlotNotCommittedQueryNotReady, SlotNotCommittedSnapshotRetry,
	SlotNotCommittedExactSetBlocked, SlotNotCommittedOther}
