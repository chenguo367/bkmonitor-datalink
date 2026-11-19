// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// ExecutionSlotWaitFacts is what a Slot's dispatch waited for an execution
// slot, carried on its slot_started line: how long the dispatcher held the
// Query Group ready to go - queued and past its readiness - while every
// execution slot was in use, and the cohort of the Query Group's shortest
// due interval its queue place was given under. A Slot that found a slot free
// carries a zero wait; one started without a dispatch carries none.
type ExecutionSlotWaitFacts struct {
	WaitMillis float64
	Cohort     string
}

// Waited is whether the dispatch waited at all.
func (facts ExecutionSlotWaitFacts) Waited() bool { return facts.WaitMillis > 0 }

// ExecutionSlotWaitOperations is every operation the waits are counted
// under: the Slot operations a dispatch starts, and other.
var ExecutionSlotWaitOperations = []Operation{OperationNormal, OperationRetry, OperationReplay, OperationProbe, OperationSupplement, OperationOther}

// NormalizeExecutionSlotWaitOperation keeps the Slot operations and folds
// the rest into other.
func NormalizeExecutionSlotWaitOperation(operation Operation) Operation {
	switch operation {
	case OperationNormal, OperationRetry, OperationReplay, OperationProbe, OperationSupplement:
		return operation
	}
	return OperationOther
}
