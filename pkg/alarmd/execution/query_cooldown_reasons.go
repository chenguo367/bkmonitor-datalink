// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"

// QueryCooldownReasons is every reason a Query Group can be in the degraded
// pool under, closed: the reasons the access layer names a primary query
// unavailable for, the two AttributedReason puts in place of a reason no
// attempt gave (QUERY_NOT_ATTEMPTED, QUERY_REASON_UNRECORDED), and the
// timeout the scheduler reads a backend that did not answer as
// (QUERY_TIMEOUT, one of the first). The pool keeps its entry's reason
// (SlotExecutionResult.QueryUnavailableReason, its record's Reason), and the
// pages name it from this list; a reason outside it is read as other. The
// access layer's test holds the list to what that layer names.
var QueryCooldownReasons = []ReasonCode{
	ReasonCode(contract.ReasonDetectIntervalStorageNotSliding),
	ReasonCode(contract.ReasonExecutionBudgetExhausted),
	ReasonCode(contract.ReasonQueryNamedOutputDuplicateKey),
	ReasonCode(contract.ReasonQueryPartial),
	ReasonCode(contract.ReasonQueryRejected),
	ReasonCode(contract.ReasonQueryTargetMissing),
	ReasonCode(contract.ReasonQueryTimeout),
	ReasonCode(contract.ReasonQueryUnavailable),
	ReasonCode(contract.ReasonReadinessBudgetInvalid),
	ReasonQueryNotAttempted,
	ReasonQueryReasonUnrecorded,
}
