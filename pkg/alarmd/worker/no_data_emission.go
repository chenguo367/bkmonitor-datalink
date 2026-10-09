// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// noDataEmissionOf is what a write did with its no-data events, read off the
// write's own answer: everything acknowledged when it succeeded; on a partial
// write the events it names as not written, and only those, known unwritten;
// a whole batch whose acknowledgement is unknown counted apart, since it may
// have landed; any other failed batch known unwritten. Counted here, after
// the write, and nowhere earlier: every full round decides a NORMAL for
// every group that reported, so a count of decisions says nothing about
// what reached the alert store.
func noDataEmissionOf(events []contract.TriggerEventV1, err error) *observability.NoDataEmissionFacts {
	notWritten, partial := outputNotWritten(err)
	unknown := err != nil && !partial && isRetryableOutputDependency(err)
	var facts *observability.NoDataEmissionFacts
	for _, event := range events {
		if _, tagged := event.RecordRef.Dimensions[contract.NoDataDimensionTag]; !tagged {
			continue
		}
		if facts == nil {
			facts = &observability.NoDataEmissionFacts{}
		}
		_, skipped := notWritten[event.EventID]
		switch {
		case unknown:
			facts.AckUnknown++
			continue
		case err != nil && (!partial || skipped):
			facts.NotWritten++
			continue
		}
		latest := &facts.LastAbnormal
		switch event.EventKind {
		case contract.TriggerEventAbnormal:
			facts.AbnormalSent++
		case contract.TriggerEventRecovery:
			facts.RecoverySent++
			latest = &facts.LastRecovery
		default:
			continue
		}
		if *latest != nil && (*latest).EvaluationTime > event.EvaluationTime {
			continue
		}
		group, truncated := observability.NoDataEmittedGroup(event.RecordRef.Dimensions, contract.NoDataDimensionTag)
		*latest = &observability.NoDataEmittedEvent{EvaluationTime: event.EvaluationTime, AlertKey: event.DedupeMD5,
			Group: group, GroupTruncated: truncated}
	}
	return facts
}
