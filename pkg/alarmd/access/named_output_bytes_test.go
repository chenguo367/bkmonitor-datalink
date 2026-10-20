// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

type shareReportingConsumer struct {
	execution.QueryExecutionConsumer
	left    uint64
	bounded bool
}

func (consumer shareReportingConsumer) RetainedShareLeftBytes() (uint64, bool) {
	return consumer.left, consumer.bounded
}

// A query with named outputs is sent bounded by what its Slot's share has
// left, read through the consumer the series are delivered to; a query
// without them, or a Slot no share bounds, is sent with no bound.
func TestANamedOutputsQueryIsBoundedByItsSlotsShareLeft(t *testing.T) {
	named := execution.PhysicalQuerySpec{PlanFacts: execution.QueryPlanFacts{NamedOutputs: &execution.NamedOutputs{LegacyRef: "C"}}}
	consumer := &serializedQueryConsumer{QueryExecutionConsumer: shareReportingConsumer{left: 650, bounded: true}}
	if bound := namedOutputBytes(named, consumer); bound == nil || *bound != 650 {
		t.Fatalf("bound %v, want the 650 bytes the share has left", bound)
	}
	if bound := namedOutputBytes(execution.PhysicalQuerySpec{}, consumer); bound != nil {
		t.Fatalf("a query without named outputs is bounded at %d", *bound)
	}
	unbounded := &serializedQueryConsumer{QueryExecutionConsumer: shareReportingConsumer{}}
	if bound := namedOutputBytes(named, unbounded); bound != nil {
		t.Fatalf("a Slot no share bounds put %d on the query", *bound)
	}
}
