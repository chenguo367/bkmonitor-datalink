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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Slot's queries' phases are summed and the longest of each kept: the sum
// is the time its queries spent, the longest the one that held it.
func TestASlotsQueryPhasesAreSummedAndTheLongestKept(t *testing.T) {
	var totals queryPhaseTotals
	totals.add(execution.ProviderStats{WaitMillis: 3000, BodyMillis: 200})
	totals.add(execution.ProviderStats{WaitMillis: 500, BodyMillis: 1200})
	if totals != (queryPhaseTotals{wait: 3500, body: 1400, waitMax: 3000, bodyMax: 1200}) {
		t.Fatalf("totals %+v, want sums 3500 and 1400, longest 3000 and 1200", totals)
	}
}
