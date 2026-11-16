// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A series whose PRIMARY the provider answered short of FULL is decided as
// an incomplete input, not evaluated (mergePrimaryIncompleteSeries): it was
// taken up, and a Slot all of whose series were so is no Slot that lost any.
// Counted as not taken up, every PARTIAL answer read as series lost.
func TestASeriesAnsweredPartialIsTakenUpNotLost(t *testing.T) {
	fixture := newSlicedFixture(t, slicedHosts(8, 1<<10), sliceableFacts(), 200)
	fixture.source.completeness = func(int, int) execution.Completeness { return execution.CompletenessPartial }
	result, err := fixture.coordinator.Execute(context.Background(), windowedRequest())
	if err != nil || !result.Completed {
		t.Fatalf("result=%+v err=%v, want the Slot completed on its PARTIAL answer", result, err)
	}
	if len(fixture.source.partial) == 0 {
		t.Fatal("no series was answered PARTIAL")
	}
	if lost := result.Usage.SeriesNotEvaluated; lost != ([len(lost)]uint64{}) {
		t.Fatalf("series not taken up %v, want none: every series answered PARTIAL was decided incomplete", lost)
	}
}
