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
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
)

// The Plans a Slot read in slices got past are a slice's own: a read
// evaluates its Plans one after another, so getting past a Plan says only
// that the slice's series of it are done. Plan b, being evaluated as the
// first slice ended, is not got past in the second while its evaluation is
// still at Plan a there - neither landed whole by a replacement past the
// budget in that slice (plansPassed) nor handed as got past to the window
// cut there (takeWindow, which decideLanding reads).
func TestThePlansASliceGotPastAreItsOwn(t *testing.T) {
	fixture := newWindowedFixture(t)
	stream := fixture.stream
	stream.slices = &sliceReading{}
	a, b := execution.PlanIdentity{StrategyID: "a"}, execution.PlanIdentity{StrategyID: "b"}
	stream.beginSlice(inputslice.Leaf{}, false)
	stream.beginEvaluating(a)
	stream.beginEvaluating(b)
	if _, past := stream.plansPassed()[a]; !past {
		t.Fatal("fixture: the first slice did not get past Plan a, evaluating b after it")
	}
	stream.beginSlice(inputslice.Leaf{}, false)
	stream.beginEvaluating(a)
	if passed := stream.plansPassed(); len(passed) != 0 {
		t.Fatalf("the second slice, evaluating its first Plan, got past %v", passed)
	}
	if len(stream.fill.passed) != 0 {
		t.Fatalf("a window cut now would be handed %v as got past", stream.fill.passed)
	}
}
