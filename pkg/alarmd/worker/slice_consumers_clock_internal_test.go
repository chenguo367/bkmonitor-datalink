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
	"time"
)

// The dispatch's clock stops as its consumers begin: what it spent since its
// first slice completed is charged to the phase it was in, and a landing it
// takes while they read charges nothing (enter) - the wall clock of their
// reads is theirs, and summed with the dispatch's would be counted twice.
func TestTheDispatchsClockStopsWhileItsConsumersRead(t *testing.T) {
	dispatch := &streamedExecution{slotStream: &slotStream{}}
	dispatch.clockPhase, dispatch.clockSince = slotPhaseOther, time.Now().Add(-time.Second)
	dispatch.stopClockForConsumers()
	if !dispatch.clockSince.IsZero() || dispatch.spentByPhase[slotPhaseOther] < uint64(time.Second) {
		t.Fatalf("clock since %v, other %v: want the second it ran charged and the clock stopped",
			dispatch.clockSince, time.Duration(dispatch.spentByPhase[slotPhaseOther]))
	}
	charged := dispatch.spentByPhase
	leave := dispatch.enter(slotPhaseLand)
	leave()
	if dispatch.spentByPhase != charged || !dispatch.clockSince.IsZero() {
		t.Fatalf("a landing while the consumers read charged %v, clock since %v: want nothing, the clock stopped",
			dispatch.spentByPhase, dispatch.clockSince)
	}
	// A clock not running is left so: nothing is charged from its zero.
	idle := &streamedExecution{slotStream: &slotStream{}}
	idle.stopClockForConsumers()
	if idle.spentByPhase != ([slotPhaseCount]uint64{}) {
		t.Fatalf("a clock not running charged %v", idle.spentByPhase)
	}
}
