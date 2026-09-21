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

// A completed Slot reports its retained bytes split by what held them, and the
// split adds up to the total it also reports.
//
// Run through the whole coordinator rather than against the counters, because
// the two numbers a reader compares are produced in different places: the
// phases are charged where the memory is taken and the total is reported on the
// completion. A test that reads the counters cannot see a completion that
// reports one and not the others.
func TestACompletedSlotSaysWhichPhaseHeldItsBytes(t *testing.T) {
	fixture := newFixture(t, true, "")
	result, err := fixture.coordinator.Execute(context.Background(), slotRequest(execution.OperationNormal))
	if err != nil || !result.Completed {
		t.Fatalf("Execute() result=%+v error=%v", result, err)
	}
	usage := result.Usage
	t.Logf("retained: total=%d input=%d gap=%d output=%d", usage.RetainedBytes,
		usage.RetainedInputBytes, usage.RetainedGapBytes, usage.RetainedOutputBytes)

	if sum := usage.RetainedInputBytes + usage.RetainedGapBytes + usage.RetainedOutputBytes; sum != usage.RetainedBytes {
		t.Fatalf("the phases sum to %d against a reported total of %d; a reader cannot tell which of the two "+
			"is the number the pool was charged", sum, usage.RetainedBytes)
	}
	// This Slot reads series, stands under a gap marker and writes side
	// effects, so all three phases are nonzero on a passing run. A fixture
	// that exercised two of them would let the third be wired to anything.
	if usage.RetainedInputBytes == 0 || usage.RetainedGapBytes == 0 || usage.RetainedOutputBytes == 0 {
		t.Fatalf("a Slot that read series, read a gap marker and wrote effects reported input=%d gap=%d output=%d",
			usage.RetainedInputBytes, usage.RetainedGapBytes, usage.RetainedOutputBytes)
	}
	// Three distinct numbers, so one counter reported under three names cannot
	// pass. They are distinct by what the phases hold rather than by
	// arrangement: the series this Slot reads, the one marker it stands under
	// and the effects it produces are three different sizes.
	if usage.RetainedInputBytes == usage.RetainedGapBytes || usage.RetainedGapBytes == usage.RetainedOutputBytes ||
		usage.RetainedInputBytes == usage.RetainedOutputBytes {
		t.Fatalf("two phases report the same number (input=%d gap=%d output=%d), so this case cannot tell a "+
			"real split from one counter reported under several names",
			usage.RetainedInputBytes, usage.RetainedGapBytes, usage.RetainedOutputBytes)
	}
}
