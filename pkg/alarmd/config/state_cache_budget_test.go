// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package config

import "testing"

// The State cache's 128 MiB on a 4 GiB container comes out of the
// retained-bytes pool, 1,024 -> 896 MiB (fourth-batch design §4.5, option
// 1), and nothing else moves: the execution ceiling and its query permits
// are derived on their own line.
func TestTheStateCacheIsTakenOutOfThePoolAndNothingElse(t *testing.T) {
	inputs := CapacityInputs{MemoryLimitBytes: 4 << 30}
	if got := DeriveStateCacheBytes(inputs); got != 128<<20 {
		t.Fatalf("state cache budget = %d, want 128 MiB", got)
	}
	if got := DeriveCoordinator(inputs, 1, 1).MaxRetainedBytes; got != 896<<20 {
		t.Fatalf("retained-bytes pool = %d, want 896 MiB", got)
	}
	if got := DeriveScheduler(inputs).ActiveExecutionCeiling; got != (1<<30)/bytesPerParkedExecution {
		t.Fatalf("execution ceiling = %d, want the 1 GiB line's, untouched", got)
	}
}
