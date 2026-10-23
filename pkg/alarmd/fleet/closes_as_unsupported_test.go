// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import "testing"

// A strategy refused for a capability alarmd does not support closes its
// alerts, by design or not yet supported; one refused for the deployment's
// configuration or a writer's order, which a change lifts, does not, nor
// does a reason the table does not know.
func TestOnlyAnIrreversibleCapabilityRefusalClosesAsUnsupported(t *testing.T) {
	for reason, want := range map[string]bool{
		"QUERY_SOURCE_RETIRED":              true,
		"ALGORITHM_NOT_MIGRATED":            true,
		"UNSUPPORTED_MULTI_ITEM_STRATEGY":   true,
		"DYNAMIC_GROUP_SOURCE_UNCONFIGURED": false,
		"TARGET_PLAN_MISSING":               false,
		"COMPLETION_OFFSET_BELOW_RESERVE":   false,
		"SNAPSHOT_RETENTION_INSUFFICIENT":   false,
		"A_REASON_NOBODY_WROTE":             false,
	} {
		if got := ClosesAsUnsupported(reason); got != want {
			t.Errorf("ClosesAsUnsupported(%s) = %t, want %t", reason, got, want)
		}
	}
}
