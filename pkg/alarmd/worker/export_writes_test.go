// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "context"

// WatchPorts has every coordinator made after it wrap its ports with watch
// (writes_guard_test.go).
func WatchPorts(watch func(Ports) Ports) { watchedPorts = watch }

// WriteCleared is how a write sent with ctx stands (014 ⑩): attempt, that
// ctx is a Slot attempt's that keeps its writes' ledger (slotWrites);
// cleared, that beforeWrite cleared it, and unfenced, as a write whose script
// does not run the owner fence; begun, that the attempt owes no BeginSlot.
func WriteCleared(ctx context.Context) (attempt, cleared, unfenced, begun bool) {
	writes := slotWritesFrom(ctx)
	if writes == nil {
		return false, false, false, false
	}
	clearance, cleared := ctx.Value(writeClearanceContextKey{}).(writeClearance)
	writes.mu.Lock()
	defer writes.mu.Unlock()
	return true, cleared, clearance.unfenced, writes.owed == nil
}
