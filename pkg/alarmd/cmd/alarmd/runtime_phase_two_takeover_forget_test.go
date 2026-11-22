// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// The production ownership runtime forgets a Query Group on the takeover
// clock its Slot sources share: held under epoch 7, let go of and given back
// under epoch 8, it is taken over at the moment it is held again, where one
// only rebuilt keeps the moment it was first taken over.
func TestTheProductionOwnershipForgetsALetGoQueryGroupOnTheSharedTakeoverClock(t *testing.T) {
	clock := scheduler.NewTakeoverClock()
	runtime := &productionPhaseTwoOwnership{takeovers: clock}
	first, back := time.Unix(1_700_000_000, 0), time.Unix(1_700_000_120, 0)
	fence := func(epoch uint64) execution.OwnerFence {
		return execution.OwnerFence{OwnerID: "worker-1", OwnerEpoch: epoch}
	}
	clock.Anchor("query-group-1", fence(7), first)
	clock.Anchor("query-group-2", fence(7), first)
	runtime.ForgetTakeover("query-group-1")
	if got := clock.Anchor("query-group-1", fence(8), back); !got.Equal(back) {
		t.Fatalf("let go of and given back: taken over at %v, want %v", got, back)
	}
	if got := clock.Anchor("query-group-2", fence(8), back); !got.Equal(first) {
		t.Fatalf("rebuilt: taken over at %v, want %v", got, first)
	}
}
