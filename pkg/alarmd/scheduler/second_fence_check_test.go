// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// secondFenceStore fails only the fence check taken just before the Slot runs.
type secondFenceStore struct {
	*countingOwnershipStore
	checkErr error
}

func (store *secondFenceStore) CheckFence(context.Context, execution.OwnerFence) error {
	return store.checkErr
}

// The check taken just before the Slot runs is the one that ended the session on
// any failure, so a store that could not be asked at that moment lost the Query
// Group a lease nobody had refused. Through the real session a Runner keeps it,
// and runs the Slot once the store answers; a refusal still ends it.
func TestTheFenceCheckBeforeTheSlotEndsTheSessionOnlyOnARefusal(t *testing.T) {
	at := time.Unix(200, 0)
	slot := frozenSlot("query-group-1")
	open := func(t *testing.T, checkErr error) (*Runner, *ownership.Session, *secondFenceStore, *int) {
		t.Helper()
		store := &secondFenceStore{countingOwnershipStore: &countingOwnershipStore{
			fence: slot.Dispatch.OwnerFence, deadline: at.Add(time.Hour), assignment: testAssignment("worker-1", 3),
		}, checkErr: checkErr}
		session, err := ownership.OpenSession(context.Background(), store, "query-group-1", "worker-1", at, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		executed := 0
		runner, err := NewRunner("query-group-1", session, &fakeSlotSource{slot: slot},
			workflowExecutor(func(context.Context, execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
				executed++
				return execution.SlotExecutionResult{Completed: true}, nil
			}), NewFlightCoordinator(), func() time.Time { return at })
		if err != nil {
			t.Fatal(err)
		}
		return runner, session, store, &executed
	}

	t.Run("a store that could not be asked", func(t *testing.T) {
		unreachable := errors.New("ownership store unreachable")
		runner, session, store, executed := open(t, unreachable)
		if _, attempted, err := runner.RunOne(context.Background()); attempted || !errors.Is(err, unreachable) || *executed != 0 {
			t.Fatalf("RunOne() attempted=%t err=%v executed=%d, want the failure returned and no Slot", attempted, err, *executed)
		}
		if deadline := session.Deadline(); deadline.IsZero() {
			t.Fatal("a store that could not be asked ended the session")
		}
		store.checkErr = nil
		if _, _, err := runner.RunOne(context.Background()); err != nil || *executed != 1 {
			t.Fatalf("once the store answers err=%v executed=%d, want the Slot run", err, *executed)
		}
	})

	t.Run("a refusal", func(t *testing.T) {
		runner, session, _, executed := open(t, ownership.ErrNotDesired)
		if _, attempted, err := runner.RunOne(context.Background()); attempted || !errors.Is(err, ownership.ErrNotDesired) || *executed != 0 {
			t.Fatalf("RunOne() attempted=%t err=%v executed=%d, want the refusal returned and no Slot", attempted, err, *executed)
		}
		if deadline := session.Deadline(); !deadline.IsZero() {
			t.Fatalf("a refusal left the session admitting until %s", deadline)
		}
	})
}
