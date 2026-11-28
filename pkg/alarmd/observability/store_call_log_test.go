// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package observability

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

// A Slot that failed on a store that did not answer says on its completion
// row which job's call it was, why it failed, how long it ran and how much of
// its caller's deadline it had: one read tells a store that hung from a call
// the steps before it had left almost no time. A call whose caller had no
// deadline carries no time left, and a row without a store call carries none
// of the keys.
func TestTheCompletionRowSaysHowLongTheStoreCallRanAndWhatTimeItHad(t *testing.T) {
	row := func(observation Observation) map[string]any {
		var output bytes.Buffer
		renderTo(&output, observation)
		var row map[string]any
		if err := json.Unmarshal(output.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		return row
	}
	failed := Observation{Component: ComponentScheduler, Stage: StageSlotCompleted, Result: ResultFailed, ReasonCode: "REDIS_UNAVAILABLE",
		StoreCall: &StoreCallFacts{Client: "runtime", Caller: "runtime_state", Reason: "timeout",
			Waited: 2950 * time.Millisecond, Left: 3 * time.Second, LeftKnown: true}}
	got := row(failed)
	for key, want := range map[string]any{"store_call_client": "runtime", "store_call_caller": "runtime_state",
		"store_call_reason": "timeout", "store_call_waited_ms": float64(2950), "store_call_left_ms": float64(3000)} {
		if got[key] != want {
			t.Errorf("%s = %v, want %v; row %v", key, got[key], want, got)
		}
	}
	failed.StoreCall.LeftKnown = false
	if got := row(failed); got["store_call_waited_ms"] != float64(2950) || got["store_call_left_ms"] != nil {
		t.Errorf("a call without a deadline: waited %v left %v, want the wait and no time left", got["store_call_waited_ms"], got["store_call_left_ms"])
	}
	failed.StoreCall = nil
	for key := range row(failed) {
		if len(key) > len("store_call_") && key[:len("store_call_")] == "store_call_" {
			t.Errorf("a row without a store call carries %s", key)
		}
	}
}
