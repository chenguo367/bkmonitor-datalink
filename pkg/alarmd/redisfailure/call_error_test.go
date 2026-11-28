// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package redisfailure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// A CallError reads as the call's own error, names the same reason, and is
// found through any wrapping that keeps the chain.
func TestACallErrorReadsAsTheCallsOwnError(t *testing.T) {
	call := &CallError{Client: "runtime", Caller: CallerRuntimeState, Reason: Timeout, Err: context.DeadlineExceeded}
	wrapped := fmt.Errorf("complete Slot: %w", fmt.Errorf("gap preflight: %w", call))
	if wrapped.Error() != "complete Slot: gap preflight: "+context.DeadlineExceeded.Error() {
		t.Fatalf("text %q, want the call's own words", wrapped)
	}
	if !errors.Is(wrapped, context.DeadlineExceeded) || Reason(wrapped) != Timeout || CallErrorOf(wrapped) != call {
		t.Fatalf("wrapped mark: is deadline %v, reason %s, found %v", errors.Is(wrapped, context.DeadlineExceeded), Reason(wrapped), CallErrorOf(wrapped))
	}
	if CallErrorOf(context.DeadlineExceeded) != nil || CallErrorOf(nil) != nil {
		t.Fatal("an unmarked error has a CallError")
	}
}

// The store not answering is exactly the five reasons that say so.
func TestStoreUnavailableIsTheReasonsThatSayTheStoreDidNotAnswer(t *testing.T) {
	want := map[string]bool{ConnectionClosed: true, ConnectionRefused: true, SentinelUnreachable: true, Timeout: true, PoolTimeout: true}
	for _, reason := range append(Reasons, "") {
		if StoreUnavailable(reason) != want[reason] {
			t.Errorf("StoreUnavailable(%q) = %v, want %v", reason, StoreUnavailable(reason), want[reason])
		}
	}
}

// A call ran out its time when it was issued with time left and waited all
// of it; one with time to spare, one whose caller gave no deadline, and one
// issued with none left did not.
func TestACallRanOutItsTimeWhenItWaitedAllItWasGiven(t *testing.T) {
	for name, test := range map[string]struct {
		call *CallError
		want bool
	}{
		"waited all of it":   {&CallError{Waited: 300 * time.Millisecond, Left: 300 * time.Millisecond, LeftKnown: true}, true},
		"waited past it":     {&CallError{Waited: 310 * time.Millisecond, Left: 300 * time.Millisecond, LeftKnown: true}, true},
		"time to spare":      {&CallError{Waited: 2 * time.Millisecond, Left: 300 * time.Millisecond, LeftKnown: true}, false},
		"no deadline":        {&CallError{Waited: 300 * time.Millisecond}, false},
		"none left at issue": {&CallError{Waited: time.Millisecond, LeftKnown: true}, false},
		"no call":            {nil, false},
	} {
		if got := test.call.RanOutItsTime(); got != test.want {
			t.Errorf("%s: RanOutItsTime = %v, want %v", name, got, test.want)
		}
	}
}

// A witness keeps the call of a job it keeps that ran out its time and waited
// longest, the first noted among equal waits, whatever its attempt's
// goroutines note and in whatever order; a call with time to spare and
// another job's call are not kept, and a context without a witness takes
// nothing.
func TestAWitnessKeepsTheLongestWaitOfItsJobsCallsThatRanOutTheirTime(t *testing.T) {
	own := func(caller string) bool { return caller == CallerRuntimeState }
	ranOut := func(caller string, waited time.Duration) *CallError {
		return &CallError{Caller: caller, Reason: Timeout, Waited: waited, Left: waited, LeftKnown: true, Err: context.DeadlineExceeded}
	}
	ctx, witness := WithWitness(context.Background(), own)
	NoteWitnessed(ctx, &CallError{Caller: CallerRuntimeState, Reason: ConnectionRefused, Waited: time.Millisecond, Left: time.Second, LeftKnown: true})
	NoteWitnessed(ctx, ranOut(CallerDiagnosticRead, time.Hour))
	if witness.Call() != nil {
		t.Fatalf("kept %+v, want nothing yet", witness.Call())
	}
	first := ranOut(CallerRuntimeState, time.Second)
	NoteWitnessed(ctx, first)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			NoteWitnessed(ctx, ranOut(CallerRuntimeState, time.Second))
			NoteWitnessed(ctx, ranOut(CallerRuntimeState, time.Millisecond))
		}()
	}
	wait.Wait()
	if witness.Call() != first {
		t.Fatalf("kept %+v, want the first of the longest waits", witness.Call())
	}
	longer := ranOut(CallerRuntimeState, 2*time.Second)
	NoteWitnessed(ctx, longer)
	if witness.Call() != longer {
		t.Fatalf("kept %+v, want the longer wait", witness.Call())
	}

	ctx, witness = WithWitness(context.Background(), own)
	var calls []*CallError
	for index := range 32 {
		calls = append(calls, ranOut(CallerRuntimeState, time.Duration(index+1)*time.Millisecond))
	}
	for _, call := range calls {
		wait.Add(1)
		go func() {
			defer wait.Done()
			NoteWitnessed(ctx, call)
		}()
	}
	wait.Wait()
	if witness.Call() != calls[len(calls)-1] {
		t.Fatalf("calls noted at once: kept %+v, want the longest wait", witness.Call())
	}

	NoteWitnessed(context.Background(), first)
	var none *Witness
	if none.Call() != nil {
		t.Fatal("a nil witness has a call")
	}
}
