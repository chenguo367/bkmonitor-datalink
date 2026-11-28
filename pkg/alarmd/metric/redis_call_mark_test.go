// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package metric

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// A call the store did not answer comes back marked: the error go-redis
// hands the caller is a CallError carrying the client, the job, the reason,
// how long the call ran and how much of its caller's deadline it had, and
// errors.Is still reaches the call's own error.
func TestACallTheStoreDidNotAnswerComesBackMarked(t *testing.T) {
	hook := NewRecorder(BuildInfo{}).RedisHook("runtime")
	ctx, cancel := context.WithTimeout(redisfailure.WithCaller(context.Background(), redisfailure.CallerRuntimeState), 2*time.Second) // fixed-wait: the caller's deadline the mark reads back; nothing waits on it
	defer cancel()
	ctx, _ = hook.BeforeProcess(ctx, nil)
	cmd := failedCommand("hmget", context.DeadlineExceeded)
	call := redisfailure.CallErrorOf(hook.AfterProcess(ctx, cmd))
	if call == nil {
		t.Fatal("AfterProcess returned no CallError for a call that ran out its time with time left")
	}
	if call.Client != "runtime" || call.Caller != redisfailure.CallerRuntimeState || call.Reason != redisfailure.Timeout {
		t.Fatalf("mark = %+v, want the runtime client, the Runtime State job and timeout", call)
	}
	if !call.LeftKnown || call.Left <= time.Second || call.Left > 2*time.Second || call.Waited < 0 || call.Waited >= time.Second {
		t.Fatalf("mark left %s (known %v) waited %s, want about the caller's 2s left and a call that ran for no time", call.Left, call.LeftKnown, call.Waited)
	}
	if !errors.Is(call, context.DeadlineExceeded) || call.Error() != context.DeadlineExceeded.Error() {
		t.Fatalf("mark %q does not read as the call's own error", call)
	}

	// A caller with no deadline gives the call no time to report.
	ctx, _ = hook.BeforeProcess(context.Background(), nil)
	if call := redisfailure.CallErrorOf(hook.AfterProcess(ctx, failedCommand("get", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}))); call == nil || call.LeftKnown {
		t.Fatalf("mark of a call without a deadline = %+v, want one that knows no time left", call)
	}
}

// What is not the store failing is not marked: an empty result, a script
// cache miss, an error reply, a reply of the wrong kind, a call its caller
// cancelled once it was out, a call issued with its deadline already past,
// a success, and an error already marked by a hook of the client.
func TestWhatIsNotTheStoreFailingIsNotMarked(t *testing.T) {
	hook := NewRecorder(BuildInfo{}).RedisHook("runtime")
	state := redisfailure.WithCaller(context.Background(), redisfailure.CallerRuntimeState)
	afterIssue := func(ctx context.Context, err error) error {
		ctx, _ = hook.BeforeProcess(ctx, nil)
		return hook.AfterProcess(ctx, failedCommand("get", err))
	}
	cancelling, cancel := context.WithCancel(state)
	issued, _ := hook.BeforeProcess(cancelling, nil)
	cancel()
	if err := hook.AfterProcess(issued, failedCommand("get", context.Canceled)); err != nil {
		t.Errorf("a call cancelled after it was out came back %v, want nothing", err)
	}
	spent, stop := context.WithDeadline(state, time.Now().Add(-time.Second)) // fixed-wait: a deadline already past, the call issued with no time left; nothing waits on it
	defer stop()
	already := &redisfailure.CallError{Client: "runtime", Reason: redisfailure.Timeout, Err: context.DeadlineExceeded}
	for name, check := range map[string]func() error{
		"empty result":   func() error { return afterIssue(state, redis.Nil) },
		"script miss":    func() error { return afterIssue(state, errors.New("NOSCRIPT No matching script. Please use EVAL.")) },
		"error reply":    func() error { return afterIssue(state, replyError("ERR wrong number of arguments")) },
		"other failure":  func() error { return afterIssue(state, errors.New("redis: unexpected type")) },
		"spent at issue": func() error { return afterIssue(spent, context.DeadlineExceeded) },
		"success":        func() error { return afterIssue(state, nil) },
		"already marked": func() error { return afterIssue(state, already) },
	} {
		if err := check(); err != nil {
			t.Errorf("%s came back %v, want nothing", name, err)
		}
	}
}

// A batch the store did not answer is marked as one call is: every member
// go-redis failed with the connection's error carries the mark, and the batch
// returns it. A batch one member of which had its answer is returned as it
// came, so no answered member is marked failed; its failed members still
// carry the mark, and a member's error reply keeps its own words.
func TestABatchTheStoreDidNotAnswerIsMarkedAndAnAnsweredMemberIsNot(t *testing.T) {
	hook := NewRecorder(BuildInfo{}).RedisHook("runtime")
	state := redisfailure.WithCaller(context.Background(), redisfailure.CallerControlPlane)
	ctx, _ := hook.BeforeProcessPipeline(state, nil)
	failed := []redis.Cmder{failedCommand("hget", replyError("WRONGTYPE Operation against a key holding the wrong kind of value")),
		failedCommand("hget", context.DeadlineExceeded), failedCommand("hget", context.DeadlineExceeded)}
	call := redisfailure.CallErrorOf(hook.AfterProcessPipeline(ctx, failed))
	if call == nil || call.Caller != redisfailure.CallerControlPlane || call.Reason != redisfailure.Timeout {
		t.Fatalf("batch mark = %+v, want the control plane's timeout", call)
	}
	if redisfailure.CallErrorOf(failed[0].Err()) != nil || redisfailure.CallErrorOf(failed[1].Err()) == nil || redisfailure.CallErrorOf(failed[2].Err()) == nil {
		t.Fatalf("members %v %v %v, want the error reply unmarked and both timeouts marked", failed[0].Err(), failed[1].Err(), failed[2].Err())
	}

	ctx, _ = hook.BeforeProcessPipeline(state, nil)
	answered := failedCommand("hget", nil)
	mixed := []redis.Cmder{answered, failedCommand("hget", context.DeadlineExceeded)}
	if err := hook.AfterProcessPipeline(ctx, mixed); err != nil {
		t.Fatalf("a batch with an answered member came back %v, want nothing", err)
	}
	if answered.Err() != nil || redisfailure.CallErrorOf(mixed[1].Err()) == nil {
		t.Fatalf("members %v %v, want the answered one untouched and the failed one marked", answered.Err(), mixed[1].Err())
	}
}

// Through a real Sentinel client, a call and a batch whose Sentinels hang
// past the caller's deadline both reach the caller marked: go-redis returns
// what the hook returns, for Exec too.
func TestARealClientsCallAndBatchReachTheCallerMarked(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	client := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "mymaster", SentinelAddrs: []string{hungListener(t)}, MaxRetries: -1,
		DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second})
	client.AddHook(r.RedisHook("runtime"))
	t.Cleanup(func() { _ = client.Close() })
	withTime := func() context.Context {
		ctx, cancel := context.WithTimeout(redisfailure.WithCaller(context.Background(), redisfailure.CallerRuntimeState), 300*time.Millisecond) // fixed-wait: the caller's time left, which the hung Sentinel runs out
		t.Cleanup(cancel)
		return ctx
	}
	call := redisfailure.CallErrorOf(client.GetRange(withTime(), "k", 0, 10).Err())
	if call == nil || call.Caller != redisfailure.CallerRuntimeState || !redisfailure.StoreUnavailable(call.Reason) || !call.LeftKnown || call.Left > 300*time.Millisecond {
		t.Fatalf("call came back %+v, want a mark of the Runtime State job within its 300ms", call)
	}
	var member redis.Cmder
	_, err := client.Pipelined(withTime(), func(pipe redis.Pipeliner) error {
		member = pipe.GetRange(context.Background(), "k", 0, 10)
		return nil
	})
	if batch := redisfailure.CallErrorOf(err); batch == nil || batch.Caller != redisfailure.CallerRuntimeState {
		t.Fatalf("Exec returned %v, want the batch's mark", err)
	}
	if redisfailure.CallErrorOf(member.Err()) == nil {
		t.Fatalf("member error %v, want it marked", member.Err())
	}
}
