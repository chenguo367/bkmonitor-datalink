// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// frameExecutor plays a supervisor whose started and complete reports arrive
// after the given delays, and records when Exec gave up on the stream.
type frameExecutor struct {
	beforeStart, beforeComplete time.Duration
	neverStart, neverComplete   bool
	invoked, startedAt, gaveUp  time.Time
}

func (f *frameExecutor) Stream(ctx context.Context, request StreamRequest) (StreamResult, error) {
	f.invoked = time.Now()
	nonce := request.Argv[4]
	wait := func(delay time.Duration, never bool) error {
		if never {
			<-ctx.Done()
			f.gaveUp = time.Now()
			return ctx.Err()
		}
		select {
		case <-time.After(delay):
			return nil
		case <-ctx.Done():
			f.gaveUp = time.Now()
			return ctx.Err()
		}
	}
	if err := wait(f.beforeStart, f.neverStart); err != nil {
		return StreamResult{}, err
	}
	f.startedAt = time.Now()
	io.WriteString(request.Stdout, fmt.Sprintf(`{"nonce":%q,"event":"started"}`+"\n", nonce))
	if err := wait(f.beforeComplete, f.neverComplete); err != nil {
		return StreamResult{}, err
	}
	io.WriteString(request.Stdout, fmt.Sprintf(`{"nonce":%q,"event":"complete","exit_code":0}`+"\n", nonce))
	zero := 0
	return StreamResult{ExitCode: &zero}, nil
}

// A slow connection or interpreter start on a loaded node does not consume
// the command's deadline: the supervisor arms its timer only when it starts,
// and its report one second after a 100 ms deadline is within the grace that
// follows. Counted from the call instead, the stream would have been cut at
// 4.1 s, before the supervisor's own report.
func TestTheExecBoundCountsFromTheSupervisorStart(t *testing.T) {
	t.Parallel()
	provider, _, target := runtimeProvider(t)
	executor := &frameExecutor{beforeStart: StartupAllowance - 500*time.Millisecond, beforeComplete: time.Second}
	provider.options.Executor = executor
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}, TimeoutMS: 100})
	if err != nil || result.RemoteState != RemoteCompleted || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("the supervisor's report within its grace was not taken: %+v %v", result, err)
	}
}

// Past the deadline the stream is held for TerminationGrace after the
// supervisor's start plus the request's own timeout, and then given up as
// unknown rather than waiting on the target forever.
func TestTheExecBoundEndsAtTheDeadlinePlusGrace(t *testing.T) {
	t.Parallel()
	provider, _, target := runtimeProvider(t)
	executor := &frameExecutor{neverComplete: true}
	provider.options.Executor = executor
	timeout := 100 * time.Millisecond
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}, TimeoutMS: timeout.Milliseconds()})
	requireCode(t, err, RemoteUnknown)
	if result.RemoteState != RemoteUnknown {
		t.Fatalf("an unreported command was not unknown: %+v", result)
	}
	if held := executor.gaveUp.Sub(executor.startedAt); held < timeout+TerminationGrace {
		t.Fatalf("stream given up %v after start, before the deadline plus grace %v", held, timeout+TerminationGrace)
	}
}

// A supervisor that never reports started is given up after StartupAllowance,
// whatever the command's own deadline: a 10 s command does not hold an
// unstarted stream for 10 s plus grace.
func TestTheExecStartupIsBounded(t *testing.T) {
	t.Parallel()
	provider, _, target := runtimeProvider(t)
	executor := &frameExecutor{neverStart: true}
	provider.options.Executor = executor
	timeout := 10 * time.Second
	result, err := provider.Exec(context.Background(), ExecRequest{Target: target, Argv: []string{"true"}, TimeoutMS: timeout.Milliseconds()})
	requireCode(t, err, RemoteUnknown)
	if result.RemoteState != RemoteUnknown {
		t.Fatalf("an unstarted stream was not unknown: %+v", result)
	}
	held := executor.gaveUp.Sub(executor.invoked)
	if held < StartupAllowance || held >= timeout {
		t.Fatalf("unstarted stream given up after %v, want from %v and before the command's %v", held, StartupAllowance, timeout)
	}
}
