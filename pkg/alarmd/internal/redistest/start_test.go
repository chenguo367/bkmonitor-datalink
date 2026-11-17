// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package redistest

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// A started server answers on its address, keeps nothing across a restart,
// and stops when asked, any number of times.
func TestAStartedServerAnswersRestartsEmptyAndStops(t *testing.T) {
	ctx := context.Background()
	server := Start(t)
	client := server.Client()
	if err := client.Set(ctx, "key", "value", 0).Err(); err != nil {
		t.Fatalf("SET on a started server: %v", err)
	}
	address := server.Addr
	server.Restart()
	if server.Addr != address {
		t.Fatalf("restarted on %s, want the same address %s", server.Addr, address)
	}
	// The old client's pooled connection went with the old process, and the
	// client does not retry, so the restarted server is asked on a new one.
	client = server.Client()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("PING after a restart: %v", err)
	}
	if err := client.Get(ctx, "key").Err(); err != redis.Nil {
		t.Fatalf("GET after a restart = %v, want nothing kept: these servers save nothing", err)
	}
	server.Stop()
	server.Stop()
	if err := client.Ping(ctx).Err(); err == nil {
		t.Fatal("a stopped server still answered")
	}
}

// A server asked for on an address another server already serves is refused
// by name: the one answering there is not this test's, and writing to it
// would be writing to another test's data.
func TestStartingOnAnAddressAnotherServerHoldsIsRefused(t *testing.T) {
	holder := Start(t)
	tb := &recordingTB{TB: t}
	if instance := StartAt(tb, holder.Addr); instance != nil || tb.fatal == "" {
		t.Fatalf("StartAt(%s) on a held address = %v, fatal %q; want refused", holder.Addr, instance, tb.fatal)
	}
	if !strings.Contains(tb.fatal, holder.Addr) {
		t.Fatalf("refusal %q does not name the address", tb.fatal)
	}
	if err := holder.Client().Ping(context.Background()).Err(); err != nil {
		t.Fatalf("the holder stopped answering after the refused start: %v", err)
	}
}

// A server that exits before it answers is reported at once, with what it
// wrote, rather than at the test's deadline: a bad argument is the server's
// own complaint, not a slow start.
func TestAServerThatExitsBeforeAnsweringIsReportedAtOnce(t *testing.T) {
	started := time.Now()
	instance, reason := TryStartAt(t, FreeAddress(t), "--no-such-option", "x")
	if instance != nil || reason == "" {
		t.Fatalf("TryStartAt with a bad option = %v, %q; want refused", instance, reason)
	}
	// Read from its exit it takes milliseconds; waited out, the deadline's two
	// minutes. Ten seconds tells the two apart on any host.
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("the refusal took %s, want it as soon as the server exits", elapsed)
	}
	if !strings.Contains(reason, "exited before it answered") || !strings.Contains(strings.ToLower(reason), "no-such-option") {
		t.Fatalf("refusal %q does not carry the server's own output", reason)
	}
}

// Extra arguments reach the server.
func TestAStartedServerTakesTheExtraArguments(t *testing.T) {
	server := Start(t, "--timeout", "7")
	got, err := server.Client().ConfigGet(context.Background(), "timeout").Result()
	if err != nil || len(got) != 2 || got[1] != "7" {
		t.Fatalf("CONFIG GET timeout = %v, %v; want 7", got, err)
	}
}

// loggingTB keeps the lines a start writes to the test's log, and passes them on.
type loggingTB struct {
	testing.TB
	lines []string
}

func (tb *loggingTB) Logf(format string, args ...any) {
	tb.lines = append(tb.lines, fmt.Sprintf(format, args...))
	tb.TB.Logf(format, args...)
}

// A server that answers says so on the test's log, in one fixed line with the
// version the executable itself reports, its address and the executable, once
// for each start, Restart included; a start that never answers writes no such
// line. A gate counts the tests that ran a real Redis, and on which version,
// by this line.
func TestEveryStartedServerSaysSoOnTheTestLogWithItsVersion(t *testing.T) {
	executable := Server(t)
	if executable == "" {
		return
	}
	output, err := exec.Command(executable, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("%s --version: %v: %s", executable, err, output)
	}
	reported := regexp.MustCompile(` v=([0-9][0-9.]*) `).FindStringSubmatch(string(output))
	if reported == nil {
		t.Fatalf("%s --version wrote no version: %q", executable, output)
	}
	tb := &loggingTB{TB: t}
	server := Start(tb)
	want := fmt.Sprintf("%s version=%s addr=%s executable=%s", StartedMarker, reported[1], server.Addr, executable)
	if len(tb.lines) != 1 || tb.lines[0] != want {
		t.Fatalf("a start logged %q, want the one line %q", tb.lines, want)
	}
	server.Restart()
	if len(tb.lines) != 2 || tb.lines[1] != want {
		t.Fatalf("after a restart the log is %q, want a second %q", tb.lines, want)
	}
	if instance, reason := TryStartAt(tb, FreeAddress(t), "--no-such-option", "x"); instance != nil || reason == "" {
		t.Fatalf("TryStartAt with a bad option = %v, %q; want refused", instance, reason)
	}
	if len(tb.lines) != 2 {
		t.Fatalf("a start that never answered logged %q", tb.lines[2:])
	}
}
