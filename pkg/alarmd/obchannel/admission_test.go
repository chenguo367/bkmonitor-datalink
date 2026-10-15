// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeMemory admits holds up to a byte room and counts what it gave out.
type fakeMemory struct {
	mu   sync.Mutex
	room uint64
	held uint64
}

func (m *fakeMemory) Hold(bytes uint64) (func(), bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held+bytes > m.room {
		return nil, false
	}
	m.held += bytes
	return func() {
		m.mu.Lock()
		m.held -= bytes
		m.mu.Unlock()
	}, true
}

// Reads are admitted while the memory they may take fits under the line:
// how many run at once follows the memory, not a fixed count.
func TestReadsRunTogetherWhileTheirMemoryFits(t *testing.T) {
	gate := newAdmission(1 << 20)
	gate.memory = &fakeMemory{room: 3 << 20}
	var releases []func()
	for i := 0; i < 3; i++ {
		release, refusal := gate.acquire(context.Background(), time.Second)
		if release == nil {
			t.Fatalf("read %d refused: %s", i, refusal)
		}
		releases = append(releases, release)
	}
	if release, _ := gate.acquire(context.Background(), 50*time.Millisecond); release != nil {
		t.Fatal("a fourth read beyond the memory was admitted")
	}
	for _, release := range releases {
		release()
	}
}

// A pool admits at least one read, memory or not: with nothing in flight a
// read runs even when the line refuses its bytes, and with no line at all
// one read runs at a time.
func TestAPoolAlwaysAdmitsOneRead(t *testing.T) {
	refusing := newAdmission(1 << 20)
	refusing.memory = &fakeMemory{room: 0}
	release, refusal := refusing.acquire(context.Background(), time.Second)
	if release == nil {
		t.Fatalf("the only read was refused: %s", refusal)
	}
	release()
	alone := newAdmission(1 << 20)
	first, _ := alone.acquire(context.Background(), time.Second)
	if first == nil {
		t.Fatal("a pool without a line refused its first read")
	}
	if second, _ := alone.acquire(context.Background(), 30*time.Millisecond); second != nil {
		t.Fatal("a pool without a line admitted a second read at once")
	}
	first()
}

// A read that finds the pool full waits for a slot, up to the admission
// timeout, instead of being refused at once; one that waits it out is told
// how long it waited and what held the pool.
func TestAReadWaitsForASlotAndARefusalSaysWhy(t *testing.T) {
	gate := newAdmission(1 << 20)
	first, _ := gate.acquire(context.Background(), time.Second)
	done := make(chan func(), 1)
	go func() {
		release, _ := gate.acquire(context.Background(), time.Second)
		done <- release
	}()
	time.Sleep(50 * time.Millisecond)
	first()
	select {
	case release := <-done:
		if release == nil {
			t.Fatal("the waiting read was refused after the slot freed")
		}
		release()
	case <-time.After(time.Second):
		t.Fatal("the waiting read never ran")
	}
	held, _ := gate.acquire(context.Background(), time.Second)
	defer held()
	start := time.Now()
	release, refusal := gate.acquire(context.Background(), 80*time.Millisecond)
	if release != nil || time.Since(start) < 80*time.Millisecond {
		t.Fatal("a read was refused before its admission timeout")
	}
	if !regexp.MustCompile(`waited [0-9]+ms for a slot`).MatchString(refusal) || !strings.Contains(refusal, "1 read in flight") {
		t.Fatalf("refusal = %q, want the wait and the occupancy", refusal)
	}
}

// A caller that leaves while waiting stops waiting.
func TestAWaitEndsWithItsCaller(t *testing.T) {
	gate := newAdmission(1 << 20)
	held, _ := gate.acquire(context.Background(), time.Second)
	defer held()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	release, refusal := gate.acquire(ctx, 5*time.Second)
	if release != nil || time.Since(start) > time.Second {
		t.Fatal("a wait outlived its caller")
	}
	if !strings.Contains(refusal, "for a slot; 1 read in flight") {
		t.Fatalf("refusal = %q, want the wait and the occupancy", refusal)
	}
}

// Through the channel: with memory for two reads, two SRE reads run at once
// and a third waits for a slot and then runs, rather than being refused.
func TestTwoReadsRunAndAThirdWaitsThroughTheChannel(t *testing.T) {
	var mu sync.Mutex
	running, peak := 0, 0
	release := make(chan struct{})
	op := Operation{ID: "pod.logs", Summary: "Logs", ExecutionPool: "sre", ExecutionTimeout: 10 * time.Second, Run: func(context.Context, Params) Outcome {
		mu.Lock()
		running++
		peak = max(peak, running)
		mu.Unlock()
		<-release
		mu.Lock()
		running--
		mu.Unlock()
		return Outcome{Complete: true}
	}}
	c := testChannel(t, &testAuth{}, op)
	c.AttachReadMemory(&fakeMemory{room: 2 * MaxResponseBytes})
	statuses := make(chan int, 3)
	for i := 0; i < 3; i++ {
		go func() {
			status, _ := call(t, c, envelope(c, "invoke", "pod.logs", nil))
			statuses <- status
		}()
	}
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	together := running
	mu.Unlock()
	if together != 2 {
		t.Fatalf("%d reads running, want two while the third waits", together)
	}
	release <- struct{}{}
	time.Sleep(200 * time.Millisecond)
	close(release)
	for i := 0; i < 3; i++ {
		if status := <-statuses; status != 200 {
			t.Fatalf("a read answered %d, want every read run", status)
		}
	}
	if peak != 2 {
		t.Fatalf("peak %d reads at once, want the memory's two", peak)
	}
}

// Room on the line can come back without any read of this pool ending (a
// collection, another consumer, the other pool letting go): a waiting read
// rechecks while it waits and is admitted soon after, not at its timeout.
func TestAWaitingReadSeesRoomFreedElsewhere(t *testing.T) {
	memory := &fakeMemory{room: 2 << 20}
	gate := newAdmission(1 << 20)
	gate.memory = memory
	first, _ := gate.acquire(context.Background(), time.Second)
	defer first()
	memory.mu.Lock()
	memory.held += 1 << 20 // another consumer takes the rest of the room
	memory.mu.Unlock()
	go func() {
		time.Sleep(100 * time.Millisecond)
		memory.mu.Lock()
		memory.held -= 1 << 20 // and gives it back, with no read of this pool ending
		memory.mu.Unlock()
	}()
	start := time.Now()
	release, refusal := gate.acquire(context.Background(), 3*time.Second)
	if release == nil {
		t.Fatalf("the waiting read was refused: %s", refusal)
	}
	release()
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("admitted after %v, want soon after the room came back", waited)
	}
}
