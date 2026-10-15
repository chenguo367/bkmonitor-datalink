// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReadMemory is the process's observation memory line as a read sees it: a
// read holds the bytes it may take while it runs and gives them back after.
type ReadMemory interface {
	Hold(bytes uint64) (release func(), held bool)
}

// admission is one execution pool's gate. How many reads run at once is not
// a count: each read holds the bytes it may take on the observation memory
// line, and a read is admitted while that fits. With nothing in flight a
// read is admitted whatever the line says, since a pool must admit
// something; without a line, that is the only read admitted. A read that
// finds the pool full waits for a slot, up to its admission timeout, rather
// than being refused at once.
type admission struct {
	bytes uint64

	mu       sync.Mutex
	memory   ReadMemory
	inFlight int
	changed  chan struct{}
}

// admissionRecheck is how often a waiting read asks the line again: room
// can come back with no read of this pool ending (a collection, another
// consumer, the other pool letting go), and nothing signals that.
const admissionRecheck = 150 * time.Millisecond

func newAdmission(bytes uint64) *admission {
	return &admission{bytes: bytes, changed: make(chan struct{})}
}

// attach gives the gate the memory line, once the process has one.
func (a *admission) attach(memory ReadMemory) {
	a.mu.Lock()
	a.memory = memory
	a.mu.Unlock()
}

func (a *admission) attached() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.memory != nil
}

// acquire admits a read or, after waiting up to timeout (or until ctx ends),
// says why not: how long it waited and what held the pool.
func (a *admission) acquire(ctx context.Context, timeout time.Duration) (func(), string) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	recheck := time.NewTicker(admissionRecheck)
	defer recheck.Stop()
	for {
		a.mu.Lock()
		release, admitted := a.tryLocked()
		changed, inFlight := a.changed, a.inFlight
		a.mu.Unlock()
		if admitted {
			return release, ""
		}
		select {
		case <-changed:
		case <-recheck.C:
		case <-timer.C:
			return nil, a.refusal(time.Since(start), inFlight)
		case <-ctx.Done():
			return nil, a.refusal(time.Since(start), inFlight)
		}
	}
}

func (a *admission) tryLocked() (func(), bool) {
	var giveBack func()
	switch {
	case a.inFlight == 0:
		if a.memory != nil {
			giveBack, _ = a.memory.Hold(a.bytes)
		}
	case a.memory == nil:
		return nil, false
	default:
		var held bool
		if giveBack, held = a.memory.Hold(a.bytes); !held {
			return nil, false
		}
	}
	a.inFlight++
	var once sync.Once
	return func() {
		once.Do(func() {
			if giveBack != nil {
				giveBack()
			}
			a.mu.Lock()
			a.inFlight--
			close(a.changed)
			a.changed = make(chan struct{})
			a.mu.Unlock()
		})
	}, true
}

func (a *admission) refusal(waited time.Duration, inFlight int) string {
	reads := "reads"
	if inFlight == 1 {
		reads = "read"
	}
	return fmt.Sprintf("waited %s for a slot; %d %s in flight, each holding up to %d bytes of the observation memory line",
		waited.Round(10*time.Millisecond), inFlight, reads, a.bytes)
}
