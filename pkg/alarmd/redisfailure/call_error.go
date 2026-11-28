// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package redisfailure

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CallError is a Redis call that failed because the store did not answer:
// the connection closed or was refused, no Sentinel answered, the pool had no
// connection, or the call ran out of time after it was issued with time left.
// The call hook puts it on the call's error, so whatever is made of that error
// further up can still say the store failed and for which job.
//
// A call the store answered with an error reply, an empty result, a call its
// caller cancelled, and one issued with its deadline already past are not
// CallErrors: the store said something, or the caller's clock ran out before
// the call left.
//
// Its text is the wrapped error's, so a line that printed the error before
// prints the same words; errors.Is and errors.As reach the wrapped error.
type CallError struct {
	// Client is the bounded name of the client that made the call.
	Client string
	// Caller is the job the call was made for (Callers), "" when its context
	// named none.
	Caller string
	// Reason is why it failed (StoreUnavailable).
	Reason string
	// Waited is how long the call ran, from issue to failure.
	Waited time.Duration
	// Left is how much of its caller's deadline the call had when it was
	// issued, when LeftKnown. A Waited close to Left is a call that used the
	// time its caller gave it; a Left much shorter than the caller's usual
	// budget is a call the steps before it had already spent most of.
	Left time.Duration
	// LeftKnown is whether the caller's context had a deadline at all.
	LeftKnown bool
	// Err is the call's own error.
	Err error
}

func (e *CallError) Error() string { return e.Err.Error() }

func (e *CallError) Unwrap() error { return e.Err }

// RanOutItsTime says the call was issued with time left and waited all of
// it: its caller's deadline ran out while it waited on the store.
func (e *CallError) RanOutItsTime() bool {
	return e != nil && e.LeftKnown && e.Left > 0 && e.Waited >= e.Left
}

// Witness keeps, for one attempt of work, a call the attempt made that the
// store did not answer and that ran out the time it was issued with
// (RanOutItsTime), for whoever reads the attempt's outcome when its error no
// longer carries the call: a store that turned the failure into a status,
// and a caller that then stopped on its own deadline.
//
// Of the calls of the jobs keep accepts, it keeps the one that waited
// longest, the first noted among equal waits. The attempt's goroutines may
// note into it at once; a call that failed with time left, a call of another
// job, and a call that waited less do not displace the one kept, so what it
// keeps does not depend on which goroutine ran last. The longest is kept
// rather than the first because a call that ran out a short time of its own
// early in the attempt says less of where the attempt's time went than one
// that waited out most of it later.
type Witness struct {
	keep func(caller string) bool
	mu   sync.Mutex
	call *CallError
}

type witnessKey struct{}

// WithWitness is ctx with a new Witness for the calls made under it, keeping
// those of the jobs keep accepts (every job when keep is nil). A Witness
// belongs to the attempt that made it: each attempt makes its own.
func WithWitness(ctx context.Context, keep func(caller string) bool) (context.Context, *Witness) {
	witness := &Witness{keep: keep}
	return context.WithValue(ctx, witnessKey{}, witness), witness
}

// NoteWitnessed hands call to the Witness ctx carries, if it carries one.
func NoteWitnessed(ctx context.Context, call *CallError) {
	witness, _ := ctx.Value(witnessKey{}).(*Witness)
	if witness == nil || !call.RanOutItsTime() || (witness.keep != nil && !witness.keep(call.Caller)) {
		return
	}
	witness.mu.Lock()
	defer witness.mu.Unlock()
	if witness.call == nil || call.Waited > witness.call.Waited {
		witness.call = call
	}
}

// Call is the call the Witness kept, nil when no call of its attempt ran out
// its time on the store.
func (w *Witness) Call() *CallError {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.call
}

// storeUnavailable are the reasons that say the store did not answer.
var storeUnavailable = map[string]struct{}{
	ConnectionClosed: {}, ConnectionRefused: {}, SentinelUnreachable: {}, Timeout: {}, PoolTimeout: {},
}

// StoreUnavailable says reason is the store not answering, as opposed to the
// store answering with an error (ServerError), an answer of the wrong shape
// (MalformedReply), the caller giving up (Canceled) or a failure nothing names
// (Other).
func StoreUnavailable(reason string) bool {
	_, unavailable := storeUnavailable[reason]
	return unavailable
}

// CallErrorOf is the CallError in err's chain, nil when there is none.
func CallErrorOf(err error) *CallError {
	var call *CallError
	if errors.As(err, &call) {
		return call
	}
	return nil
}
