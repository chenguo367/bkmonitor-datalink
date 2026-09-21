// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

// A read that ran out of time is not the dependency being unavailable.
//
// The classifier used to have two buckets - an identity error, and "anything
// else" named after Redis - so a read of ours that did not fit its own timeout
// was reported as a Redis outage. On the cluster that produced this decision
// the store was answering 2,400 other objects on the same replica that second:
// every reader who followed the name went to a dependency that was fine, and
// the row said nothing about the size of the read that actually failed.
func TestAReadThatRanOutOfTimeIsNotTheDependencyBeingDown(t *testing.T) {
	for name, err := range map[string]error{
		"the client's own read timeout": timeoutError{},
		"a deadline on the call":        context.DeadlineExceeded,
		"a deadline on the socket":      os.ErrDeadlineExceeded,
		"wrapped":                       errors.Join(errors.New("state: Redis MGET"), timeoutError{}),
	} {
		view := runtimeLoadFailure(execution.RuntimeStateView{}, err)
		if view.Status != execution.StateRetryableIO {
			t.Fatalf("%s: status = %q, want a retryable read", name, view.Status)
		}
		if view.ReasonCode != execution.ReasonCode(contract.ReasonStateReadTimeout) {
			t.Fatalf("%s: reason = %q, want %q; named after the dependency it sends the reader to a store "+
				"that was answering everyone else", name, view.ReasonCode, contract.ReasonStateReadTimeout)
		}
	}

	// A dial that timed out is the dependency not being reachable - a black
	// hole, a partition, a server that is gone - and it is the one timeout that
	// really is the dependency's. Named as ours it lands in the fleet view as
	// this deployment's doing and points the page at a read size that had
	// nothing to do with it: the same misattribution, aimed the other way.
	dial := runtimeLoadFailure(execution.RuntimeStateView{}, &net.OpError{
		Op: "dial", Net: "tcp", Err: timeoutError{},
	})
	if dial.ReasonCode != execution.ReasonCode(contract.ReasonRedisUnavailable) {
		t.Fatalf("a dial timeout = %q, want %q: nothing was read, so no read was too large",
			dial.ReasonCode, contract.ReasonRedisUnavailable)
	}
	// A read on an established connection still is ours, so the check above is
	// about dialling rather than about net.OpError.
	read := runtimeLoadFailure(execution.RuntimeStateView{}, &net.OpError{
		Op: "read", Net: "tcp", Err: timeoutError{},
	})
	if read.ReasonCode != execution.ReasonCode(contract.ReasonStateReadTimeout) {
		t.Fatalf("a read timeout = %q, want %q", read.ReasonCode, contract.ReasonStateReadTimeout)
	}

	// The other branch still exists, or the case above is satisfied by naming
	// everything a timeout.
	refused := runtimeLoadFailure(execution.RuntimeStateView{}, errors.New("connection refused"))
	if refused.ReasonCode != execution.ReasonCode(contract.ReasonRedisUnavailable) {
		t.Fatalf("a dependency that refused the connection = %q, want %q",
			refused.ReasonCode, contract.ReasonRedisUnavailable)
	}
	corrupt := runtimeLoadFailure(execution.RuntimeStateView{}, &IdentityError{})
	if corrupt.Status != execution.StateDeterministicInvalid {
		t.Fatalf("an identity error = %q, want it still deterministic-invalid", corrupt.Status)
	}
}
