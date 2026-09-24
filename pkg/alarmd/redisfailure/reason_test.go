// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package redisfailure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/go-redis/redis/v8"
)

type serverReply string

func (reply serverReply) Error() string { return string(reply) }
func (serverReply) RedisError()         {}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// Each way a call fails is named apart, the way it reaches the caller:
// wrapped by the network layer and by go-redis.
func TestEachFailureIsNamedApart(t *testing.T) {
	reset := &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
	for err, want := range map[error]string{
		nil:                            "",
		io.EOF:                         ConnectionClosed,
		fmt.Errorf("read: %w", io.EOF): ConnectionClosed,
		reset:                          ConnectionClosed,
		&net.OpError{Op: "write", Net: "tcp", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}: ConnectionClosed,
		refused: ConnectionRefused,
		&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("no such host")}: ConnectionRefused,
		context.DeadlineExceeded:                                  Timeout,
		&net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}}: Timeout,
		errors.New("redis: connection pool timeout"):              PoolTimeout,
		context.Canceled:                                          Canceled,
		serverReply("ERR unknown command"):                        ServerError,
		redis.Nil:                                                 Other,
		errors.New("something else"):                              Other,
	} {
		if got := Reason(err); got != want {
			t.Errorf("Reason(%v) = %q, want %q", err, got, want)
		}
	}
	for _, reason := range Reasons {
		if reason == "" {
			t.Fatal("an empty reason in the closed set")
		}
	}
}
