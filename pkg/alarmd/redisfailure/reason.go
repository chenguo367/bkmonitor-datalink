// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package redisfailure names why a Redis call failed, in a closed set a
// counter can carry: a read that answers only "unavailable" cannot tell a
// connection the network cut while it sat idle from a server that refused
// the command, and those are fixed in different places.
package redisfailure

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/go-redis/redis/v8"
)

// Reasons, closed.
const (
	// ConnectionClosed is a connection the other end or the network closed:
	// EOF, reset, broken pipe. A pooled connection cut while idle fails so.
	ConnectionClosed = "connection_closed"
	// ConnectionRefused is no connection made: refused, or no route.
	ConnectionRefused = "connection_refused"
	// Timeout is a deadline reached: the call's or the socket's.
	Timeout = "timeout"
	// PoolTimeout is no connection free in the client's pool in time.
	PoolTimeout = "pool_timeout"
	// Canceled is the caller giving up before an answer.
	Canceled = "canceled"
	// ServerError is the server answering with an error reply.
	ServerError = "server_error"
	// MalformedReply is an answer that is not the shape the caller expects.
	MalformedReply = "malformed_reply"
	// Other is any failure none of the above names.
	Other = "other"
)

// Reasons is every reason, for counters created at startup.
var Reasons = []string{ConnectionClosed, ConnectionRefused, Timeout, PoolTimeout, Canceled, ServerError, MalformedReply, Other}

// Reason names why err failed; empty for no error.
func Reason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return Timeout
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed),
		errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNABORTED):
		return ConnectionClosed
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return ConnectionRefused
	case err.Error() == "redis: connection pool timeout":
		return PoolTimeout
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return Timeout
	}
	var dial *net.OpError
	if errors.As(err, &dial) && dial.Op == "dial" {
		return ConnectionRefused
	}
	var reply redis.Error
	if errors.As(err, &reply) && !strings.HasPrefix(err.Error(), "redis: ") {
		return ServerError
	}
	return Other
}
