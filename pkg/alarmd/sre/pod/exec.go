// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed remote.py
var remoteSupervisor string

// Exec runs one command behind the declared remote supervisor. Output is capped
// remotely and locally while control frames continue after output truncation.
// A canceled connection returns unknown; the supervisor retains its own timer.
func (p *Provider) Exec(ctx context.Context, request ExecRequest) (ExecReceipt, error) {
	result := ExecReceipt{Target: request.Target, StartedAt: p.options.Now().UTC(), RemoteState: RemoteNotStarted, EvidenceScope: "not_executed", RedactionContract: RedactionContract}
	id, err := randomID()
	if err != nil {
		return result, err
	}
	result.RequestID = id
	finish := func() { result.FinishedAt = p.options.Now().UTC() }
	release, err := p.acquire(ctx)
	if err != nil {
		finish()
		return result, err
	}
	defer release()
	if p.options.Executor == nil {
		finish()
		return result, &Error{Code: CodeTimeoutUnsupported, Message: "Kubernetes exec adapter is unavailable"}
	}
	if len(request.Argv) == 0 || len(request.Argv) > 128 || request.Argv[0] == "" || int64(len(request.Stdin)) > p.options.MaxStdinBytes {
		finish()
		return result, invalid("argv and stdin exceed the execution contract")
	}
	argvBytes := 0
	for _, arg := range request.Argv {
		argvBytes += len(arg)
		if strings.ContainsRune(arg, '\x00') {
			finish()
			return result, invalid("argv contains a NUL byte")
		}
	}
	if argvBytes > 32<<10 {
		finish()
		return result, invalid("argv byte budget exceeded")
	}
	limit, err := p.outputLimit(request.OutputBytes)
	if err != nil {
		finish()
		return result, err
	}
	timeout := p.options.DefaultTimeout
	if request.TimeoutMS != 0 {
		if request.TimeoutMS < 1 || request.TimeoutMS > p.options.MaxTimeout.Milliseconds() {
			finish()
			return result, invalid("execution deadline exceeds provider limit")
		}
		timeout = time.Duration(request.TimeoutMS) * time.Millisecond
	}
	encoded, _ := json.Marshal(struct {
		Argv  []string `json:"argv"`
		Stdin string   `json:"stdin"`
	}{request.Argv, request.Stdin})
	digest := sha256.Sum256(encoded)
	result.ScriptDigest = "sha256:" + hex.EncodeToString(digest[:])
	verifyCtx, verifyCancel := p.readContext(ctx)
	observation, scope, err := p.resolve(verifyCtx, request.Target, true)
	verifyCancel()
	if err != nil {
		var failure *Error
		if errors.As(err, &failure) && failure.Code == CodeTargetChanged {
			result.TargetChanged = true
			result.ObservedAfter = &observation
			result.EvidenceScope = "identity_changed_before_execution"
		}
		finish()
		return result, err
	}
	if observation.Phase != "Running" || !observation.Running || observation.Target.ContainerID == "" || observation.Target.ImageID == "" {
		finish()
		return result, &Error{Code: CodeTargetNotRunning, Message: "target container has no running identity"}
	}
	if scope.Timeout.Contract != TimeoutContract {
		finish()
		return result, &Error{Code: CodeTimeoutUnsupported, Message: "target scope has no declared Python process-group timeout contract"}
	}
	result.EvidenceScope = "identity_verified_before"
	nonce, err := randomID()
	if err != nil {
		finish()
		return result, err
	}
	argvJSON, _ := json.Marshal(request.Argv)
	remoteArgv := []string{scope.Timeout.PythonPath, "-u", "-c", remoteSupervisor, nonce, strconv.FormatFloat(timeout.Seconds(), 'f', 6, 64), strconv.FormatInt(limit, 10), string(argvJSON)}
	protocol := newProtocolWriter(nonce, limit)
	// Additional time permits TERM/KILL and the terminal receipt. The command
	// deadline itself is enforced inside the target, independently of this timer.
	execCtx, execCancel := context.WithTimeout(ctx, timeout+4*time.Second)
	transport, streamErr := p.options.Executor.Stream(execCtx, StreamRequest{Target: request.Target, Argv: remoteArgv, Stdin: strings.NewReader(request.Stdin), Stdout: protocol, Stderr: &discardBounded{limit: 4096}})
	execCancel()
	state := protocol.snapshot()
	var stdoutCut, stderrCut bool
	result.Stdout, stdoutCut = p.redactor.bounded(state.stdout, limit)
	remaining := limit - int64(len(result.Stdout))
	result.Stderr, stderrCut = p.redactor.bounded(state.stderr, remaining)
	result.Truncated = state.truncated || stdoutCut || stderrCut
	if streamErr != nil || state.invalid || !state.finished || transport.ExitCode == nil || *transport.ExitCode != 0 {
		result.RemoteState = RemoteUnknown
		if streamErr == nil && !state.started && !state.invalid && transport.ExitCode != nil {
			result.RemoteState = RemoteNotStarted
			result.EvidenceScope = "wrapper_failed_before_command"
			finish()
			return result, &Error{Code: CodeTimeoutUnsupported, Message: "declared supervisor could not start on the target"}
		}
	} else {
		switch state.event {
		case "unsupported":
			result.RemoteState = RemoteNotStarted
			finish()
			return result, &Error{Code: CodeTimeoutUnsupported, Message: "target runtime lacks the declared timeout capability"}
		case "launch_failed":
			result.RemoteState = RemoteNotStarted
			finish()
			return result, &Error{Code: CodeCommandStartFailed, Message: "target argv could not be started"}
		case "complete":
			result.RemoteState = RemoteCompleted
			if state.timedOut {
				result.RemoteState = RemoteTimedOut
			}
			result.ExitCode = state.exitCode
		default:
			result.RemoteState = RemoteUnknown
		}
	}
	result.ObservedAfter, result.TargetChanged, result.EvidenceScope = p.postObserve(ctx, request.Target)
	finish()
	if result.TargetChanged {
		return result, &Error{Code: CodeTargetChanged, Message: "target identity changed during execution"}
	}
	if result.RemoteState == RemoteUnknown {
		return result, &Error{Code: RemoteUnknown, Message: "remote termination is unconfirmed; inspect existing execution before explicit retry"}
	}
	return result, nil
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", &Error{Code: "request_id_unavailable", Message: "secure request identity is unavailable"}
	}
	return hex.EncodeToString(value[:]), nil
}

type wireFrame struct {
	Nonce     string `json:"nonce"`
	Event     string `json:"event"`
	Stream    string `json:"stream"`
	Data      string `json:"data"`
	ExitCode  *int   `json:"exit_code"`
	TimedOut  bool   `json:"timed_out"`
	Truncated bool   `json:"truncated"`
}
type protocolState struct {
	stdout, stderr                                  string
	started, finished, invalid, timedOut, truncated bool
	event                                           string
	exitCode                                        *int
}
type protocolWriter struct {
	mu             sync.Mutex
	nonce          string
	limit, kept    int64
	pending        []byte
	stdout, stderr bytes.Buffer
	state          protocolState
}

func newProtocolWriter(nonce string, limit int64) *protocolWriter {
	return &protocolWriter{nonce: nonce, limit: limit}
}
func (w *protocolWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	size := len(data)
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		take := len(data)
		if newline >= 0 {
			take = newline + 1
		}
		if len(w.pending)+take > 32<<10 {
			w.state.invalid = true
			return 0, fmt.Errorf("invalid supervisor frame")
		}
		w.pending = append(w.pending, data[:take]...)
		data = data[take:]
		if newline < 0 {
			break
		}
		if err := w.frame(w.pending); err != nil {
			w.state.invalid = true
			return 0, err
		}
		w.pending = w.pending[:0]
	}
	return size, nil
}
func (w *protocolWriter) frame(data []byte) error {
	var frame wireFrame
	if json.Unmarshal(data, &frame) != nil || frame.Nonce != w.nonce || w.state.finished {
		return fmt.Errorf("invalid supervisor control record")
	}
	switch frame.Event {
	case "started":
		if w.state.started {
			return fmt.Errorf("duplicate supervisor start")
		}
		w.state.started = true
	case "output":
		if !w.state.started {
			return fmt.Errorf("output preceded supervisor start")
		}
		data, err := base64.StdEncoding.DecodeString(frame.Data)
		if err != nil || len(data) > 16384 {
			return fmt.Errorf("invalid supervisor output")
		}
		remaining := w.limit - w.kept
		if int64(len(data)) > remaining {
			data = data[:remaining]
			w.state.truncated = true
		}
		w.kept += int64(len(data))
		switch frame.Stream {
		case "stdout":
			w.stdout.Write(data)
		case "stderr":
			w.stderr.Write(data)
		default:
			return fmt.Errorf("unknown supervisor stream")
		}
	case "unsupported", "launch_failed":
		if w.state.started {
			return fmt.Errorf("invalid supervisor failure order")
		}
		w.state.finished = true
		w.state.event = frame.Event
	case "complete":
		if !w.state.started || frame.ExitCode == nil {
			return fmt.Errorf("missing command completion identity")
		}
		w.state.finished = true
		w.state.event = frame.Event
		w.state.exitCode = frame.ExitCode
		w.state.timedOut = frame.TimedOut
		w.state.truncated = w.state.truncated || frame.Truncated
	case "unknown":
		if !w.state.started {
			return fmt.Errorf("unknown preceded start")
		}
		w.state.finished = true
		w.state.event = frame.Event
		w.state.truncated = w.state.truncated || frame.Truncated
	default:
		return fmt.Errorf("unknown supervisor record")
	}
	return nil
}
func (w *protocolWriter) snapshot() protocolState {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.state
	state.stdout = w.stdout.String()
	state.stderr = w.stderr.String()
	if len(w.pending) > 0 {
		state.invalid = true
	}
	return state
}

// Interpreter errors are not trusted evidence and are never exposed verbatim.
// Consume a bounded amount without retaining secret-bearing exception text.
type discardBounded struct{ limit int64 }

func (w *discardBounded) Write(data []byte) (int, error) {
	w.limit -= int64(len(data))
	if w.limit < 0 {
		return 0, fmt.Errorf("supervisor bootstrap output exceeded limit")
	}
	return len(data), nil
}
