// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// handoffClock is a shared answer's decode clock, still but for what the case
// moves it by, which knows from its caller which side of the frame handoff
// has begun to wait and not yet ended: the reader in its send
// (readSharedFrames' send closure begins the wait, the func it defers ends
// it), the Slot in next (decodeShared's closure, each wait one call to begin
// and one to end). A case moves it only while the side it times is in such a
// wait, so what each side counts is exactly what the case moved it by then,
// however the goroutines are scheduled.
type handoffClock struct {
	mu                       sync.Mutex
	at                       time.Time
	readerBegun, readerEnded int
	slotCalls                int
	// unknown is every caller in readSharedFrames or decodeShared the clock
	// does not know - none but the version frame's time, in readSharedFrames
	// itself, is expected - named so a change of their shape says so.
	unknown []string
	// slotWaiting is sent to, without blocking, when the Slot begins a wait.
	slotWaiting chan struct{}
}

func newHandoffClock() *handoffClock {
	return &handoffClock{at: time.Unix(1_788_000_000, 0), slotWaiting: make(chan struct{}, 1)}
}

func (clock *handoffClock) now() time.Time {
	caller := ""
	pcs := make([]uintptr, 8)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.Function, "handoffClock") {
			caller = frame.Function
			break
		}
		if !more {
			break
		}
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	switch {
	case strings.HasSuffix(caller, "readSharedFrames.func1"):
		clock.readerBegun++
	case strings.HasSuffix(caller, "readSharedFrames.func1.1"):
		clock.readerEnded++
	case strings.Contains(caller, "decodeShared.func"):
		clock.slotCalls++
		if clock.slotCalls%2 == 1 {
			select {
			case clock.slotWaiting <- struct{}{}:
			default:
			}
		}
	case strings.Contains(caller, "readSharedFrames") && !strings.HasSuffix(caller, ".readSharedFrames"),
		strings.Contains(caller, "decodeShared"):
		clock.unknown = append(clock.unknown, caller)
	}
	return clock.at
}

// settled fails t unless the decode's clock was asked only by the callers it
// knows, and every wait it saw begin ended: an answer decoded to its end.
func (clock *handoffClock) settled(t *testing.T) {
	t.Helper()
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if len(clock.unknown) != 0 || clock.readerBegun != clock.readerEnded || clock.slotCalls%2 != 0 {
		t.Fatalf("the decode's clock was asked by %v, which the case does not know; the reader began %d waits and ended %d, "+
			"the Slot asked it %d times (want an even count)", clock.unknown, clock.readerBegun, clock.readerEnded, clock.slotCalls)
	}
}

func (clock *handoffClock) readerWaiting() bool {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.readerBegun > clock.readerEnded
}

func (clock *handoffClock) advance(by time.Duration) {
	clock.mu.Lock()
	clock.at = clock.at.Add(by)
	clock.mu.Unlock()
}

// holdingSink, for its first holds batches, holds each until the reader is
// waiting to hand on a frame and moves the clock by step before taking the
// next: the Slot slow to take a frame, the reader's wait the case counts.
type holdingSink struct {
	t     *testing.T
	clock *handoffClock
	step  time.Duration
	holds int
	taken int
}

func (sink *holdingSink) ConsumeProviderSeries(context.Context, execution.ProviderSeriesBatch) error {
	sink.taken++
	if sink.taken <= sink.holds {
		testwait.Until(sink.t, "the reader waiting to hand on a frame", sink.clock.readerWaiting)
		sink.clock.advance(sink.step)
	}
	return nil
}

// pacedAnswer serves a shared answer one line at a time: its headers at once,
// then each line the reader hands the Slot a frame for only once the Slot
// waits for it, the clock moved by step first - an answer whose frames come
// apart, the Slot's wait the case counts - and a schema frame's, which the
// reader keeps, at once. A Slot that never waits ends the answer short,
// which fails its decode.
func pacedAnswer(lines []string, clock *handoffClock, step time.Duration, expired <-chan time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", SharedSchemaMediaType)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for _, line := range lines {
			if !strings.Contains(line, `"schemas"`) {
				select {
				case <-clock.slotWaiting:
				case <-expired:
					return
				}
				clock.advance(step)
			}
			_, _ = io.WriteString(w, line)
			w.(http.Flusher).Flush()
		}
	}
}

// Each side of a shared-schema answer's frames counts what it waited for the
// other, on the answer's completion, and nothing for what it did not: a Slot
// slow to hand series on - holding each of its first three frames until the
// reader waits to hand on the next, 30 ms by the decode's clock - leaves the
// reader waiting 90 ms and itself waiting none; an answer whose eight frames
// for the Slot (the version, six series frames, the footer; each series
// frame after its schema frame, which the reader keeps) each come only once
// the Slot waits, 30 ms after, leaves the Slot waiting 240 ms and the reader
// none, on the completion Execute returns. A legacy answer counts neither,
// however slow either side is. The clock is asked only by the callers it
// knows, which it says by name otherwise.
func TestEachSideOfASharedAnswersFramesCountsItsWaitForTheOther(t *testing.T) {
	const step = 30 * time.Millisecond
	series := make([]responseSeries, 6)
	for index := range series {
		series[index] = sharedTestSeries()
		series[index].GroupValues = []json.RawMessage{json.RawMessage(fmt.Sprintf(`"10.0.0.%d"`, index))}
	}
	body := sharedTestBody(t, series, map[string]any{"is_partial": false})
	identity := queryIdentity{Spec: validAttempt(t).Spec, AttemptNo: 1}
	decode := func(clock *handoffClock, reader io.Reader, sink execution.ProviderSeriesSink) (time.Duration, time.Duration) {
		t.Helper()
		client := fixtureClient(t, http.StatusOK, "", DefaultLimits()).WithNow(clock.now)
		done, err := client.decodeShared(context.Background(), nil, reader, identity, sink)
		if err != nil || done.Completeness != execution.CompletenessFull {
			t.Fatalf("decode = %+v, %v", done, err)
		}
		return time.Duration(done.Stats.SharedFrameWaitConsumerMicros) * time.Microsecond,
			time.Duration(done.Stats.SharedFrameWaitReaderMicros) * time.Microsecond
	}

	clock := newHandoffClock()
	consumer, reader := decode(clock, strings.NewReader(body), &holdingSink{t: t, clock: clock, step: step, holds: 3})
	clock.settled(t)
	if consumer != 3*step || reader != 0 {
		t.Fatalf("a slow Slot: the reader waited %v on it and it %v on the reader, want 90ms and none", consumer, reader)
	}

	// The slow answer through Execute: what decoding counted is what the
	// completion it returns says.
	clock = newHandoffClock()
	lines := strings.SplitAfter(body, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	paced := httptest.NewServer(pacedAnswer(lines, clock, step, testwait.Expired(t)))
	defer paced.Close()
	client, err := NewClientWithLimits(paced.URL, "alarmd", paced.Client(), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	done, err := client.WithNow(clock.now).Execute(context.Background(), validAttempt(t), &collectingSink{})
	if err != nil || done.Completeness != execution.CompletenessFull {
		t.Fatalf("Execute = %+v, %v", done, err)
	}
	clock.settled(t)
	consumer = time.Duration(done.Stats.SharedFrameWaitConsumerMicros) * time.Microsecond
	reader = time.Duration(done.Stats.SharedFrameWaitReaderMicros) * time.Microsecond
	if len(lines) != 14 || reader != 8*step || consumer != 0 {
		t.Fatalf("a slow answer of %d lines: the Slot waited %v on the reader and the reader %v on it, want 240ms and none",
			len(lines), reader, consumer)
	}

	// A legacy answer, through Execute: its lines 30 ms apart and its Slot
	// 30 ms a batch, and no frame wait counted.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		time.Sleep(step) // fixed-wait: the answer coming late, which a legacy answer does not count
		_, _ = io.WriteString(w, `{"series":[`+identityTestSeriesJSON()+`],"is_partial":false}`)
	}))
	defer server.Close()
	client, err = NewClientWithLimits(server.URL, "alarmd", server.Client(), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	done, err = client.Execute(context.Background(), validAttempt(t), pausingSink{pause: step})
	if err != nil {
		t.Fatal(err)
	}
	if done.Stats.SharedFrameWaitConsumerMicros != 0 || done.Stats.SharedFrameWaitReaderMicros != 0 {
		t.Fatalf("a legacy answer counted %d and %d µs of frame waits", done.Stats.SharedFrameWaitConsumerMicros,
			done.Stats.SharedFrameWaitReaderMicros)
	}
}

// pausingSink takes each batch pause after it is handed it.
type pausingSink struct{ pause time.Duration }

func (sink pausingSink) ConsumeProviderSeries(context.Context, execution.ProviderSeriesBatch) error {
	time.Sleep(sink.pause) // fixed-wait: the Slot slow to take a batch, which a legacy answer does not count
	return nil
}
