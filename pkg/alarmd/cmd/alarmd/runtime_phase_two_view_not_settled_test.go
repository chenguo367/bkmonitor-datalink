// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A round a case asks for on a view that never settles - the Worker's control
// stream withheld, so the view has nothing to install from - fails saying the
// view did not settle, how long it was waited for and what it lacked, and
// does not run; by hand through the runner a fixture holds and by the
// scheduled pass alike. Before, each waited out its bound and ran the round
// on the unsettled view, and a case reading it failed, if at all, on an
// assertion about something else: whether the view had settled was left to
// be inferred from how long the case took.
//
// Each wait is said slow on the log past settleBound and fails at its
// deadline - here a bound and a few seconds on, not the binary's timeout -
// with what the view lacked and the view stream's state: the control stream
// withheld.
func TestARoundOnAViewThatNeverSettlesFailsSayingSoAndDoesNotRun(t *testing.T) {
	fixture := startRoundsFixture(t, roundsOptions{hosts: true})
	withholdControlStreamForTest(fixture.bundle)
	shortly := func() settleT { return settleBy(t, time.Now().Add(settleBound+settleMargin+2*time.Second)) }
	fixture.runner = settledRunner(shortly(), fixture.bundle, fixture.queryGroup)
	unsettled := func(how string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s on a view that never settles: no error, want the view not settled said", how)
		}
		for _, said := range []string{"view not settled", "waited", string(fixture.queryGroup), "control stream withheld"} {
			if !strings.Contains(err.Error(), said) {
				t.Fatalf("%s: %q does not say %q", how, err, said)
			}
		}
	}
	ran := func(mark int) bool {
		for _, observation := range fixture.since(mark) {
			if observation.Stage == observability.StageSlotStarted || observation.Stage == observability.StageSlotCompleted {
				return true
			}
		}
		return false
	}

	mark := fixture.mark()
	fixture.clock.Store(roundReadAtMillis(fixture.bundle.dependencies.Config, fixture.queryGroup, fixture.evaluationAt(1), fixture.interval))
	_, attempted, err := fixture.runner.RunOne(context.Background())
	unsettled("the runner's round", err)
	if attempted || ran(mark) {
		t.Fatalf("the runner's round ran on the unsettled view (attempted %t)", attempted)
	}

	mark = fixture.mark()
	unsettled("the scheduled pass", runScheduledOnceSettled(shortly(), context.Background(), fixture.bundle))
	if ran(mark) {
		t.Fatal("the scheduled pass ran a round on the unsettled view")
	}
}

// loggedSettle is t with what a settling wait says on its log kept.
type loggedSettle struct {
	*testing.T
	mu   sync.Mutex
	said []string
}

func (t *loggedSettle) Logf(format string, args ...any) {
	t.T.Helper()
	t.mu.Lock()
	t.said = append(t.said, fmt.Sprintf(format, args...))
	t.mu.Unlock()
	t.T.Logf(format, args...)
}

func (t *loggedSettle) log() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.said, "\n")
}

// A view that settles only after its bound - the Worker's control stream
// withheld past it, then served again - is waited for on to the test's
// deadline and the round goes on: no error, and the log says the wait was
// slow, what the view lacked, the stream's state and how long, and when it
// settled. A load beside the binary settles it late in the same way, and
// the log is what tells it apart from a stream that stood still.
func TestAViewSettlingPastItsBoundIsWaitedForAndSaidSlow(t *testing.T) {
	fixture := startRoundsFixture(t, roundsOptions{hosts: true})
	withholdControlStreamForTest(fixture.bundle)
	const bound = 300 * time.Millisecond
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		time.Sleep(4 * bound) // fixed-wait: the stream comes back well past the bound, which is what the case is about
		restoreControlStreamForTest(fixture.bundle)
	}()
	logged := &loggedSettle{T: t}
	if err := settleExecutableView(logged, context.Background(), fixture.bundle, bound); err != nil {
		t.Fatalf("a view settling past its bound: %v, want it waited for", err)
	}
	<-restored
	said := logged.log()
	for _, want := range []string{"view not settled within 300ms", "waited", string(fixture.queryGroup),
		"control stream withheld", "view settled after"} {
		if !strings.Contains(said, want) {
			t.Fatalf("the log %q does not say %q", said, want)
		}
	}
}
