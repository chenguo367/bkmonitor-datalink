// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// runScheduledOnceSettled runs one scheduler pass once the Worker's
// executable view has caught up with its records: for every owned Query
// Group the installed view carries the entry with content and a timeline
// revision, the Assignment record says the same revision, and the session's
// lease has brought it - renewing the lease when it has not, since a
// fixture's renewal interval is a minute and the accepted lag is one renewal.
//
// This is the wait a production Worker pays after a rollout or a cutover
// (decision-016 batch 4b, the accepted bound: renewal interval plus delta
// propagation); a fixture that ran the pass at once ran it inside that
// window and got view_not_executable, which is the answer it would get, not
// the one the case is about. Bounded: a view that has not settled by the
// deadline is the pass's error (viewNotSettledError), and the pass does not
// run - run on, it would answer on a lease behind its record, and the case
// would fail, if at all, on an assertion about something else.
func runScheduledOnceSettled(ctx context.Context, bundle *phaseTwoWorkerBundle) error {
	if err := settleExecutableView(ctx, bundle, 10*time.Second); err != nil {
		return err
	}
	return bundle.runScheduledOnce(ctx)
}

// runScheduledOnceBestEffort is runScheduledOnceSettled for a fleet with no
// steady Control Leader to settle the view from - the Control process gone,
// flapping or in a crash loop, as the election cases put it - whose Workers'
// passes go on past the bound on an unsettled view, as their own schedulers
// do (settleExecutableViewBestEffort).
func runScheduledOnceBestEffort(ctx context.Context, bundle *phaseTwoWorkerBundle) error {
	settleExecutableViewBestEffort(ctx, bundle, 10*time.Second)
	return bundle.runScheduledOnce(ctx)
}

// controlStreamServers is the control stream listener each bundle under test
// got, keyed by bundle: a fixture that only Start()s the bundle serves no
// listener, and the Worker's own view client has nothing to install from
// until one is served on the address its registration advertises. The
// helper has no test to hang a cleanup on, so each listener lives as long
// as its bundle: it is closed, and its entry let go, once the bundle has
// shut down (releaseWhenClosed). Kept for the test process instead, every
// bundle a case ever settled stayed reachable from here, its server and all
// it held - seven gigabytes of a whole-package run. A case about a Worker whose
// Leader is unreachable withholds it (withholdControlStreamForTest), and
// the settling helpers leave it withheld until the case gives it back.
var controlStreamServers sync.Map

type testControlStream struct {
	server   *http.Server
	listener *trackedListener
	withheld bool
}

func (stream *testControlStream) stop() {
	if stream.server != nil {
		_ = stream.server.Close()
		stream.listener.closeAll()
	}
}

func reserveBundleAddress() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "127.0.0.1:1"
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func serveControlStreamForTest(bundle *phaseTwoWorkerBundle) {
	if bundle.dependencies.ControlStream == nil {
		return
	}
	if _, served := controlStreamServers.Load(bundle); served {
		return
	}
	// Connections are tracked so a case can cut them: h2c hijacks the
	// connection, and http.Server.Close does not reach a hijacked one.
	listener, err := listenTracked(bundle.dependencies.Config.HTTP.Listen)
	if err != nil {
		return
	}
	handler := h2c.NewHandler(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.ProtoMajor == 2 && strings.HasPrefix(request.Header.Get("Content-Type"), "application/grpc") {
			bundle.dependencies.ControlStream.ServeHTTP(response, request)
			return
		}
		http.NotFound(response, request)
	}), &http2.Server{})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	served := &testControlStream{server: server, listener: listener}
	if _, raced := controlStreamServers.LoadOrStore(bundle, served); raced {
		_ = listener.Close()
		return
	}
	go func() { _ = server.Serve(listener) }()
	go releaseWhenClosed(bundle, served)
}

// releaseWhenClosed closes the bundle's test listener and lets its entry go
// once the bundle has shut down, unless another has taken its place (a case
// withheld or restored it). It holds the bundle only until then.
func releaseWhenClosed(bundle *phaseTwoWorkerBundle, served *testControlStream) {
	ticker := time.NewTicker(200 * time.Millisecond) // fixed-wait: the poll interval of a condition wait, bounded by the bundle's shutdown
	defer ticker.Stop()
	for range ticker.C {
		if current, ok := controlStreamServers.Load(bundle); !ok || current != any(served) {
			return
		}
		bundle.mu.RLock()
		closed := bundle.closed
		bundle.mu.RUnlock()
		if closed {
			if controlStreamServers.CompareAndDelete(bundle, served) {
				served.stop()
			}
			return
		}
	}
}

// withholdControlStreamForTest takes the bundle's control stream away --
// listener and every connection on it -- and keeps the settling helpers
// from standing it up again until restoreControlStreamForTest.
func withholdControlStreamForTest(bundle *phaseTwoWorkerBundle) {
	if previous, served := controlStreamServers.Swap(bundle, &testControlStream{withheld: true}); served {
		previous.(*testControlStream).stop()
	}
}

func restoreControlStreamForTest(bundle *phaseTwoWorkerBundle) {
	controlStreamServers.Delete(bundle)
	serveControlStreamForTest(bundle)
}

// viewNotSettledError is a view that did not settle (viewSettled) within the
// bound a round waits for it: how long it was waited for, and what it still
// lacked when the wait ended. A round run past it runs on a lease behind its
// record, so a case reading it would fail, if at all, on an assertion about
// something else; this says which it was.
type viewNotSettledError struct {
	waited, bound time.Duration
	lacked        string
}

func (err *viewNotSettledError) Error() string {
	return fmt.Sprintf("view not settled: waited %s (bound %s), %s", err.waited.Round(time.Millisecond), err.bound, err.lacked)
}

// settleExecutableView waits, up to wait, for every Query Group of the bundle
// to be executable from the view with its lease at the record's timeline
// revision (viewSettled), and is a viewNotSettledError when that has not come
// by then. A case whose view cannot settle by design - its fleet with no
// steady Control Leader - says so with settleExecutableViewBestEffort; a case
// that needs the settled view for longer than the bound waits for it with
// requireSettledView, to the test's deadline.
func settleExecutableView(ctx context.Context, bundle *phaseTwoWorkerBundle, wait time.Duration) error {
	client := bundle.dependencies.ViewClient
	ownership, ok := bundle.dependencies.Ownership.(*productionPhaseTwoOwnership)
	if client == nil || !ok {
		return nil
	}
	serveControlStreamForTest(bundle)
	started := time.Now()
	deadline := started.Add(wait)
	for { // fixed-wait: how long a round waits for the view to settle before it fails saying so
		renewLapsedRegistrationForTest(ctx, bundle, ownership)
		lacked := viewLacks(ctx, bundle, ownership)
		if lacked == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return &viewNotSettledError{waited: time.Since(started), bound: wait, lacked: lacked}
		}
		time.Sleep(10 * time.Millisecond) // fixed-wait: the poll interval of a bounded settle; requireSettledView waits on the condition
	}
}

// settleExecutableViewBestEffort is settleExecutableView for a case whose view
// cannot settle by design: a fleet with no steady Control Leader to settle it
// from - the Control process gone, flapping or in a crash loop, the election
// cases - or a clock stepped back before a lease's last confirmation, which
// its session does not take again until the clock passes it
// (ownership/session.go:179; closePathsFixture.publish). A Worker's pass goes
// on past the bound on an unsettled view, as its own scheduler does. It says
// whether the view settled. Every other case fails on a view that does not
// settle.
func settleExecutableViewBestEffort(ctx context.Context, bundle *phaseTwoWorkerBundle, wait time.Duration) bool {
	return settleExecutableView(ctx, bundle, wait) == nil
}

// requireSettledView waits, until the test's deadline, for every Query Group
// of the bundle to be executable from the view with its lease at the
// record's timeline revision (viewSettled), and fails t if that never comes.
// A case asserting what a settled round reads past settleExecutableView's
// bound waits here: run before the view settles, the round runs on a lease
// behind the record, and its stale hint is answered through the activation
// header.
func requireSettledView(t *testing.T, ctx context.Context, bundle *phaseTwoWorkerBundle) {
	t.Helper()
	production, ok := bundle.dependencies.Ownership.(*productionPhaseTwoOwnership)
	if bundle.dependencies.ViewClient == nil || !ok {
		return
	}
	serveControlStreamForTest(bundle)
	testwait.Until(t, "the view settling with every lease at its record's timeline revision", func() bool {
		renewLapsedRegistrationForTest(ctx, bundle, production)
		return viewSettled(ctx, bundle, production)
	})
}

// renewLapsedRegistrationForTest re-registers the Worker when its own
// registration has lapsed by the bundle's clock. A fixture moves that clock
// by half an hour in one store; the renewal loop runs on the wall clock and
// takes an interval to notice, while the stream's admission reads the
// registration by the moved clock and refuses the Worker's own session as
// unknown until then. A production clock never moves past the registration
// TTL between two renewals; this is the renewal the loop would have run.
func renewLapsedRegistrationForTest(ctx context.Context, bundle *phaseTwoWorkerBundle, production *productionPhaseTwoOwnership) {
	registry, ok := production.dependencies.Store.(viewStreamRegistry)
	if !ok {
		return
	}
	registration, found, err := registry.ReadWorker(ctx, bundle.dependencies.Config.PhaseTwo.Worker.ID)
	if err != nil || (found && registration.ExpiresAt.After(bundle.dependencies.Now())) {
		return
	}
	_ = bundle.register(ctx, ownership.WorkerReady)
}

func viewSettled(ctx context.Context, bundle *phaseTwoWorkerBundle, ownership *productionPhaseTwoOwnership) bool {
	return viewLacks(ctx, bundle, ownership) == ""
}

// viewLacks is what keeps the view from settling - the first Query Group the
// view or its lease is not yet executable for, and why - or nothing when it
// has settled.
func viewLacks(ctx context.Context, bundle *phaseTwoWorkerBundle, ownership *productionPhaseTwoOwnership) string {
	bundle.mu.RLock()
	groups := append([]execution.QueryGroupIdentity(nil), bundle.queryGroups...)
	runners := make(map[execution.QueryGroupIdentity]phaseTwoQueryGroupRuntime, len(bundle.runners))
	for group, lifecycle := range bundle.runners {
		runners[group] = lifecycle.runner
	}
	bundle.mu.RUnlock()
	worker := bundle.dependencies.Config.PhaseTwo.Worker.ID
	for _, group := range groups {
		entry, inView := bundle.dependencies.ViewClient.Entry(group)
		if !inView {
			// The bundle knows every Query Group, held or not, and a Worker's
			// view is its own projection of the desired set
			// (viewstream/view.go:151): a group the record desires on another
			// Worker is not in this one's view, and nothing waits for it.
			if record, err := ownership.dependencies.Store.ReadAssignment(ctx, group); err == nil && record.DesiredWorkerID != worker {
				continue
			}
			return fmt.Sprintf("Query Group %s is not in the view", group)
		}
		// In the view without content: draining with nothing left to run, or
		// a Segment that names none. The view has spoken; there is nothing to
		// wait for, and a case about that Query Group reads its own word.
		if entry.Content == nil {
			continue
		}
		if entry.Assignment.TimelineRecordRevision == 0 {
			return fmt.Sprintf("Query Group %s's view entry names no timeline revision", group)
		}
		record, err := ownership.dependencies.Store.ReadAssignment(ctx, group)
		if err != nil {
			return fmt.Sprintf("Query Group %s's Assignment record did not read: %v", group, err)
		}
		if record.TimelineRecordRevision != entry.Assignment.TimelineRecordRevision {
			return fmt.Sprintf("Query Group %s's view is at timeline revision %d, its record at %d", group,
				entry.Assignment.TimelineRecordRevision, record.TimelineRecordRevision)
		}
		runtime, opened := runners[group].(*productionPhaseTwoQueryGroup)
		if !opened {
			continue
		}
		lease, held := runtime.session.Current()
		if !held {
			return fmt.Sprintf("Query Group %s's session holds no lease", group)
		}
		if lease.TimelineRecordRevision != record.TimelineRecordRevision || lease.ContentScope != record.ContentScope {
			ttl := bundle.dependencies.Config.PhaseTwo.Ownership.LeaseTTL.Duration()
			// A millisecond past the case's clock: a session takes no
			// confirmation dated at or before its last (ownership/session.go:179),
			// and a case's clock stands still while this waits, where a
			// Worker's moves on between the gate's confirmations.
			if err := runtime.session.Renew(ctx, runtime.clock().Add(time.Millisecond), ttl); err != nil {
				return fmt.Sprintf("Query Group %s's lease at timeline revision %d did not renew to its record's %d: %v", group,
					lease.TimelineRecordRevision, record.TimelineRecordRevision, err)
			}
			lease, _ = runtime.session.Current()
			if lease.TimelineRecordRevision != record.TimelineRecordRevision {
				return fmt.Sprintf("Query Group %s's lease renewed at timeline revision %d, its record at %d", group,
					lease.TimelineRecordRevision, record.TimelineRecordRevision)
			}
		}
	}
	return ""
}

// settledRunner is a fixture's handle on one of the bundle's Query Group
// runtimes that settles the executable view before each round, the way the
// scheduled pass does through runScheduledOnceSettled. A case that runs a
// round by hand runs it inside the same window a production Worker waits
// out after a rollout or a cutover, and would read view_not_executable
// where the case is about something else. A view that has not settled by
// the bound is the round's error (viewNotSettledError) and the round does
// not run. Everything but RunOne is the runtime's own.
func settledRunner(bundle *phaseTwoWorkerBundle, queryGroup execution.QueryGroupIdentity) phaseTwoQueryGroupRuntime {
	bundle.mu.RLock()
	lifecycle := bundle.runners[queryGroup]
	bundle.mu.RUnlock()
	if lifecycle == nil {
		return nil
	}
	return &settlingRuntime{phaseTwoQueryGroupRuntime: lifecycle.runner, bundle: bundle}
}

type settlingRuntime struct {
	phaseTwoQueryGroupRuntime
	bundle *phaseTwoWorkerBundle
}

func (runtime *settlingRuntime) RunOne(ctx context.Context) (execution.SlotExecutionResult, bool, error) {
	if err := settleExecutableView(ctx, runtime.bundle, 10*time.Second); err != nil {
		return execution.SlotExecutionResult{}, false, err
	}
	return runtime.phaseTwoQueryGroupRuntime.RunOne(ctx)
}

func (runtime *settlingRuntime) RunOneAdmitted(ctx context.Context, admission scheduler.ExecutionAdmission) (execution.SlotExecutionResult, bool, bool, error) {
	if err := settleExecutableView(ctx, runtime.bundle, 10*time.Second); err != nil {
		return execution.SlotExecutionResult{}, false, false, err
	}
	return runtime.phaseTwoQueryGroupRuntime.RunOneAdmitted(ctx, admission)
}
