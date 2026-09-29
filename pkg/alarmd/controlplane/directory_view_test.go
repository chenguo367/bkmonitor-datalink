// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
)

// viewFixture is a Leader that published a catalog and activated it: the
// publication its reconciler remembers, the activation on the store.
type viewFixture struct {
	h          *objectCatalogHarness
	reconciler *controlplane.SourceReconciler
	view       *controlplane.DirectoryView
	published  controlplane.PublishedSnapshot
	catalog    controlplane.Catalog
	at         time.Time
}

func newViewFixture(t *testing.T, catalog controlplane.Catalog) *viewFixture {
	t.Helper()
	h := newObjectCatalogHarness(t)
	scheduled := catalogWithSchedule(t, catalog, 60, 0)
	published := h.publish(t, scheduled)
	if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, published.Publication); err != nil {
		t.Fatal(err)
	}
	f := &viewFixture{h: h, published: published, catalog: scheduled, at: time.Unix(1000, 0)}
	f.reconciler = f.newReconciler(t)
	f.reconciler.RememberPublicationForTest(published.Publication, scheduled)
	f.view = f.newView(t, f.reconciler, h.repository)
	return f
}

func (f *viewFixture) newReconciler(t *testing.T) *controlplane.SourceReconciler {
	t.Helper()
	compiler, semantics := runtimePlanCompiler(t)
	reconciler, err := controlplane.NewSourceReconciler(f.h.repository, compiler, semantics)
	if err != nil {
		t.Fatal(err)
	}
	return reconciler
}

func (f *viewFixture) newView(t *testing.T, reconciler *controlplane.SourceReconciler, repository *controlplane.RedisCatalogRepository) *controlplane.DirectoryView {
	t.Helper()
	view, err := controlplane.NewDirectoryView(reconciler, repository, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func (f *viewFixture) page(t *testing.T, view *controlplane.DirectoryView, strategy string) controlplane.StrategyDirectorySnapshot {
	t.Helper()
	return view.Page(f.h.ctx, f.at, "", "", strategy, 0, 100)
}

func (f *viewFixture) group(t *testing.T, catalog controlplane.Catalog, identity execution.QueryGroupIdentity) controlplane.QueryGroup {
	t.Helper()
	for _, group := range catalog.QueryGroups {
		if group.Identity == identity {
			return group
		}
	}
	t.Fatalf("no Query Group %s in the catalog", identity)
	return controlplane.QueryGroup{}
}

// The directory is the Leader's own publication and the activation it runs:
// every Plan a row with the Query Group, the object digest and the
// revisions the catalog published, the activation that selected it, and the
// output context it renders by, with no copy kept between requests. A
// process that holds no publication of its own - never published, or a
// Leader that stepped down - is not ready, and says so rather than
// answering empty.
func TestTheDirectoryIsTheLeadersPublicationAndItsActivation(t *testing.T) {
	f := newViewFixture(t, objectCatalogTwoGroups(t, 80))

	s := f.page(t, f.view, "")
	if !s.Complete || s.Reason != "" || len(s.Rows) != 2 {
		t.Fatalf("directory = complete %v reason %q rows %d, want complete with both Plans: %+v", s.Complete, s.Reason, len(s.Rows), s)
	}
	pub := f.published.Publication
	if s.Published != pub || s.Current != pub || s.SourceObservation != f.catalog.ObservationID || s.GroupsTotal != 2 || s.GroupsKnown != 2 {
		t.Fatalf("directory = published %+v current %+v observation %q groups %d/%d, want the Leader's publication, its activation and observation %q, 2 groups",
			s.Published, s.Current, s.SourceObservation, s.GroupsKnown, s.GroupsTotal, f.catalog.ObservationID)
	}
	if want := fmt.Sprintf("%s:%d:%d", pub.SnapshotRevision, pub.PublicationEpoch, s.ActivationRevision); s.ActivationRevision == 0 || s.Revision != want {
		t.Fatalf("revision = %q (activation %d), want %q", s.Revision, s.ActivationRevision, want)
	}
	if len(s.Publications) != 1 || s.Publications[0] != (controlplane.DirectoryPublication{Publication: pub, Plans: 2, Manifest: "memory"}) {
		t.Fatalf("publications = %+v, want the Leader's one, from memory, carrying both Plans", s.Publications)
	}
	wantOrder := []string{"1001", "1002"}
	for index, row := range s.Rows {
		group := f.group(t, f.catalog, row.QueryGroup)
		digest, err := controlplane.DeriveQueryGroupObjectDigest(group)
		if err != nil {
			t.Fatal(err)
		}
		if row.Identity.StrategyID != wantOrder[index] || row.Publication != pub || row.Role != string(execution.ActivationCurrent) ||
			row.Activation == nil || row.Activation.Selection != execution.ActivationCurrent || row.Activation.Plan != row.Identity {
			t.Fatalf("row %d = %+v, want strategy %s current under the Leader's publication", index, row, wantOrder[index])
		}
		if row.ObjectDigest != digest || row.QueryRevision != group.QueryPlan.QueryRevision || row.ScheduleRevision != group.ScheduleRevision {
			t.Fatalf("row %d = digest %s query %s schedule %s, want the published group's %s %s %s", index,
				row.ObjectDigest, row.QueryRevision, row.ScheduleRevision, digest, group.QueryPlan.QueryRevision, group.ScheduleRevision)
		}
		if row.OutputContext == "" || row.ManifestExpired {
			t.Fatalf("row %d = %+v, want its output context named from the content the activation read", index, row)
		}
	}

	// One strategy, its dispositions beside it; a filter that matches no
	// identity is an empty answer, not another strategy's rows; a page is a
	// window over the same order.
	one := f.page(t, f.view, "1002")
	if len(one.Rows) != 1 || one.Rows[0].Identity.StrategyID != "1002" || one.SourceMatchedTotal == 0 || len(one.Unattributed) != one.SourceMatchedTotal {
		t.Fatalf("strategy 1002 = rows %+v, dispositions %d of %d, want its row and its dispositions", one.Rows, len(one.Unattributed), one.SourceMatchedTotal)
	}
	for _, disposition := range one.Unattributed {
		if disposition.SourceID != "1002" {
			t.Fatalf("strategy 1002 carries a disposition of %s", disposition.SourceID)
		}
	}
	if other := f.view.Page(f.h.ctx, f.at, "tenant-b", "", "", 0, 100); len(other.Rows) != 0 || !other.Complete {
		t.Fatalf("another tenant = %+v, want complete and empty", other.Rows)
	}
	if business := f.view.Page(f.h.ctx, f.at, "", "3", "", 0, 100); len(business.Rows) != 1 || business.Rows[0].Identity.BusinessID != "3" {
		t.Fatalf("business 3 = %+v, want its one row", business.Rows)
	}
	if second := f.view.Page(f.h.ctx, f.at, "", "", "", 1, 1); len(second.Rows) != 1 || second.Rows[0].Identity != s.Rows[1].Identity {
		t.Fatalf("second page of one = %+v, want the second row", second.Rows)
	}

	// The current row resolves, pinned to the revision a page was read at;
	// its Plan is the object's.
	row, err := f.view.ResolveCurrent(f.h.ctx, f.at, "", "", "1001", "", s.Revision)
	if err != nil || row.Identity.StrategyID != "1001" || row.Role != string(execution.ActivationCurrent) {
		t.Fatalf("resolve 1001 = (%+v, %v), want its current row", row, err)
	}
	if _, err := f.view.ResolveCurrent(f.h.ctx, f.at, "", "", "1001", "", "a-revision-read-before"); !errors.Is(err, controlplane.ErrObservationChanged) {
		t.Fatalf("resolve at a stale revision = %v, want ErrObservationChanged", err)
	}
	if _, err := f.view.ResolveCurrent(f.h.ctx, f.at, "", "", "4242", ""); !errors.Is(err, controlplane.ErrSnapshotUnavailable) {
		t.Fatalf("resolve a strategy with no row = %v, want ErrSnapshotUnavailable", err)
	}
	plan, err := f.view.EffectivePlan(f.h.ctx, row)
	if err != nil || plan.Identity != row.Identity {
		t.Fatalf("effective plan = (%+v, %v), want the row's Plan", plan.Identity, err)
	}

	// Not ready: a process that never published, and the Leader once it
	// stepped down.
	follower := f.newView(t, f.newReconciler(t), f.h.repository)
	for label, view := range map[string]*controlplane.DirectoryView{"never published": follower, "stepped down": f.view} {
		if label == "stepped down" {
			f.reconciler.StepDown()
		}
		if view.Available() {
			t.Fatalf("%s: available, want not", label)
		}
		answer := f.page(t, view, "")
		if answer.Complete || answer.Reason != "LEADER_CATALOG_NOT_READY" || answer.Rows == nil || len(answer.Rows) != 0 {
			t.Fatalf("%s: page = %+v, want not ready, incomplete and empty", label, answer)
		}
		if _, err := view.ResolveCurrent(f.h.ctx, f.at, "", "", "1001", ""); !errors.Is(err, controlplane.ErrSnapshotUnavailable) {
			t.Fatalf("%s: resolve = %v, want ErrSnapshotUnavailable", label, err)
		}
	}
}

// A Query Group drains on the publication before the one the Leader just
// made: the activation still carries its Plans there. Each such Plan is a
// row of the carried publication with its revisions read from its object,
// beside the row the latest publication has for it, and it is the current
// one. A carried publication whose manifest is past its retention names
// its Plans from the activation alone and the answer goes on; one whose
// manifest does not read fails the answer by name and guesses nothing.
func TestADrainingPublicationsPlansAreRowsNamedFromItsContent(t *testing.T) {
	for _, side := range []string{"read", "manifest expired", "manifest unreadable"} {
		t.Run(side, func(t *testing.T) {
			f := newViewFixture(t, objectCatalogTwoGroups(t, 80))
			carried := f.published.Publication
			// A later publication the activation has not cut over to.
			latestCatalog := catalogWithSchedule(t, objectCatalogTwoGroups(t, 90), 60, 0)
			latest := f.h.publish(t, latestCatalog)
			f.reconciler.RememberPublicationForTest(latest.Publication, latestCatalog)
			carriedKey := f.h.prefix + ":manifest:" + string(carried.SnapshotRevision)
			view := f.view
			switch side {
			case "manifest expired":
				if err := f.h.client.Del(f.h.ctx, carriedKey).Err(); err != nil {
					t.Fatal(err)
				}
			case "manifest unreadable":
				if err := f.h.client.Set(f.h.ctx, carriedKey, "{", 0).Err(); err != nil {
					t.Fatal(err)
				}
				// A repository that has not read it: the Leader's own would
				// answer from the content it remembers.
				view = f.newView(t, f.reconciler, f.h.newRepository(t))
			}
			s := f.page(t, view, "")
			if s.Published != latest.Publication || s.Current != carried {
				t.Fatalf("published %+v current %+v, want the latest publication beside the carried activation", s.Published, s.Current)
			}
			var latestRows, carriedRows, expiredRows int
			for _, row := range s.Rows {
				switch {
				case row.ManifestExpired:
					expiredRows++
					if row.Publication != carried || row.QueryGroup != "" || row.ObjectDigest != "" || row.Activation == nil ||
						row.Role != string(execution.ActivationCurrent) {
						t.Fatalf("expired row = %+v, want the carried Plan named by its activation and nothing guessed", row)
					}
				case row.Publication == latest.Publication:
					latestRows++
					if row.Activation != nil || row.Role != "PUBLISHED" || row.QueryGroup == "" || row.ObjectDigest == "" {
						t.Fatalf("latest row = %+v, want published and not yet active", row)
					}
				case row.Publication == carried:
					carriedRows++
					group := f.group(t, f.catalog, row.QueryGroup)
					digest, err := controlplane.DeriveQueryGroupObjectDigest(group)
					if err != nil {
						t.Fatal(err)
					}
					if row.Activation == nil || row.Role != string(execution.ActivationCurrent) || row.ObjectDigest != digest ||
						row.QueryRevision != group.QueryPlan.QueryRevision || row.ScheduleRevision != group.ScheduleRevision || row.OutputContext == "" {
						t.Fatalf("carried row = %+v, want current with the carried group's digest %s and revisions %s %s", row, digest,
							group.QueryPlan.QueryRevision, group.ScheduleRevision)
					}
				default:
					t.Fatalf("row %+v belongs to neither publication", row)
				}
			}
			switch side {
			case "read":
				if !s.Complete || latestRows != 2 || carriedRows != 2 || expiredRows != 0 {
					t.Fatalf("rows latest %d carried %d expired %d complete %v, want 2, 2, 0 and complete: %+v", latestRows, carriedRows, expiredRows, s.Complete, s)
				}
				if len(s.Publications) != 2 || s.Publications[1].Publication != carried || s.Publications[1].Plans != 2 || s.Publications[1].Manifest != "memory" {
					t.Fatalf("publications = %+v, want the carried one second, with both Plans, from memory", s.Publications)
				}
				// The current row is the carried one, not the latest.
				row, err := view.ResolveCurrent(f.h.ctx, f.at, "", "", "1001", "")
				if err != nil || row.Publication != carried {
					t.Fatalf("resolve 1001 = (%+v, %v), want the carried row", row, err)
				}
				// Rows of one identity: by Query Group, then publication.
				for index := 1; index < len(s.Rows); index++ {
					previous, row := s.Rows[index-1], s.Rows[index]
					if previous.Identity == row.Identity && previous.QueryGroup == row.QueryGroup &&
						previous.Publication.PublicationEpoch > row.Publication.PublicationEpoch {
						t.Fatalf("rows %d and %d out of publication order: %+v, %+v", index-1, index, previous, row)
					}
				}
			case "manifest expired":
				if !s.Complete || s.FailedRead != "" || latestRows != 2 || expiredRows != 2 || carriedRows != 0 {
					t.Fatalf("rows latest %d carried %d expired %d complete %v failed %q, want 2, 0, 2, complete", latestRows, carriedRows, expiredRows, s.Complete, s.FailedRead)
				}
				if s.Publications[1].Manifest != "expired" {
					t.Fatalf("publications = %+v, want the carried one expired", s.Publications)
				}
			case "manifest unreadable":
				if s.Complete || s.Reason != "DEPENDENCY_UNAVAILABLE" || s.FailedRead != "manifest" || s.FailedKey != carriedKey ||
					s.FailedPublication == nil || *s.FailedPublication != carried || s.Error == "" {
					t.Fatalf("unreadable = complete %v reason %q read %q key %q publication %+v error %q, want the carried manifest named",
						s.Complete, s.Reason, s.FailedRead, s.FailedKey, s.FailedPublication, s.Error)
				}
				if expiredRows != 0 || carriedRows != 0 || s.Publications[1].Manifest != "failed" {
					t.Fatalf("rows expired %d carried %d publications %+v, want none guessed and the carried one failed", expiredRows, carriedRows, s.Publications)
				}
			}
		})
	}
}

// The strategy-level output read reports the word the Leader froze with the
// Plan, not what the deployment's choice would resolve to now, from the
// Leader's own object cache first; its gaps have names. Legacy is the
// choice the automatic rule never produces for a revisioned strategy, so a
// read that re-derived the format would report something else.
func TestTheDirectoryReadsTheFrozenOutputFormatNotTheCurrentChoice(t *testing.T) {
	byRule, _ := controlplane.EffectiveWireFormat("", 7)
	if byRule == contract.WireFormatPythonCompatible {
		t.Fatalf("the rule resolves a revisioned strategy to %q, the fixture's word: nothing here discriminates", byRule)
	}
	f := newViewFixture(t, revisionedCatalog(t, "legacy"))
	if err := f.h.repository.ConfigureObjectCache(64, 1<<20); err != nil {
		t.Fatal(err)
	}
	s := f.page(t, f.view, "")
	if len(s.Rows) != 2 {
		t.Fatalf("rows = %+v, want two", s.Rows)
	}
	for _, row := range s.Rows {
		selected, err := f.view.ResolveCurrent(f.h.ctx, f.at, row.Identity.TenantID, row.Identity.BusinessID, row.Identity.StrategyID, string(row.QueryGroup))
		if err != nil {
			t.Fatal(err)
		}
		facts := f.view.EffectiveOutput(f.h.ctx, selected)
		if !facts.Known || facts.WireFormat != contract.WireFormatPythonCompatible || facts.EffectiveWireFormat != contract.WireFormatPythonCompatible ||
			facts.DecidedBy != controlplane.WireFormatDecidedFrozen || facts.SnapshotRevision != 7 || !facts.CompatibilityContext ||
			facts.OutputContextDigest != row.OutputContext {
			t.Fatalf("output for %s = %+v, want the frozen legacy word with its context, revision 7, the row's digest", row.Identity.StrategyID, facts)
		}
	}
	row := s.Rows[0]
	unretained := row
	unretained.OutputContext = ""
	if facts := f.view.EffectiveOutput(f.h.ctx, unretained); facts.Known || facts.Reason != controlplane.OutputContextRefNotRetained {
		t.Fatalf("row without a digest = %+v, want %s", facts, controlplane.OutputContextRefNotRetained)
	}
	// The object gone from the store: the Leader answers from its own cache,
	// a repository without it says unavailable, not corrupt.
	h := f.h
	h.client.Del(h.ctx, h.prefix+":outctx:"+string(row.OutputContext))
	if facts := f.view.EffectiveOutput(h.ctx, row); !facts.Known {
		t.Fatalf("the Leader after the store lost the object = %+v, want known from its cache", facts)
	}
	cold := f.newView(t, f.reconciler, h.newRepository(t))
	if facts := cold.EffectiveOutput(h.ctx, row); facts.Known || facts.Reason != controlplane.OutputContextUnavailable {
		t.Fatalf("deleted object on a cold repository = %+v, want %s", facts, controlplane.OutputContextUnavailable)
	}

	// Over HTTP, beside the configuration, under the include only.
	api := fleet.WithStrategyDirectory(http.NotFoundHandler(), f.view, nil, func() time.Time { return f.at })
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&tenant=tenant-a&business=2&strategy=1001&include=effective_config", nil))
	var body struct {
		EffectiveConfig json.RawMessage                 `json:"effective_config"`
		EffectiveOutput *controlplane.OutputFormatFacts `json:"effective_output"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 200 {
		t.Fatalf("config request = %d %s (%v)", w.Code, w.Body.String(), err)
	}
	if len(body.EffectiveConfig) == 0 || body.EffectiveOutput == nil || !body.EffectiveOutput.Known {
		t.Fatalf("response = %s, want the configuration and a known effective output", w.Body.String())
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&tenant=tenant-a&business=2&strategy=1001", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), `"effective_output"`) {
		t.Fatalf("list response = %d %s, want no effective_output without the include", w.Code, w.Body.String())
	}
}

// An output context the earlier rule wrote carries trigger_event_v1, a word
// no Plan selects now and the sink resolves to the standard raw event: the
// read says both, and names the decision.
func TestTheDirectoryReportsAHistoricalFrozenWordBesideWhatIsWritten(t *testing.T) {
	f := newViewFixture(t, revisionedCatalog(t, ""))
	row := f.page(t, f.view, "").Rows[0]
	if current := f.view.EffectiveOutput(f.h.ctx, row); !current.Known || current.WireFormat != contract.WireFormatStandardRawEvent ||
		current.DecidedBy != controlplane.WireFormatDecidedFrozen {
		t.Fatalf("a Plan built now = %+v, want the standard raw event, frozen", current)
	}
	payload, err := f.h.client.Get(f.h.ctx, f.h.prefix+":outctx:"+string(row.OutputContext)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var historical controlplane.OutputContextObject
	if err = json.Unmarshal(payload, &historical); err != nil {
		t.Fatal(err)
	}
	historical.WireFormat = contract.WireFormatTriggerEvent
	digest, err := contract.DeriveCanonicalDigestV2(historical.ContractVersion, historical)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := contract.CanonicalJSONV2(historical)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.h.client.Set(f.h.ctx, f.h.prefix+":outctx:"+digest, encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	row.OutputContext = execution.OutputContextDigest(digest)
	facts := f.view.EffectiveOutput(f.h.ctx, row)
	if !facts.Known || facts.WireFormat != contract.WireFormatTriggerEvent || facts.DecidedBy != controlplane.WireFormatDecidedHistorical ||
		facts.EffectiveWireFormat != contract.ResolveOutputWireFormat(contract.WireFormatTriggerEvent, facts.SnapshotRevision) ||
		facts.EffectiveWireFormat == facts.WireFormat {
		t.Fatalf("historical object = %+v, want the word in the object, what the sink writes, and the decision named", facts)
	}
}

// forwardRecorder stands in for the hop to the Leader: it answers with the
// Leader's handler, marked as forwarded, or refuses with a word.
type forwardRecorder struct {
	leader  http.Handler
	refusal string
	calls   int
	methods []string
	bodies  []string
}

func (r *forwardRecorder) forward(w http.ResponseWriter, request *http.Request) (bool, string) {
	r.calls++
	if r.refusal != "" {
		return false, r.refusal
	}
	payload, _ := io.ReadAll(request.Body)
	r.methods, r.bodies = append(r.methods, request.Method), append(r.bodies, string(payload))
	hop := httptest.NewRequest(request.Method, request.URL.RequestURI(), bytes.NewReader(payload))
	hop.Header.Set(fleet.ForwardedHeader(), "follower")
	r.leader.ServeHTTP(w, hop)
	return true, ""
}

// A follower answers the directory by handing the request to the Leader;
// a request already handed on is answered where it lands, as not ready,
// never handed on again; no Leader to hand it to is its own refusal. The
// Leader answers from memory.
func TestAFollowerHandsTheDirectoryToTheLeader(t *testing.T) {
	f := newViewFixture(t, objectCatalogTwoGroups(t, 80))
	now := func() time.Time { return f.at }
	leader := fleet.WithStrategyDirectory(http.NotFoundHandler(), f.view, nil, now)
	hop := &forwardRecorder{leader: leader}
	followerView := f.newView(t, f.newReconciler(t), f.h.repository)
	follower := fleet.WithStrategyDirectory(http.NotFoundHandler(), followerView, hop.forward, now)

	w := httptest.NewRecorder()
	follower.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&strategy=1001", nil))
	var answered controlplane.StrategyDirectorySnapshot
	if err := json.Unmarshal(w.Body.Bytes(), &answered); err != nil || w.Code != 200 || hop.calls != 1 || len(answered.Rows) != 1 {
		t.Fatalf("follower = %d %s after %d hops (%v), want the Leader's rows through one hop", w.Code, w.Body.String(), hop.calls, err)
	}

	w = httptest.NewRecorder()
	handedOn := httptest.NewRequest("GET", "/api/objects?scope=strategies&strategy=1001", nil)
	handedOn.Header.Set(fleet.ForwardedHeader(), "another")
	follower.ServeHTTP(w, handedOn)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "LEADER_CATALOG_NOT_READY") || hop.calls != 1 {
		t.Fatalf("a handed-on request on a follower = %d %s after %d hops, want not ready and no second hop", w.Code, w.Body.String(), hop.calls)
	}

	refused := &forwardRecorder{refusal: "NO_LEADER"}
	w = httptest.NewRecorder()
	fleet.WithStrategyDirectory(http.NotFoundHandler(), followerView, refused.forward, now).
		ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies", nil))
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "LEADER_UNAVAILABLE") || !strings.Contains(w.Body.String(), "NO_LEADER") {
		t.Fatalf("no Leader = %d %s, want LEADER_UNAVAILABLE with the reason", w.Code, w.Body.String())
	}

	// The Leader answers itself and hands nothing on.
	w = httptest.NewRecorder()
	own := &forwardRecorder{leader: http.NotFoundHandler()}
	fleet.WithStrategyDirectory(http.NotFoundHandler(), f.view, own.forward, now).
		ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies", nil))
	if w.Code != 200 || own.calls != 0 {
		t.Fatalf("the Leader = %d after %d hops, want its own answer", w.Code, own.calls)
	}
}

// callerHook records the caller every command of a client names.
type callerHook struct {
	mu      sync.Mutex
	callers []string
}

func (h *callerHook) BeforeProcess(ctx context.Context, _ redis.Cmder) (context.Context, error) {
	h.mu.Lock()
	h.callers = append(h.callers, redisfailure.Caller(ctx))
	h.mu.Unlock()
	return ctx, nil
}

func (h *callerHook) AfterProcess(context.Context, redis.Cmder) error { return nil }

func (h *callerHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	h.mu.Lock()
	for range cmds {
		h.callers = append(h.callers, redisfailure.Caller(ctx))
	}
	h.mu.Unlock()
	return ctx, nil
}

func (h *callerHook) AfterProcessPipeline(context.Context, []redis.Cmder) error { return nil }

// Every read an answer makes - the activation, a carried publication, an
// object, a Plan, an output context - names itself directory_read, so a
// failure of the client it goes through says which reader lost it.
func TestTheDirectoryNamesItsRedisReads(t *testing.T) {
	f := newViewFixture(t, objectCatalogTwoGroups(t, 80))
	latestCatalog := catalogWithSchedule(t, objectCatalogTwoGroups(t, 90), 60, 0)
	latest := f.h.publish(t, latestCatalog)
	f.reconciler.RememberPublicationForTest(latest.Publication, latestCatalog)
	client := redis.NewClient(f.h.client.Options())
	t.Cleanup(func() { _ = client.Close() })
	hook := &callerHook{}
	client.AddHook(hook)
	repository, err := controlplane.NewRedisCatalogRepository(client, f.h.prefix, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	view := f.newView(t, f.reconciler, repository)
	s := f.page(t, view, "")
	row, err := view.ResolveCurrent(f.h.ctx, f.at, "", "", "1001", "")
	if err != nil || len(s.Rows) == 0 {
		t.Fatalf("rows %d, resolve %v", len(s.Rows), err)
	}
	if _, err := view.EffectivePlan(f.h.ctx, row); err != nil {
		t.Fatal(err)
	}
	_ = view.EffectiveOutput(f.h.ctx, row)
	if len(hook.callers) == 0 {
		t.Fatal("the answer made no read through the repository's client")
	}
	for _, caller := range hook.callers {
		if caller != redisfailure.CallerDirectoryRead {
			t.Fatalf("a directory read named itself %q: %v", caller, hook.callers)
		}
	}
}

// The content memo keeps the two publications read last: the current one
// and the one a draining Query Group still runs on, read alternately by one
// activation round, both stay; a third puts out the older.
func TestTheContentMemoKeepsTwoPublications(t *testing.T) {
	ref := func(epoch uint64) controlplane.SnapshotPublicationRef {
		return controlplane.SnapshotPublicationRef{SnapshotRevision: execution.SnapshotRevision(strings.Repeat(fmt.Sprint(epoch), 64)), PublicationEpoch: epoch}
	}
	content := func(epoch uint64) controlplane.PublishedContent {
		return controlplane.PublishedContent{Publication: ref(epoch)}
	}
	var memo controlplane.ContentMemoForTest
	memo.Store(content(1))
	memo.Store(content(2))
	memo.Store(content(1))
	memo.Store(content(2))
	if !memo.Holds(ref(1)) || !memo.Holds(ref(2)) {
		t.Fatalf("two publications read alternately: holds 1 %v, 2 %v, want both", memo.Holds(ref(1)), memo.Holds(ref(2)))
	}
	memo.Store(content(3))
	if memo.Holds(ref(1)) || !memo.Holds(ref(2)) || !memo.Holds(ref(3)) {
		t.Fatalf("after a third: holds 1 %v, 2 %v, 3 %v, want 2 and 3", memo.Holds(ref(1)), memo.Holds(ref(2)), memo.Holds(ref(3)))
	}
	memo.Store(content(3))
	if !memo.Holds(ref(2)) || !memo.Holds(ref(3)) {
		t.Fatalf("the newest read again: holds 2 %v, 3 %v, want both", memo.Holds(ref(2)), memo.Holds(ref(3)))
	}
	if memo.Holds(controlplane.SnapshotPublicationRef{}) {
		t.Fatal("an empty publication is held")
	}
}
