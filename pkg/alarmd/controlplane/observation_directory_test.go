package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

func directoryFixture(t *testing.T, commands int) (*objectCatalogHarness, *controlplane.ObservationDirectory, time.Time) {
	t.Helper()
	h := newObjectCatalogHarness(t)
	pub := h.publish(t, catalogWithSchedule(t, objectCatalogTwoGroups(t, 80), 60, 0))
	if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, pub.Publication); err != nil {
		t.Fatal(err)
	}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: commands, Entries: 100, Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return h, d, time.Unix(1000, 0)
}

type directoryReadSpy struct {
	redis.Cmdable
	oversize string
	reads    map[string]int
}

func (s *directoryReadSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	s.reads[key]++
	if key == s.oversize {
		return redis.NewStringResult(strings.Repeat("x", int(end-start+1)), nil)
	}
	return s.Cmdable.GetRange(ctx, key, start, end)
}

func TestObservationDirectoryOversizeCannotStarveSiblingOrCarriedPublication(t *testing.T) {
	for _, acrossPublications := range []bool{false, true} {
		t.Run(map[bool]string{false: "sibling", true: "carried"}[acrossPublications], func(t *testing.T) {
			h, baseline, at := directoryFixture(t, 32)
			baseline.Refresh(h.ctx, at)
			before := baseline.Page(at, "", "", "", 0, 20)
			manifest, err := h.repository.LoadCatalogManifest(h.ctx, before.Published.SnapshotRevision)
			if err != nil {
				t.Fatal(err)
			}
			oversize := h.prefix + ":qgobj:" + string(manifest.QueryGroups[0].ObjectDigest)
			if acrossPublications {
				manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
				manifest.QueryGroups = manifest.QueryGroups[:1]
				manifest.QueryGroups[0].ObjectDigest = execution.ObjectDigest(strings.Repeat("f", 64))
				oversize = h.prefix + ":qgobj:" + string(manifest.QueryGroups[0].ObjectDigest)
				payload, _ := json.Marshal(manifest)
				if err = h.client.Set(h.ctx, h.prefix+":manifest:"+string(manifest.SnapshotRevision), payload, 0).Err(); err != nil {
					t.Fatal(err)
				}
				if err = h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			spy := &directoryReadSpy{Cmdable: h.client, oversize: oversize, reads: map[string]int{}}
			d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, spy)
			if err != nil {
				t.Fatal(err)
			}
			d.Refresh(h.ctx, at)
			d.Refresh(h.ctx, at.Add(time.Second))
			s := d.Page(at.Add(time.Second), "", "", "", 0, 20)
			if s.Complete || len(s.Rows) == 0 || s.ReadBytes > (1<<20)+1 {
				t.Fatalf("cold work starved or unbounded: %+v", s)
			}
			if acrossPublications && s.Rows[0].Role != string(execution.ActivationCurrent) {
				t.Fatalf("carried current missing: %+v", s.Rows)
			}
		})
	}
}

func TestObservationDirectorySameDigestAcrossPublicationsIsReadOnce(t *testing.T) {
	h, baseline, at := directoryFixture(t, 32)
	baseline.Refresh(h.ctx, at)
	before := baseline.Page(at, "", "", "", 0, 20)
	manifest, err := h.repository.LoadCatalogManifest(h.ctx, before.Published.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
	payload, _ := json.Marshal(manifest)
	h.client.Set(h.ctx, h.prefix+":manifest:"+string(manifest.SnapshotRevision), payload, 0)
	h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0)
	spy := &directoryReadSpy{Cmdable: h.client, reads: map[string]int{}}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if !s.Complete || len(s.Rows) != 4 {
		t.Fatalf("carried directory %+v", s)
	}
	for _, ref := range manifest.QueryGroups {
		if got := spy.reads[h.prefix+":qgobj:"+string(ref.ObjectDigest)]; got != 1 {
			t.Fatalf("duplicate immutable object read %d", got)
		}
	}
}

func TestObservationDirectoryCursorAndConfigRevisionArePinned(t *testing.T) {
	h, d, at := directoryFixture(t, 32)
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if _, err := d.ResolveCurrent(at, "", "", s.Rows[0].Identity.StrategyID, "", "old-revision"); !errors.Is(err, controlplane.ErrObservationChanged) {
		t.Fatalf("mixed response version accepted: %v", err)
	}
	api := fleet.WithStrategyDirectory(http.NotFoundHandler(), d, func() time.Time { return at })
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&limit=1", nil))
	var page struct {
		Next string `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || page.Next == "" {
		t.Fatalf("cursor %s %v", w.Body.String(), err)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&limit=1&business=other&cursor="+page.Next, nil))
	if w.Code != 400 {
		t.Fatalf("filter mismatch=%d", w.Code)
	}
}

func TestObservationDirectoryColdWarmPointConfigAndReadOnlyHTTP(t *testing.T) {
	h, d, at := directoryFixture(t, 32)
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if !s.Complete || len(s.Rows) != 2 {
		t.Fatalf("directory %+v", s)
	}
	row := s.Rows[0]
	selected, err := d.ResolveCurrent(at, row.Identity.TenantID, row.Identity.BusinessID, row.Identity.StrategyID, string(row.QueryGroup))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := d.EffectivePlan(h.ctx, selected)
	if err != nil || plan.Identity != row.Identity {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	// Deleting the backing object cannot break cached directory pagination,
	// but point config must never fall back to a full snapshot to hide it.
	h.client.Del(h.ctx, h.prefix+":qgobj:"+string(row.ObjectDigest))
	if _, err = d.EffectivePlan(h.ctx, selected); err == nil {
		t.Fatal("missing object silently reconstructed")
	}
	api := fleet.WithStrategyDirectory(http.NotFoundHandler(), d, func() time.Time { return at })
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&limit=1", nil))
	if w.Code != 200 {
		t.Fatalf("directory request=%d %s", w.Code, w.Body.String())
	}
	stale := d.Page(at.Add(2*time.Minute), "", "", "", 0, 20)
	if stale.Complete || stale.Reason != "STALE" {
		t.Fatalf("stale projection claims current: %+v", stale)
	}
	if _, err = d.ResolveCurrent(at.Add(2*time.Minute), "", "", row.Identity.StrategyID, ""); err == nil {
		t.Fatal("stale config selected")
	}
}

func TestObservationDirectoryColdBudgetMakesProgressWithoutHTTPReads(t *testing.T) {
	h, d, at := directoryFixture(t, 3)
	// Latest publication, manifest, then exactly one cold object; the
	// activation comes through the repository's own cache and is not one of
	// the directory's commands. A later tick reuses that identity projection.
	d.Refresh(h.ctx, at)
	first := d.Page(at, "", "", "", 0, 20)
	if first.Complete || first.ReadCommands > 3 || len(first.Rows) != 1 {
		t.Fatalf("first %+v", first)
	}
	d.Refresh(h.ctx, at.Add(time.Second))
	second := d.Page(at.Add(time.Second), "", "", "", 0, 20)
	if !second.Complete || len(second.Rows) != 2 {
		t.Fatalf("cold load did not advance: %+v", second)
	}
	ctx, cancel := context.WithCancel(h.ctx)
	cancel()
	d.Refresh(ctx, at.Add(2*time.Second))
	failed := d.Page(at.Add(2*time.Second), "", "", "", 0, 20)
	if failed.Complete || failed.Reason == "" {
		t.Fatalf("dependency failure claims complete: %+v", failed)
	}
}

func TestObservationOversizeAuditCannotStarveCoreDirectory(t *testing.T) {
	h, d, at := directoryFixture(t, 32)
	h.client.Set(h.ctx, h.prefix+":latest_audit", "large", 0)
	h.client.Set(h.ctx, h.prefix+":audit:large", strings.Repeat("x", 2<<20), 0)
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if !s.Complete || len(s.Rows) != 2 || s.SourceComplete || s.SourceReason != "RESOURCE_BUDGET" || s.ReadBytes > (1<<20)+1 {
		t.Fatalf("optional audit starved core directory %+v", s)
	}
}

func TestObservationDirectoryWireBudgetAndUnknownAreNotNotFound(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 8, Commands: 2, Entries: 2, Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if s.Complete || s.Reason != "RESOURCE_BUDGET" || s.ReadBytes > 9 {
		t.Fatalf("unbounded read %+v", s)
	}
	api := fleet.WithStrategyDirectory(http.NotFoundHandler(), d, func() time.Time { return at })
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&strategy=missing", nil))
	if w.Code != 503 {
		t.Fatalf("unknown treated as absence: %d", w.Code)
	}
	if _, err = d.ResolveCurrent(at, "", "", "missing", ""); !errors.Is(err, controlplane.ErrSnapshotUnavailable) {
		t.Fatalf("resolve %v", err)
	}
}

// The directory rides the runtime's own reads. A process that has loaded the
// published content holds the manifest, entry for entry, in its catalog
// index and the parsed activation behind a header check; the directory reads
// neither the manifest nor the activation body on its own budget on such a
// process, and its snapshot says so. A process that has not loaded the content
// -- cold, or a carried publication -- reads the manifest, once. Read on the
// directory's budget they were the whole manifest and activation every refresh
// on every replica, for a diagnostics projection: the shape of the outbound
// bandwidth this deployment already fell over once.
func TestObservationDirectoryReusesTheRuntimesManifestAndActivation(t *testing.T) {
	h := newObjectCatalogHarness(t)
	pub := h.publish(t, catalogWithSchedule(t, objectCatalogTwoGroups(t, 80), 60, 0))
	if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, pub.Publication); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1000, 0)
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	manifestKey := h.prefix + ":manifest:" + string(pub.Publication.SnapshotRevision)
	activationKey := h.prefix + ":activation"

	// Warm: the repository that loaded the content. Its index is the manifest.
	if _, err := h.repository.LoadPublishedContent(h.ctx, pub.Publication); err != nil {
		t.Fatal(err)
	}
	warmSpy := &directoryReadSpy{Cmdable: h.client, reads: map[string]int{}}
	warm, err := controlplane.NewObservationDirectory(h.repository, limits, warmSpy)
	if err != nil {
		t.Fatal(err)
	}
	warm.Refresh(h.ctx, at)
	warmSnapshot := warm.Page(at, "", "", "", 0, 20)
	if !warmSnapshot.Complete || len(warmSnapshot.Rows) != 2 || warmSnapshot.ManifestsFromIndex != 1 {
		t.Fatalf("warm directory = complete %v, rows %d, manifests from index %d; want complete, 2 rows, 1 manifest from the index: %+v",
			warmSnapshot.Complete, len(warmSnapshot.Rows), warmSnapshot.ManifestsFromIndex, warmSnapshot)
	}
	if warmSpy.reads[manifestKey] != 0 || warmSpy.reads[activationKey] != 0 {
		t.Fatalf("warm directory read the manifest %d times and the activation body %d times on its own budget; want neither",
			warmSpy.reads[manifestKey], warmSpy.reads[activationKey])
	}

	// Cold: a repository that has loaded nothing reads the manifest, once, and
	// still not the activation body on its own budget.
	coldSpy := &directoryReadSpy{Cmdable: h.client, reads: map[string]int{}}
	cold, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, coldSpy)
	if err != nil {
		t.Fatal(err)
	}
	cold.Refresh(h.ctx, at)
	coldSnapshot := cold.Page(at, "", "", "", 0, 20)
	if !coldSnapshot.Complete || len(coldSnapshot.Rows) != 2 || coldSnapshot.ManifestsFromIndex != 0 {
		t.Fatalf("cold directory = %+v, want complete, 2 rows, no manifest from an index it does not have", coldSnapshot)
	}
	if coldSpy.reads[manifestKey] != 1 || coldSpy.reads[activationKey] != 0 {
		t.Fatalf("cold directory read the manifest %d times (want 1) and the activation body %d times (want 0)",
			coldSpy.reads[manifestKey], coldSpy.reads[activationKey])
	}
	// The two agree on what they projected.
	if len(warmSnapshot.Rows) != len(coldSnapshot.Rows) || warmSnapshot.Revision != coldSnapshot.Revision {
		t.Fatalf("warm and cold projections differ: %+v vs %+v", warmSnapshot.Rows, coldSnapshot.Rows)
	}
	for i := range warmSnapshot.Rows {
		if warmSnapshot.Rows[i].Identity != coldSnapshot.Rows[i].Identity || warmSnapshot.Rows[i].QueryGroup != coldSnapshot.Rows[i].QueryGroup {
			t.Fatalf("row %d differs: %+v vs %+v", i, warmSnapshot.Rows[i], coldSnapshot.Rows[i])
		}
	}
}

// revisionedCatalog is the two fixture strategies, each with a Python
// strategy_revision so a forced choice can be honoured, built under the given
// output protocol. Revisions are what make the frozen word differ from what
// the automatic rule would say: under auto a revisioned strategy publishes the
// trigger event, so a forced native or legacy choice freezes a word the rule
// would not have produced, and a read that reports the frozen word cannot be
// mistaken for one that re-derives it.
func revisionedCatalog(t *testing.T, outputProtocol string) controlplane.Catalog {
	t.Helper()
	documents := realThresholdDocuments(t)
	revisioned := func(document json.RawMessage, business int, space string) []byte {
		var decoded map[string]any
		if err := json.Unmarshal(withWireIdentity(t, document, "tenant-a", space), &decoded); err != nil {
			t.Fatal(err)
		}
		decoded["bk_biz_id"] = float64(business)
		decoded["strategy_revision"] = float64(7)
		payload, err := json.Marshal(decoded)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	planner := &businessPlanner{facts: map[string]execution.QueryPlanFacts{"2": queryFactsFor(t, "2", "bkcc__2"), "3": queryFactsFor(t, "3", "bkcc__3")}}
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{Strategies: []controlplane.SourceStrategy{
		{SourceID: "1001", Document: revisioned(documents[0], 2, "bkcc__2"), Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}},
		{SourceID: "1002", Document: revisioned(documents[1], 3, "bkcc__3"), Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "3", SpaceScope: "bkcc__3"}},
	}, Planner: planner, OutputProtocol: outputProtocol})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.QueryGroups) != 2 {
		t.Fatalf("expected two Query Groups under %q, got %d with dispositions %+v", outputProtocol, len(catalog.QueryGroups), catalog.Dispositions)
	}
	return catalog
}

// The strategy-level output read reports the word the control leader froze
// with the Plan, not what the deployment's choice would resolve to now: a
// deployment that built under native and then reads under auto must be told
// standard_raw_event, because that is what the sink writes for this Plan. The
// read has to work both where the directory rode the runtime's index (the
// executing replica, whose remembered content names the output contexts) and
// where it read the manifest from the store (a cold or follower replica), and
// has to name its gap when a row carries no digest or the object is gone.
func TestObservationDirectoryReadsTheFrozenOutputFormatNotTheCurrentChoice(t *testing.T) {
	for name, test := range map[string]struct {
		protocol   string
		wireFormat string
		compat     bool
	}{
		"native freezes the standard raw event":     {protocol: "native", wireFormat: contract.WireFormatStandardRawEvent},
		"legacy freezes compatibility with context": {protocol: "legacy", wireFormat: contract.WireFormatPythonCompatible, compat: true},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			h := newObjectCatalogHarness(t)
			pub := h.publish(t, catalogWithSchedule(t, revisionedCatalog(t, test.protocol), 60, 0))
			if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, pub.Publication); err != nil {
				t.Fatal(err)
			}
			at := time.Unix(1000, 0)
			limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
			// What the automatic rule would say for a revisioned strategy, which
			// is what a read that re-derived the format would report.
			byRule, _ := controlplane.EffectiveWireFormat("", 7)
			if byRule == test.wireFormat {
				t.Fatalf("fixture cannot discriminate: the rule and the frozen word agree on %q", byRule)
			}
			check := func(t *testing.T, label string, d *controlplane.ObservationDirectory, wantFromIndex int) {
				t.Helper()
				d.Refresh(h.ctx, at)
				s := d.Page(at, "", "", "", 0, 20)
				if !s.Complete || len(s.Rows) != 2 || s.ManifestsFromIndex != wantFromIndex {
					t.Fatalf("%s directory = complete %v, rows %d, manifests from index %d; want complete, 2, %d: %+v", label, s.Complete, len(s.Rows), s.ManifestsFromIndex, wantFromIndex, s)
				}
				for _, row := range s.Rows {
					if row.OutputContext == "" {
						t.Fatalf("%s row %+v carries no output context digest", label, row.Identity)
					}
					selected, err := d.ResolveCurrent(at, row.Identity.TenantID, row.Identity.BusinessID, row.Identity.StrategyID, string(row.QueryGroup))
					if err != nil {
						t.Fatal(err)
					}
					facts := d.EffectiveOutput(h.ctx, selected)
					if !facts.Known || facts.Reason != "" {
						t.Fatalf("%s output for %s = %+v, want known", label, row.Identity.StrategyID, facts)
					}
					if facts.WireFormat != test.wireFormat || facts.EffectiveWireFormat != test.wireFormat || facts.DecidedBy != controlplane.WireFormatDecidedFrozen {
						t.Fatalf("%s output for %s = %+v, want frozen %q", label, row.Identity.StrategyID, facts, test.wireFormat)
					}
					if facts.EffectiveWireFormat == byRule {
						t.Fatalf("%s output reports the rule's %q, not the frozen word", label, byRule)
					}
					if facts.SnapshotRevision != 7 || facts.CompatibilityContext != test.compat || facts.OutputContextDigest != row.OutputContext {
						t.Fatalf("%s output facts = %+v, want revision 7, compatibility context %t, the row's digest", label, facts, test.compat)
					}
				}
			}
			// Warm: the executing replica. Its index stands in for the manifest
			// and its remembered content names the output contexts.
			if _, err := h.repository.LoadPublishedContent(h.ctx, pub.Publication); err != nil {
				t.Fatal(err)
			}
			warm, err := controlplane.NewObservationDirectory(h.repository, limits)
			if err != nil {
				t.Fatal(err)
			}
			check(t, "warm", warm, 1)
			// Cold: a replica that loaded nothing reads the manifest, which
			// names them itself.
			cold, err := controlplane.NewObservationDirectory(h.newRepository(t), limits)
			if err != nil {
				t.Fatal(err)
			}
			check(t, "cold", cold, 0)

			// The gaps have names. A row without a digest is not retained, not
			// unavailable; an object deleted under a row is unavailable, not
			// corrupt; and neither is answered by reading a manifest.
			row := cold.Page(at, "", "", "", 0, 20).Rows[0]
			selected, err := cold.ResolveCurrent(at, row.Identity.TenantID, row.Identity.BusinessID, row.Identity.StrategyID, string(row.QueryGroup))
			if err != nil {
				t.Fatal(err)
			}
			unretained := selected
			unretained.OutputContext = ""
			if facts := cold.EffectiveOutput(h.ctx, unretained); facts.Known || facts.Reason != controlplane.OutputContextRefNotRetained {
				t.Fatalf("row without a digest = %+v, want %s", facts, controlplane.OutputContextRefNotRetained)
			}
			// The executing replica keeps a bounded object cache, as the bundle
			// configures one, and has read this object the way the runtime does
			// when it loads the activation's content for rendering; so it is in
			// that process's cache.
			if err := h.repository.ConfigureObjectCache(64, 1<<20); err != nil {
				t.Fatal(err)
			}
			if _, err := h.repository.LoadOutputContext(h.ctx, row.OutputContext); err != nil {
				t.Fatal(err)
			}
			h.client.Del(h.ctx, h.prefix+":outctx:"+string(row.OutputContext))
			if facts := cold.EffectiveOutput(h.ctx, selected); facts.Known || facts.Reason != controlplane.OutputContextUnavailable {
				t.Fatalf("deleted object = %+v, want %s", facts, controlplane.OutputContextUnavailable)
			}
			// The warm replica still answers from its own cache, which is the
			// point of asking the cache first: the replica that renders this
			// Plan has the object and pays nothing.
			if facts := warm.EffectiveOutput(h.ctx, selected); !facts.Known || facts.EffectiveWireFormat != test.wireFormat {
				t.Fatalf("warm replica after the store lost the object = %+v, want known from the process cache", facts)
			}

			// Over HTTP, beside the configuration, under the same include.
			api := fleet.WithStrategyDirectory(http.NotFoundHandler(), warm, func() time.Time { return at })
			w := httptest.NewRecorder()
			api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&tenant=tenant-a&business=2&strategy=1001&include=effective_config", nil))
			if w.Code != 200 {
				t.Fatalf("config request = %d %s", w.Code, w.Body.String())
			}
			var body struct {
				EffectiveConfig json.RawMessage                 `json:"effective_config"`
				EffectiveOutput *controlplane.OutputFormatFacts `json:"effective_output"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.EffectiveConfig) == 0 || body.EffectiveOutput == nil || !body.EffectiveOutput.Known || body.EffectiveOutput.EffectiveWireFormat != test.wireFormat {
				t.Fatalf("response = %s, want the configuration and a known effective output of %q", w.Body.String(), test.wireFormat)
			}
			// And without the include, neither travels: the list stays the list.
			w = httptest.NewRecorder()
			api.ServeHTTP(w, httptest.NewRequest("GET", "/api/objects?scope=strategies&tenant=tenant-a&business=2&strategy=1001", nil))
			if w.Code != 200 || strings.Contains(w.Body.String(), `"effective_output"`) {
				t.Fatalf("list response = %d %s, want no effective_output without the include", w.Code, w.Body.String())
			}
		})
	}
}
