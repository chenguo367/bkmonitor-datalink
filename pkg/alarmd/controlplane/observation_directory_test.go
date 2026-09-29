package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
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
	h, d, at := directoryFixture(t, 4)
	// Latest publication, the manifest's length and bytes, then exactly one
	// cold object; the activation comes through the repository's own cache
	// and is not one of the directory's commands. A later tick reuses that
	// identity projection and the manifest it walked.
	d.Refresh(h.ctx, at)
	first := d.Page(at, "", "", "", 0, 20)
	if first.Complete || first.ReadCommands > 4 || len(first.Rows) != 1 {
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
// deployment that built under a forced choice and then reads under auto must
// be told the forced word, because that is what the sink writes for this
// Plan. The read has to work both where the directory rode the runtime's
// index (the executing replica, whose remembered content names the output
// contexts) and where it read the manifest from the store (a cold or follower
// replica), and has to name its gap when a row carries no digest or the
// object is gone.
//
// The table's two forced choices are the discriminator: a read that
// re-derived the format from the automatic rule would report the rule's word,
// so at least one frozen word here must differ from it. Which one differs
// depends on what the rule resolves an unset choice to, and that is the
// rule's own contract, tested where it lives; this test asks the rule rather
// than assuming its answer, and requires only that the table still tells the
// two reads apart.
func TestObservationDirectoryReadsTheFrozenOutputFormatNotTheCurrentChoice(t *testing.T) {
	// What the automatic rule says for a revisioned strategy, which is what a
	// read that re-derived the format would report.
	byRule, _ := controlplane.EffectiveWireFormat("", 7)
	table := map[string]struct {
		protocol   string
		wireFormat string
		compat     bool
	}{
		// Only the legacy word can discriminate here: since auto resolves a
		// revisioned strategy to the standard raw event, a native word agrees
		// with the rule and a read that re-derived the format would report
		// the same thing. Legacy is the word the rule would never produce
		// for a revisioned strategy, so it is the case that tells the two
		// reads apart.
		"legacy freezes compatibility with context": {protocol: "legacy", wireFormat: contract.WireFormatPythonCompatible, compat: true},
	}
	discriminating := 0
	for _, test := range table {
		if test.wireFormat != byRule {
			discriminating++
		}
	}
	if discriminating == 0 {
		t.Fatalf("no fixture discriminates: the rule and every frozen word agree on %q", byRule)
	}
	for name, test := range table {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			h := newObjectCatalogHarness(t)
			pub := h.publish(t, catalogWithSchedule(t, revisionedCatalog(t, test.protocol), 60, 0))
			if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, pub.Publication); err != nil {
				t.Fatal(err)
			}
			at := time.Unix(1000, 0)
			limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
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

// A deployment carries output contexts frozen under the earlier rule, whose
// word is trigger_event_v1: no new Plan selects it, nothing rewrote the
// objects, and the sink resolves it to the standard raw event. The read has
// to say both -- the word that is in the object and the format that is
// written -- and name the decision, because a read that reported the word as
// the format would tell an operator the sink writes something it does not.
//
// The object is a real one from a publication with its word changed and
// re-addressed by its own digest, which is the shape of an object the earlier
// rule wrote: same contract version, same identity, same everything but the
// word.
func TestObservationDirectoryReportsAHistoricalFrozenWordBesideWhatIsWritten(t *testing.T) {
	h := newObjectCatalogHarness(t)
	pub := h.publish(t, catalogWithSchedule(t, revisionedCatalog(t, ""), 60, 0))
	if _, err := indexReconciler(t, h.repository, sharedClock()).Ensure(h.ctx, pub.Publication); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1000, 0)
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	row := d.Page(at, "", "", "", 0, 20).Rows[0]
	selected, err := d.ResolveCurrent(at, row.Identity.TenantID, row.Identity.BusinessID, row.Identity.StrategyID, string(row.QueryGroup))
	if err != nil {
		t.Fatal(err)
	}
	current := d.EffectiveOutput(h.ctx, selected)
	if !current.Known || current.WireFormat != contract.WireFormatStandardRawEvent || current.DecidedBy != controlplane.WireFormatDecidedFrozen {
		t.Fatalf("a Plan built now = %+v, want the standard raw event, frozen", current)
	}
	// The same object as the earlier rule wrote it.
	payload, err := h.client.Get(h.ctx, h.prefix+":outctx:"+string(row.OutputContext)).Bytes()
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
	if err = h.client.Set(h.ctx, h.prefix+":outctx:"+digest, encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	selected.OutputContext = execution.OutputContextDigest(digest)
	facts := d.EffectiveOutput(h.ctx, selected)
	if !facts.Known {
		t.Fatalf("historical object = %+v, want known", facts)
	}
	if facts.WireFormat != contract.WireFormatTriggerEvent {
		t.Errorf("wire_format = %q, want the word that is in the object, %q", facts.WireFormat, contract.WireFormatTriggerEvent)
	}
	if want := contract.ResolveOutputWireFormat(contract.WireFormatTriggerEvent, facts.SnapshotRevision); facts.EffectiveWireFormat != want {
		t.Errorf("effective_wire_format = %q, want what the sink writes, %q", facts.EffectiveWireFormat, want)
	}
	if facts.DecidedBy != controlplane.WireFormatDecidedHistorical {
		t.Errorf("decided_by = %q, want %s: the word and the format differ", facts.DecidedBy, controlplane.WireFormatDecidedHistorical)
	}
	if facts.EffectiveWireFormat == facts.WireFormat {
		t.Errorf("the historical read shows one word %q for both fields; the sink does not write that word", facts.WireFormat)
	}
}

// The composition counts the Plans that carry an authoritative strategy
// revision beside every Plan, from the frozen Plans themselves: a source
// whose documents publish no revision composes to zero revisioned, and one
// whose documents do composes to all of them. It is the number that decides
// whether the standard output path is reachable under the automatic choice,
// and it was established on a live deployment by a Kafka read instead.
func TestTheCompositionCountsRevisionedPlansFromTheFrozenPlans(t *testing.T) {
	without := controlplane.ComposeCatalog(objectCatalogTwoGroups(t, 80))
	if without.PlansTotal != 2 || without.RevisionedPlans != 0 {
		t.Fatalf("unrevisioned source = %d plans, %d revisioned; want 2 and 0", without.PlansTotal, without.RevisionedPlans)
	}
	with := controlplane.ComposeCatalog(revisionedCatalog(t, ""))
	if with.PlansTotal != 2 || with.RevisionedPlans != 2 {
		t.Fatalf("revisioned source = %d plans, %d revisioned; want 2 and 2", with.PlansTotal, with.RevisionedPlans)
	}
}

// The composition counts every Plan by the wire format its events go out
// as, resolved the way the sink resolves it, every format present at zero:
// an unrevisioned source composes to all Python-compatible and none standard,
// and a revisioned one to the reverse. The number that answers "how many
// strategies publish the standard raw event" is this one; before it the
// answer was a Kafka read, and a log search for the word found no line.
func TestTheCompositionCountsPlansByTheWireFormatTheSinkResolves(t *testing.T) {
	without := controlplane.ComposeCatalog(objectCatalogTwoGroups(t, 80))
	if got := without.PlansByWireFormat; got[contract.WireFormatPythonCompatible] != 2 || got[contract.WireFormatStandardRawEvent] != 0 ||
		got[observability.WireFormatOther] != 0 || len(got) != len(observability.WireFormats) {
		t.Fatalf("unrevisioned source by wire format = %v, want 2 python_compatible and every other format at zero", got)
	}
	with := controlplane.ComposeCatalog(revisionedCatalog(t, ""))
	if got := with.PlansByWireFormat; got[contract.WireFormatStandardRawEvent] != 2 || got[contract.WireFormatPythonCompatible] != 0 {
		t.Fatalf("revisioned source by wire format = %v, want 2 standard_raw_event and 0 python_compatible", got)
	}
	total := 0
	for _, count := range with.PlansByWireFormat {
		total += count
	}
	if total != with.PlansTotal {
		t.Fatalf("by-format counts sum to %d, want the %d Plans: the counts must partition", total, with.PlansTotal)
	}
	// Plans frozen before the word existed carry none, and one frozen under
	// the historical spelling carries a word the sink never writes: both are
	// counted under what the sink resolves them to, not under _other and not
	// under the historical word.
	historical := revisionedCatalog(t, "")
	for index := range historical.QueryGroups {
		for planIndex := range historical.QueryGroups[index].Plans {
			historical.QueryGroups[index].Plans[planIndex].Plan.WireFormat = contract.WireFormatTriggerEvent
		}
	}
	unworded := objectCatalogTwoGroups(t, 80)
	for index := range unworded.QueryGroups {
		for planIndex := range unworded.QueryGroups[index].Plans {
			unworded.QueryGroups[index].Plans[planIndex].Plan.WireFormat = ""
		}
	}
	if got := controlplane.ComposeCatalog(historical).PlansByWireFormat; got[contract.WireFormatStandardRawEvent] != 2 || got[observability.WireFormatOther] != 0 {
		t.Fatalf("historical word by wire format = %v, want 2 standard_raw_event and nothing under _other", got)
	}
	if got := controlplane.ComposeCatalog(unworded).PlansByWireFormat; got[contract.WireFormatPythonCompatible] != 2 || got[observability.WireFormatOther] != 0 {
		t.Fatalf("no word, no revision by wire format = %v, want 2 python_compatible and nothing under _other", got)
	}
}

// failingReadSpy fails every bounded read of a key containing fail with err,
// or, when err is nil, with a word numbered by the order of the failures.
type failingReadSpy struct {
	redis.Cmdable
	fail   string
	err    error
	failed int
}

func (s *failingReadSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	if strings.Contains(key, s.fail) {
		s.failed++
		if s.err == nil {
			return redis.NewStringResult("", fmt.Errorf("failure number %d", s.failed))
		}
		return redis.NewStringResult("", s.err)
	}
	return s.Cmdable.GetRange(ctx, key, start, end)
}

// readOrderSpy records the keys a directory refresh reads, in order, a key
// once for each command on it - a manifest's length, then its bytes.
type readOrderSpy struct {
	redis.Cmdable
	keys []string
}

func (s *readOrderSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	s.keys = append(s.keys, key)
	return s.Cmdable.GetRange(ctx, key, start, end)
}

func (s *readOrderSpy) StrLen(ctx context.Context, key string) *redis.IntCmd {
	s.keys = append(s.keys, key)
	return s.Cmdable.StrLen(ctx, key)
}

// A refresh that fails says which step failed and in its own words, not only
// that a dependency did: a cold replica whose manifest read failed on every
// refresh reported DEPENDENCY_UNAVAILABLE, and the cost ranking fed from the
// directory tracked nothing, with a timeout, a missing key and a body that
// did not decode all reading the same. The first failure is kept: the walk
// goes on past a group it could not read, and a later word would hide why.
func TestADirectoryRefreshThatFailsSaysWhichReadAndWhy(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	spy := &failingReadSpy{Cmdable: h.client, fail: ":manifest:", err: errors.New("read tcp 127.0.0.1:6379: i/o timeout")}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	if s.Complete || s.Reason != "DEPENDENCY_UNAVAILABLE" || s.FailedRead != "manifest" || !strings.Contains(s.Error, "i/o timeout") {
		t.Fatalf("manifest failure = complete %v reason %q read %q error %q", s.Complete, s.Reason, s.FailedRead, s.Error)
	}

	// Every group object would fail: the first failure stops the refresh's
	// reads, so it is the only one - kept, in its own words - and the group
	// after it is unread.
	groups := &failingReadSpy{Cmdable: h.client, fail: ":qgobj:"}
	walked, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, groups)
	if err != nil {
		t.Fatal(err)
	}
	walked.Refresh(h.ctx, at)
	if s := walked.Page(at, "", "", "", 0, 20); groups.failed != 1 || s.FailedRead != "group_object" || !strings.Contains(s.Error, "failure number 1") || s.GroupsUnread != 1 {
		t.Fatalf("group failures = %d, read %q error %q unread %d, want the one failure and the other group unread", groups.failed, s.FailedRead, s.Error, s.GroupsUnread)
	}

	ctx, cancel := context.WithCancel(h.ctx)
	cancel()
	cold, err := controlplane.NewObservationDirectory(h.newRepository(t), limits)
	if err != nil {
		t.Fatal(err)
	}
	cold.Refresh(ctx, at)
	if s := cold.Page(at, "", "", "", 0, 20); s.FailedRead != "latest_publication" || !strings.Contains(s.Error, "context canceled") {
		t.Fatalf("cancelled refresh = read %q error %q, want the first read and its words", s.FailedRead, s.Error)
	}

	tight, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 8, Commands: 2, Entries: 2, Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	tight.Refresh(h.ctx, at)
	if s := tight.Page(at, "", "", "", 0, 20); s.Reason != "RESOURCE_BUDGET" || s.FailedRead == "" || s.Error == "" {
		t.Fatalf("budget = reason %q read %q error %q, want the step it ran out on", s.Reason, s.FailedRead, s.Error)
	}

	healthy, _ := controlplane.NewObservationDirectory(h.newRepository(t), limits)
	healthy.Refresh(h.ctx, at)
	if s := healthy.Page(at, "", "", "", 0, 20); !s.Complete || s.FailedRead != "" || s.Error != "" {
		t.Fatalf("a complete refresh carries a failure: read %q error %q", s.FailedRead, s.Error)
	}
}

// A publication an active Plan is still carried on keeps its objects renewed
// and not its manifest, so past the catalog's retention its manifest is gone
// while its Plans run. The directory stopped at the first missing manifest,
// and on a deployment whose activation carried such a publication it was
// empty on every replica every refresh. Both sides: a carried publication's
// manifest missing is named, its Plans are rows marked as such with nothing
// the manifest would have said, and the latest publication's rows are built;
// the latest publication's own manifest missing still fails the refresh, by
// its key.
func TestADirectoryNamesACarriedPublicationWhoseManifestIsGoneAndGoesOn(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	for _, side := range []string{"carried", "latest", "carried read failed", "carried out of allowance"} {
		t.Run(side, func(t *testing.T) {
			h, baseline, at := directoryFixture(t, 32)
			baseline.Refresh(h.ctx, at)
			carried := baseline.Page(at, "", "", "", 0, 20).Published
			manifest, err := h.repository.LoadCatalogManifest(h.ctx, carried.SnapshotRevision)
			if err != nil {
				t.Fatal(err)
			}
			// A later publication of the same groups; the activation stays on
			// the first, as it does for Plans that have not cut over.
			manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
			payload, _ := json.Marshal(manifest)
			latestKey := h.prefix + ":manifest:" + string(manifest.SnapshotRevision)
			carriedKey := h.prefix + ":manifest:" + string(carried.SnapshotRevision)
			if err = h.client.Set(h.ctx, latestKey, payload, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err = h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0).Err(); err != nil {
				t.Fatal(err)
			}
			var reads []redis.Cmdable
			if side == "carried read failed" {
				// The carried manifest is there and its read fails: that is
				// not a manifest past its retention, and it fails the refresh.
				reads = append(reads, &failingReadSpy{Cmdable: h.client, fail: carriedKey, err: errors.New("read tcp 127.0.0.1:6379: i/o timeout")})
			} else if side != "carried out of allowance" {
				if err = h.client.Del(h.ctx, map[string]string{"carried": carriedKey, "latest": latestKey}[side]).Err(); err != nil {
					t.Fatal(err)
				}
			}
			refreshLimits := limits
			if side == "carried out of allowance" {
				// The carried manifest is there and the refresh spends its
				// allowance before reaching it, as a restarted follower does on
				// the groups it has not read yet. The allowance is the reads a
				// refresh with room makes before the carried manifest, counted
				// with the group objects already cached, as they are for the
				// refresh under test.
				roomy := limits
				roomy.Commands = 1000
				var order *readOrderSpy
				for range 2 {
					order = &readOrderSpy{Cmdable: h.client}
					counted, err := controlplane.NewObservationDirectory(h.newRepository(t), roomy, order)
					if err != nil {
						t.Fatal(err)
					}
					counted.Refresh(h.ctx, at)
				}
				refreshLimits.Commands = slices.Index(order.keys, carriedKey)
				if refreshLimits.Commands <= 0 {
					t.Fatalf("setup: reads %v, want the carried manifest read after at least one other", order.keys)
				}
			}
			d, err := controlplane.NewObservationDirectory(h.newRepository(t), refreshLimits, reads...)
			if err != nil {
				t.Fatal(err)
			}
			d.Refresh(h.ctx, at)
			s := d.Page(at, "", "", "", 0, 100)
			if side == "carried read failed" {
				if s.Complete || s.FailedRead != "manifest" || s.FailedKey != carriedKey || s.FailedPublication == nil || *s.FailedPublication != carried {
					t.Fatalf("carried read failed = complete %v read %q key %q publication %+v, want the refresh failed on the carried manifest by name",
						s.Complete, s.FailedRead, s.FailedKey, s.FailedPublication)
				}
				for _, row := range s.Rows {
					if row.ManifestExpired {
						t.Fatalf("row %+v marked expired for a manifest whose read failed", row)
					}
				}
				last := s.Publications[len(s.Publications)-1]
				if last.Publication != carried || last.Manifest != "failed" {
					t.Fatalf("publications = %+v, want the carried one marked failed, not expired", s.Publications)
				}
				return
			}
			if side == "carried out of allowance" {
				last := s.Publications[len(s.Publications)-1]
				if s.Complete || s.FailedRead != "manifest" || s.FailedKey != carriedKey || last.Publication != carried || last.Manifest != "unread" {
					t.Fatalf("carried out of allowance (%d of %d reads) = complete %v read %q key %q publications %+v, want the carried manifest named unread, not failed",
						s.ReadCommands, refreshLimits.Commands, s.Complete, s.FailedRead, s.FailedKey, s.Publications)
				}
				return
			}
			if side == "latest" {
				latest := controlplane.SnapshotPublicationRef{SnapshotRevision: manifest.SnapshotRevision, PublicationEpoch: 2}
				if s.Complete || s.FailedRead != "manifest" || s.FailedKey != latestKey || s.FailedPublication == nil || *s.FailedPublication != latest {
					t.Fatalf("latest manifest gone = complete %v read %q key %q publication %+v, want the refresh failed on it by name",
						s.Complete, s.FailedRead, s.FailedKey, s.FailedPublication)
				}
				return
			}
			if !s.Complete || s.FailedRead != "" {
				t.Fatalf("carried manifest gone = complete %v read %q error %q, want the refresh to go on", s.Complete, s.FailedRead, s.Error)
			}
			var published, expired int
			for _, row := range s.Rows {
				switch {
				case row.ManifestExpired:
					expired++
					if row.Publication != carried || row.QueryGroup != "" || row.ObjectDigest != "" || row.Activation == nil ||
						row.Role != string(execution.ActivationCurrent) {
						t.Fatalf("expired row = %+v, want the carried Plan named by its activation and nothing guessed", row)
					}
				case row.Publication.SnapshotRevision == manifest.SnapshotRevision && row.QueryGroup != "":
					published++
				}
			}
			if published == 0 || expired == 0 {
				t.Fatalf("rows = %+v, want the latest publication's rows and the carried Plans named", s.Rows)
			}
			var names []string
			for _, read := range s.Publications {
				names = append(names, fmt.Sprintf("%s:%s:%d", read.Publication.SnapshotRevision[:1], read.Manifest, read.Plans))
			}
			if want := []string{"e:store:0", string(carried.SnapshotRevision[:1]) + fmt.Sprintf(":expired:%d", expired)}; strings.Join(names, ",") != strings.Join(want, ",") {
				t.Fatalf("publications = %v, want %v", names, want)
			}
		})
	}
}

// stoppingSpy records every key a directory refresh reads, in order, and
// answers a read of a key containing part (a group object when empty) with
// fail's answer for it - nil passes the read through to the store.
type stoppingSpy struct {
	redis.Cmdable
	part string
	fail func(ctx context.Context, n int) *redis.StringCmd
	keys []string
	n    int
}

func (s *stoppingSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	s.keys = append(s.keys, key)
	part := s.part
	if part == "" {
		part = ":qgobj:"
	}
	if strings.Contains(key, part) && s.fail != nil {
		s.n++
		if cmd := s.fail(ctx, s.n); cmd != nil {
			return cmd
		}
	}
	return s.Cmdable.GetRange(ctx, key, start, end)
}

func (s *stoppingSpy) StrLen(ctx context.Context, key string) *redis.IntCmd {
	s.keys = append(s.keys, key)
	return s.Cmdable.StrLen(ctx, key)
}

func (s *stoppingSpy) read(part string) []string {
	var keys []string
	for _, key := range s.keys {
		if strings.Contains(key, part) {
			keys = append(keys, key)
		}
	}
	return keys
}

// A read that fails for the store stops the refresh's reads: one failure
// counted, the group after it unread and not tried, the audit not read. The
// next refresh starts after the group that failed and reads both. A refresh
// that runs out of its own time stops the same way, named by its budget - a
// read on a spent deadline fails at once and says no Sentinel answered, and
// every later one would too. A group whose key is missing is that group's
// and the walk goes on past it.
func TestADirectoryRefreshStopsReadingAtAFailureAndResumesAfterIt(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	refused := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	spy := &stoppingSpy{Cmdable: h.client, fail: func(_ context.Context, n int) *redis.StringCmd {
		if n == 1 {
			return redis.NewStringResult("", refused)
		}
		return nil
	}}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	first := d.Page(at, "", "", "", 0, 20)
	failedGroup := spy.read(":qgobj:")
	if len(failedGroup) != 1 || len(spy.read(":latest_audit")) != 0 || first.Complete || first.Reason != "DEPENDENCY_UNAVAILABLE" ||
		first.FailedRead != "group_object" || first.GroupsUnread != 1 || first.SourceReason != "SOURCE_AUDIT_UNAVAILABLE" {
		t.Fatalf("a refresh whose group read was refused = reads %v, %+v; want one group read, no audit read, the other unread", spy.keys, first)
	}

	spy.keys = nil
	d.Refresh(h.ctx, at.Add(time.Second))
	second := d.Page(at.Add(time.Second), "", "", "", 0, 20)
	groupsRead := spy.read(":qgobj:")
	if !second.Complete || len(second.Rows) != 2 || second.GroupsUnread != 0 || len(groupsRead) != 2 || groupsRead[0] == failedGroup[0] ||
		groupsRead[1] != failedGroup[0] {
		t.Fatalf("the next refresh = reads %v, %+v; want both groups, the one that failed last", groupsRead, second)
	}

	late, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100,
		Timeout: 50 * time.Millisecond, FreshFor: time.Minute}, &stoppingSpy{Cmdable: h.client, fail: func(ctx context.Context, _ int) *redis.StringCmd {
		<-ctx.Done()
		return redis.NewStringResult("", errors.New("redis: all sentinels specified in configuration are unreachable"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	late.Refresh(h.ctx, at)
	if s := late.Page(at, "", "", "", 0, 20); s.Reason != "RESOURCE_BUDGET" || s.FailedRead != "group_object" || s.GroupsUnread != 1 || s.SourceReason != "RESOURCE_BUDGET" {
		t.Fatalf("a refresh out of time = %+v, want it named by its budget with the other group unread", s)
	}

	missing := &stoppingSpy{Cmdable: h.client, fail: func(_ context.Context, n int) *redis.StringCmd {
		if n == 1 {
			return redis.NewStringResult("", nil)
		}
		return nil
	}}
	gone, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, missing)
	if err != nil {
		t.Fatal(err)
	}
	gone.Refresh(h.ctx, at)
	if s := gone.Page(at, "", "", "", 0, 20); len(missing.read(":qgobj:")) != 2 || s.GroupsUnread != 0 || len(missing.read(":latest_audit")) != 1 || len(s.Rows) != 1 {
		t.Fatalf("a group whose key is missing = reads %v, %+v; want the walk to go on to the other group and the audit", missing.keys, s)
	}
}

// Once a refresh has stopped, nothing after it is read: a manifest read the
// store refused stops the audit read too, and a group read refused on the
// latest publication leaves a carried publication's manifest unread and
// listed so. What needs no read still counts: a group whose object is
// unchanged in a new publication makes its rows though the changed group
// before it could not be read.
func TestADirectoryRefreshThatStoppedReadsNothingAfterItAndKeepsWhatItKnows(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	refused := func(_ context.Context, _ int) *redis.StringCmd {
		return redis.NewStringResult("", errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"))
	}

	h, _, at := directoryFixture(t, 32)
	manifestSpy := &stoppingSpy{Cmdable: h.client, part: ":manifest:", fail: refused}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, manifestSpy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	if s := d.Page(at, "", "", "", 0, 20); len(manifestSpy.read(":latest_audit")) != 0 || s.FailedRead != "manifest" || s.Reason != "DEPENDENCY_UNAVAILABLE" {
		t.Fatalf("a refused manifest read = reads %v, %+v; want the refresh stopped there, the audit not read", manifestSpy.keys, s)
	}

	h, baseline, at := directoryFixture(t, 32)
	baseline.Refresh(h.ctx, at)
	carried := baseline.Page(at, "", "", "", 0, 20).Published
	manifest, err := h.repository.LoadCatalogManifest(h.ctx, carried.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
	payload, _ := json.Marshal(manifest)
	carriedKey := h.prefix + ":manifest:" + string(carried.SnapshotRevision)
	if err = h.client.Set(h.ctx, h.prefix+":manifest:"+string(manifest.SnapshotRevision), payload, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err = h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0).Err(); err != nil {
		t.Fatal(err)
	}
	groupSpy := &stoppingSpy{Cmdable: h.client, fail: refused}
	two, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, groupSpy)
	if err != nil {
		t.Fatal(err)
	}
	two.Refresh(h.ctx, at)
	s := two.Page(at, "", "", "", 0, 20)
	listed := false
	for _, publication := range s.Publications {
		listed = listed || (publication.Publication == carried && publication.Manifest == "unread")
	}
	if slices.Contains(groupSpy.keys, carriedKey) || !listed || len(groupSpy.read(":qgobj:")) != 1 {
		t.Fatalf("a refused group read on the latest publication = reads %v, publications %+v; want the carried manifest unread and listed", groupSpy.keys, s.Publications)
	}

	// Each group changed in turn, so that in one of the two the changed group
	// is walked before the unchanged one, whichever sorts first.
	for name, next := range map[string]func() controlplane.Catalog{
		"first document changed":  func() controlplane.Catalog { return objectCatalogTwoGroups(t, 90) },
		"second document changed": func() controlplane.Catalog { return objectCatalogTwoGroupsChangingSecond(t, 95) },
	} {
		h, _, at := directoryFixture(t, 32)
		known := &stoppingSpy{Cmdable: h.client}
		kept, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, known)
		if err != nil {
			t.Fatal(err)
		}
		kept.Refresh(h.ctx, at)
		if first := kept.Page(at, "", "", "", 0, 20); !first.Complete || len(first.Rows) != 2 {
			t.Fatalf("%s setup: %+v, want both groups known", name, first)
		}
		h.publish(t, catalogWithSchedule(t, next(), 60, 0))
		known.fail = refused
		known.keys = nil
		kept.Refresh(h.ctx, at.Add(time.Second))
		s := kept.Page(at.Add(time.Second), "", "", "", 0, 20)
		latest := 0
		for _, row := range s.Rows {
			if row.Publication == s.Published {
				latest++
			}
		}
		if len(known.read(":qgobj:")) != 1 || latest != 1 || s.GroupsUnread != 0 {
			t.Fatalf("%s, its group unread = reads %v, %d rows of the new publication, %d unread; want the unchanged group's row", name, known.keys, latest, s.GroupsUnread)
		}
	}
}

// objectCatalogTwoGroupsChangingSecond is objectCatalogTwoGroups with the
// second strategy's threshold changed instead of the first's.
func objectCatalogTwoGroupsChangingSecond(t *testing.T, thresholdB int) controlplane.Catalog {
	t.Helper()
	documents := realThresholdDocuments(t)
	second := string(withWireIdentity(t, documents[1], "tenant-a", "bkcc__3"))
	if strings.Count(second, `"threshold":90`) != 1 {
		t.Fatalf("setup: the second document's threshold is not 90 once: %s", second)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(strings.Replace(second, `"threshold":90`, fmt.Sprintf(`"threshold":%d`, thresholdB), 1)), &decoded); err != nil {
		t.Fatal(err)
	}
	decoded["bk_biz_id"] = float64(3)
	documentB, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	planner := &businessPlanner{facts: map[string]execution.QueryPlanFacts{"2": queryFactsFor(t, "2", "bkcc__2"), "3": queryFactsFor(t, "3", "bkcc__3")}}
	catalog, err := controlplane.BuildCatalog(context.Background(), controlplane.BuildRequest{Strategies: []controlplane.SourceStrategy{
		{SourceID: "1001", Document: documents[0], Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}},
		{SourceID: "1002", Document: documentB, Identity: controlplane.SourceIdentity{TenantID: "tenant-a", BusinessID: "3", SpaceScope: "bkcc__3"}},
	}, Planner: planner})
	if err != nil || len(catalog.QueryGroups) != 2 {
		t.Fatalf("setup: catalog %d groups, %v", len(catalog.QueryGroups), err)
	}
	return catalog
}

// manifestReadSpy records the length and byte reads a directory refresh makes
// of manifest keys.
type manifestReadSpy struct {
	redis.Cmdable
	lengths, reads []string
}

func (s *manifestReadSpy) StrLen(ctx context.Context, key string) *redis.IntCmd {
	if strings.Contains(key, ":manifest:") {
		s.lengths = append(s.lengths, key)
	}
	return s.Cmdable.StrLen(ctx, key)
}

func (s *manifestReadSpy) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	if strings.Contains(key, ":manifest:") {
		s.reads = append(s.reads, key)
	}
	return s.Cmdable.GetRange(ctx, key, start, end)
}

func (s *manifestReadSpy) of(revision execution.SnapshotRevision) (lengths, reads int) {
	for _, key := range s.lengths {
		if strings.HasSuffix(key, ":"+string(revision)) {
			lengths++
		}
	}
	for _, key := range s.reads {
		if strings.HasSuffix(key, ":"+string(revision)) {
			reads++
		}
	}
	return lengths, reads
}

// A revision's manifest is read once: the refresh after walks it again with
// one length read asking the key still exists and no byte read, and a new
// revision is read. The Slot
// path's manifest cache is not written by the directory, and a revision a
// refresh does not walk is forgotten.
func TestADirectoryReadsARevisionsManifestOnce(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	spy := &manifestReadSpy{Cmdable: h.client}
	repository := h.newRepository(t)
	d, err := controlplane.NewObservationDirectory(repository, limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	first := d.Page(at, "", "", "", 0, 20)
	revision := first.Published.SnapshotRevision
	if lengths, reads := spy.of(revision); !first.Complete || lengths != 1 || reads != 1 ||
		!slices.Equal(d.RememberedManifestsForTest(), []execution.SnapshotRevision{revision}) {
		t.Fatalf("first refresh = %d length and %d byte reads, remembered %v, %+v; want one of each and the revision remembered",
			lengths, reads, d.RememberedManifestsForTest(), first)
	}

	spy.lengths, spy.reads = nil, nil
	d.Refresh(h.ctx, at.Add(time.Second))
	second := d.Page(at.Add(time.Second), "", "", "", 0, 20)
	if len(spy.lengths) != 1 || len(spy.reads) != 0 || !second.Complete || len(second.Rows) != 2 || second.ManifestsRemembered != 1 ||
		second.Publications[0].Manifest != "remembered" {
		t.Fatalf("the same revision again = lengths %v reads %v, %+v; want its key asked about and its bytes not read", spy.lengths, spy.reads, second)
	}
	// The rows it makes are the ones the read made, output contexts included.
	contextsOf := func(rows []controlplane.StrategyDirectoryRow) string {
		var contexts []string
		for _, row := range rows {
			contexts = append(contexts, string(row.QueryGroup)+"="+string(row.OutputContext))
		}
		return strings.Join(contexts, ",")
	}
	if contextsOf(first.Rows) != contextsOf(second.Rows) || first.Rows[0].OutputContext == "" {
		t.Fatalf("rows walked from the remembered manifest = %s, want %s as read", contextsOf(second.Rows), contextsOf(first.Rows))
	}

	h.publish(t, catalogWithSchedule(t, objectCatalogTwoGroups(t, 90), 60, 0))
	spy.lengths, spy.reads = nil, nil
	d.Refresh(h.ctx, at.Add(2*time.Second))
	third := d.Page(at.Add(2*time.Second), "", "", "", 0, 20)
	next := third.Published.SnapshotRevision
	if lengths, reads := spy.of(next); next == revision || lengths != 1 || reads != 1 || !slices.Contains(d.RememberedManifestsForTest(), next) {
		t.Fatalf("a new revision = %s read %d/%d, remembered %v; want it read and remembered", next, lengths, reads, d.RememberedManifestsForTest())
	}
	for _, r := range []execution.SnapshotRevision{revision, next} {
		if _, cached := repository.SlotManifestForTest(r); cached {
			t.Fatalf("the directory put %s in the Slot path's manifest cache", r)
		}
	}

	stale := execution.SnapshotRevision(strings.Repeat("f", 64))
	d.RememberManifestForTest(stale)
	d.Refresh(h.ctx, at.Add(3*time.Second))
	if slices.Contains(d.RememberedManifestsForTest(), stale) {
		t.Fatalf("remembered %v after a refresh that did not walk %s", d.RememberedManifestsForTest(), stale)
	}
}

// A manifest is sized before it is read: a carried publication whose manifest
// key is gone has a length of zero and is named expired with no bytes read,
// and a manifest longer than the refresh has left is unread with none of it
// sent.
func TestADirectorySizesAManifestBeforeReadingIt(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	h, baseline, at := directoryFixture(t, 32)
	baseline.Refresh(h.ctx, at)
	carried := baseline.Page(at, "", "", "", 0, 20).Published
	manifest, err := h.repository.LoadCatalogManifest(h.ctx, carried.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
	payload, _ := json.Marshal(manifest)
	if err = h.client.Set(h.ctx, h.prefix+":manifest:"+string(manifest.SnapshotRevision), payload, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err = h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err = h.client.Del(h.ctx, h.prefix+":manifest:"+string(carried.SnapshotRevision)).Err(); err != nil {
		t.Fatal(err)
	}
	spy := &manifestReadSpy{Cmdable: h.client}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	expired := false
	for _, publication := range d.Page(at, "", "", "", 0, 20).Publications {
		expired = expired || (publication.Publication == carried && publication.Manifest == "expired")
	}
	if lengths, reads := spy.of(carried.SnapshotRevision); lengths != 1 || reads != 0 || !expired {
		t.Fatalf("a carried manifest gone = %d length and %d byte reads, expired %v; want its length read, zero, and no bytes", lengths, reads, expired)
	}

	h, _, at = directoryFixture(t, 32)
	latest, err := h.client.Get(h.ctx, h.prefix+":latest_publication").Result()
	if err != nil {
		t.Fatal(err)
	}
	small := &manifestReadSpy{Cmdable: h.client}
	tight, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: len(latest) + 16, Commands: 32,
		Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, small)
	if err != nil {
		t.Fatal(err)
	}
	tight.Refresh(h.ctx, at)
	s := tight.Page(at, "", "", "", 0, 20)
	if len(small.lengths) != 1 || len(small.reads) != 0 || len(s.Publications) != 1 || s.Publications[0].Manifest != "unread" || s.ReadBytes != len(latest) ||
		s.Reason != "RESOURCE_BUDGET" {
		t.Fatalf("a manifest longer than the allowance left = lengths %v reads %v, %+v; want it unread with no bytes of it sent", small.lengths, small.reads, s)
	}
}

// The latest publication's manifest in the Slot path's cache is walked with
// its key asked about and its bytes not read, and left as it was: in the order the cache holds it, which
// every Slot reading it sees. Refreshes reading it while Slots read it race
// with nothing (run under -race).
func TestADirectoryWalksTheSlotCachesManifestWithoutReadingOrReorderingIt(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	h, baseline, at := directoryFixture(t, 32)
	baseline.Refresh(h.ctx, at)
	revision := baseline.Page(at, "", "", "", 0, 20).Published.SnapshotRevision
	manifest, err := h.repository.LoadCatalogManifest(h.ctx, revision)
	if err != nil {
		t.Fatal(err)
	}
	// Held in the order a directory would change: descending.
	sort.Slice(manifest.QueryGroups, func(i, j int) bool { return manifest.QueryGroups[i].QueryGroup > manifest.QueryGroups[j].QueryGroup })
	order := func(m controlplane.CatalogManifest) string {
		var groups []string
		for _, group := range m.QueryGroups {
			groups = append(groups, string(group.QueryGroup))
		}
		return strings.Join(groups, ",")
	}
	held := order(manifest)
	repository := h.newRepository(t)
	repository.StoreSlotManifestForTest(manifest)

	spy := &manifestReadSpy{Cmdable: h.client}
	d, err := controlplane.NewObservationDirectory(repository, limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	s := d.Page(at, "", "", "", 0, 20)
	cached, _ := repository.SlotManifestForTest(revision)
	if len(spy.lengths) != 1 || len(spy.reads) != 0 || !s.Complete || len(s.Rows) != 2 || s.Publications[0].Manifest != "slot_cache" ||
		s.ManifestsRemembered != 1 || order(cached) != held {
		t.Fatalf("the Slot cache's manifest = lengths %v reads %v, cached order %s (held %s), %+v; want it walked unread and unchanged",
			spy.lengths, spy.reads, order(cached), held, s)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if m, ok := repository.SlotManifestForTest(revision); ok {
					_ = order(m)
				}
			}
		}()
	}
	for i := range 8 {
		fresh, err := controlplane.NewObservationDirectory(repository, limits, h.client)
		if err != nil {
			t.Fatal(err)
		}
		fresh.Refresh(h.ctx, at.Add(time.Duration(i)*time.Second))
	}
	close(stop)
	readers.Wait()
	if cached, _ := repository.SlotManifestForTest(revision); order(cached) != held {
		t.Fatalf("the Slot cache's manifest after concurrent refreshes = %s, want %s", order(cached), held)
	}
}

// twoPublications makes the fixture's publication a carried one: a later
// publication (revision e...) becomes the latest while the activation stays
// on the first. Its manifest is manifest when given, else the carried one's
// groups under the new revision.
func twoPublications(t *testing.T, h *objectCatalogHarness, at time.Time, latest func(controlplane.CatalogManifest) controlplane.CatalogManifest) (carried controlplane.SnapshotPublicationRef, carriedKey, latestKey string) {
	t.Helper()
	baseline, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100,
		Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	baseline.Refresh(h.ctx, at)
	carried = baseline.Page(at, "", "", "", 0, 20).Published
	manifest, err := h.repository.LoadCatalogManifest(h.ctx, carried.SnapshotRevision)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SnapshotRevision = execution.SnapshotRevision(strings.Repeat("e", 64))
	if latest != nil {
		manifest = latest(manifest)
	}
	payload, _ := json.Marshal(manifest)
	latestKey = h.prefix + ":manifest:" + string(manifest.SnapshotRevision)
	carriedKey = h.prefix + ":manifest:" + string(carried.SnapshotRevision)
	if err = h.client.Set(h.ctx, latestKey, payload, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err = h.client.Set(h.ctx, h.prefix+":latest_publication", "2\n"+string(manifest.SnapshotRevision), 0).Err(); err != nil {
		t.Fatal(err)
	}
	return carried, carriedKey, latestKey
}

// A held manifest's key is still asked about: a carried publication whose
// manifest went past its retention after the directory remembered it is
// expired - one length read, no bytes, its Plans named as expired - and
// forgotten, as it is on a replica that never held it. The latest
// publication's manifest gone fails the refresh though it was held.
func TestAHeldManifestWhoseKeyIsGoneIsNotWalked(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	h, _, at := directoryFixture(t, 32)
	carried, carriedKey, _ := twoPublications(t, h, at, nil)
	spy := &manifestReadSpy{Cmdable: h.client}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	if !slices.Contains(d.RememberedManifestsForTest(), carried.SnapshotRevision) {
		t.Fatalf("setup: remembered %v, want the carried revision", d.RememberedManifestsForTest())
	}
	if err = h.client.Del(h.ctx, carriedKey).Err(); err != nil {
		t.Fatal(err)
	}
	spy.lengths, spy.reads = nil, nil
	d.Refresh(h.ctx, at.Add(time.Second))
	s := d.Page(at.Add(time.Second), "", "", "", 0, 20)
	expired, named := false, false
	for _, publication := range s.Publications {
		expired = expired || (publication.Publication == carried && publication.Manifest == "expired")
	}
	for _, row := range s.Rows {
		named = named || (row.Publication == carried && row.ManifestExpired)
	}
	// Held and walked that round is the latest manifest alone: the carried
	// one found gone is not counted as held.
	if lengths, reads := spy.of(carried.SnapshotRevision); lengths != 1 || reads != 0 || !expired || !named || s.ManifestsRemembered != 1 ||
		slices.Contains(d.RememberedManifestsForTest(), carried.SnapshotRevision) {
		t.Fatalf("a held carried manifest gone = %d length and %d byte reads, expired %v, rows named %v, %d held, remembered %v; want it expired, not counted and forgotten",
			lengths, reads, expired, named, s.ManifestsRemembered, d.RememberedManifestsForTest())
	}

	h, _, at = directoryFixture(t, 32)
	latest, err := controlplane.NewObservationDirectory(h.newRepository(t), limits)
	if err != nil {
		t.Fatal(err)
	}
	latest.Refresh(h.ctx, at)
	revision := latest.Page(at, "", "", "", 0, 20).Published.SnapshotRevision
	if err = h.client.Del(h.ctx, h.prefix+":manifest:"+string(revision)).Err(); err != nil {
		t.Fatal(err)
	}
	latest.Refresh(h.ctx, at.Add(time.Second))
	if s := latest.Page(at.Add(time.Second), "", "", "", 0, 20); s.Complete || s.FailedRead != "manifest" || s.Publications[0].Manifest != "failed" ||
		s.ManifestsRemembered != 0 || len(latest.RememberedManifestsForTest()) != 0 {
		t.Fatalf("the latest manifest gone though held = %+v, remembered %v; want the refresh failed on it", s, latest.RememberedManifestsForTest())
	}
}

// A refresh that stopped on the latest publication still walks what it holds
// without a read: a carried publication from the index, remembered from the
// refresh before, or in the Slot path's cache is walked, listed and makes
// its rows, from groups the directory knows or the object cache holds.
func TestAStoppedRefreshStillWalksWhatItHolds(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	fake := execution.ObjectDigest(strings.Repeat("a", 64))
	refused := func(_ context.Context, _ int) *redis.StringCmd {
		return redis.NewStringResult("", errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"))
	}
	for _, source := range []string{"index", "remembered", "slot_cache"} {
		t.Run(source, func(t *testing.T) {
			h, _, at := directoryFixture(t, 32)
			// The latest publication lists one group the store refuses, so
			// the refresh stops on it before it reaches the carried one.
			carried, carriedKey, _ := twoPublications(t, h, at, func(m controlplane.CatalogManifest) controlplane.CatalogManifest {
				m.QueryGroups = []controlplane.ManifestQueryGroup{{QueryGroup: "a-refused-group", ObjectDigest: fake}}
				return m
			})
			spy := &stoppingSpy{Cmdable: h.client, part: string(fake)}
			repository := h.newRepository(t)
			if err := repository.ConfigureObjectCache(64, 1<<20); err != nil {
				t.Fatal(err)
			}
			carriedManifest, err := h.repository.LoadCatalogManifest(h.ctx, carried.SnapshotRevision)
			if err != nil {
				t.Fatal(err)
			}
			for _, group := range carriedManifest.QueryGroups {
				if _, err := repository.LoadQueryGroupObject(h.ctx, group.ObjectDigest); err != nil {
					t.Fatal(err)
				}
			}
			d, err := controlplane.NewObservationDirectory(repository, limits, spy)
			switch source {
			case "index":
				d, err = controlplane.NewObservationDirectory(h.repository, limits, spy)
			case "slot_cache":
				repository.StoreSlotManifestForTest(carriedManifest)
			case "remembered":
				// Two refreshes the store answers, so the carried manifest is
				// remembered and the next refresh walks the latest first again.
				d.Refresh(h.ctx, at)
				d.Refresh(h.ctx, at.Add(time.Second))
			}
			if err != nil {
				t.Fatal(err)
			}
			spy.fail = refused
			spy.keys = nil
			d.Refresh(h.ctx, at.Add(2*time.Second))
			s := d.Page(at.Add(2*time.Second), "", "", "", 0, 20)
			listed, rows := false, 0
			for _, publication := range s.Publications {
				listed = listed || (publication.Publication == carried && publication.Manifest == source)
			}
			for _, row := range s.Rows {
				if row.Publication == carried {
					rows++
				}
			}
			if s.FailedRead != "group_object" || !listed || rows != 2 || slices.Contains(spy.keys, carriedKey) {
				t.Fatalf("stopped on the latest, the carried one from %s = failed %q, publications %+v, %d rows, reads %v; want it walked, listed, its 2 rows, its key not asked about",
					source, s.FailedRead, s.Publications, rows, spy.keys)
			}
		})
	}
}

// A manifest is sent when it fits what the refresh has left exactly, and not
// when it is one byte longer; with the refresh's commands spent its length
// is not asked. A manifest held from the index is not remembered: the index
// is there every refresh. The manifests remembered are held in what the rows
// leave: rows filling most of the allowance leave room for one of two.
func TestAManifestIsReadOnlyWhenItFitsAndHeldOnlyInWhatIsLeft(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	latest, err := h.client.Get(h.ctx, h.prefix+":latest_publication").Result()
	if err != nil {
		t.Fatal(err)
	}
	revision := execution.SnapshotRevision(strings.Split(latest, "\n")[1])
	length, err := h.client.StrLen(h.ctx, h.prefix+":manifest:"+string(revision)).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		limits   controlplane.DirectoryLimits
		lengths  int
		reads    int
		manifest string
	}{
		{"fits exactly", controlplane.DirectoryLimits{WireBytes: len(latest) + int(length), Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, 1, 1, "store"},
		{"one byte long", controlplane.DirectoryLimits{WireBytes: len(latest) + int(length) - 1, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, 1, 0, "unread"},
		{"commands spent", controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 1, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}, 0, 0, "unread"},
	} {
		spy := &manifestReadSpy{Cmdable: h.client}
		d, err := controlplane.NewObservationDirectory(h.newRepository(t), c.limits, spy)
		if err != nil {
			t.Fatal(err)
		}
		d.Refresh(h.ctx, at)
		s := d.Page(at, "", "", "", 0, 20)
		if lengths, reads := spy.of(revision); lengths != c.lengths || reads != c.reads || s.Publications[0].Manifest != c.manifest {
			t.Errorf("%s = %d length and %d byte reads, manifest %q; want %d, %d, %q", c.name, lengths, reads, s.Publications[0].Manifest, c.lengths, c.reads, c.manifest)
		}
	}

	indexed, err := controlplane.NewObservationDirectory(h.repository, controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100,
		Timeout: time.Second, FreshFor: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	indexed.Refresh(h.ctx, at)
	if s := indexed.Page(at, "", "", "", 0, 20); s.Publications[0].Manifest != "index" || len(indexed.RememberedManifestsForTest()) != 0 {
		t.Fatalf("a manifest from the index = %q, remembered %v; want it not remembered", s.Publications[0].Manifest, indexed.RememberedManifestsForTest())
	}

	for _, c := range []struct {
		entries, remembered int
	}{{100, 2}, {3, 1}} {
		h, _, at := directoryFixture(t, 32)
		twoPublications(t, h, at, nil)
		d, err := controlplane.NewObservationDirectory(h.newRepository(t), controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: c.entries,
			Timeout: time.Second, FreshFor: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		d.Refresh(h.ctx, at)
		if got := len(d.RememberedManifestsForTest()); got != c.remembered {
			t.Errorf("two manifests walked with %d entries' allowance = %d remembered, want %d", c.entries, got, c.remembered)
		}
	}
}

// A manifest that does not decode is that manifest's: the refresh does not
// stop on it and still reads the audit. A refresh cancelled by its caller is
// the store unavailable to it, not its budget.
func TestAManifestThatDoesNotDecodeDoesNotStopTheRefresh(t *testing.T) {
	limits := controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 100, Timeout: time.Second, FreshFor: time.Minute}
	h, _, at := directoryFixture(t, 32)
	_, carriedKey, _ := twoPublications(t, h, at, nil)
	if err := h.client.Set(h.ctx, carriedKey, "{not json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	spy := &stoppingSpy{Cmdable: h.client}
	d, err := controlplane.NewObservationDirectory(h.newRepository(t), limits, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	if s := d.Page(at, "", "", "", 0, 20); s.FailedRead != "manifest" || len(spy.read(":latest_audit")) != 1 {
		t.Fatalf("a carried manifest that does not decode = failed %q, reads %v; want it named and the audit still read", s.FailedRead, spy.keys)
	}

	ctx, cancel := context.WithCancel(h.ctx)
	cancel()
	d.Refresh(ctx, at.Add(time.Second))
	if s := d.Page(at.Add(time.Second), "", "", "", 0, 20); s.Reason != "DEPENDENCY_UNAVAILABLE" {
		t.Fatalf("a refresh its caller cancelled = %q, want DEPENDENCY_UNAVAILABLE", s.Reason)
	}
}

// Why a refresh's reads stopped is kept as it was then: stopped on the store
// refusing a read, and past it out of room for rows, the snapshot's reason
// reads the budget, and the unread audit still says the store stopped it.
func TestWhyAStoppedRefreshStoppedIsKeptPastALaterFailure(t *testing.T) {
	h, _, at := directoryFixture(t, 32)
	fake := execution.ObjectDigest(strings.Repeat("a", 64))
	twoPublications(t, h, at, func(m controlplane.CatalogManifest) controlplane.CatalogManifest {
		m.QueryGroups = []controlplane.ManifestQueryGroup{{QueryGroup: "a-refused-group", ObjectDigest: fake}}
		return m
	})
	spy := &stoppingSpy{Cmdable: h.client, part: string(fake), fail: func(_ context.Context, _ int) *redis.StringCmd {
		return redis.NewStringResult("", errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"))
	}}
	d, err := controlplane.NewObservationDirectory(h.repository, controlplane.DirectoryLimits{WireBytes: 1 << 20, Commands: 32, Entries: 1,
		Timeout: time.Second, FreshFor: time.Minute}, spy)
	if err != nil {
		t.Fatal(err)
	}
	d.Refresh(h.ctx, at)
	if s := d.Page(at, "", "", "", 0, 20); s.FailedRead != "group_object" || s.Reason != "RESOURCE_BUDGET" || s.SourceReason != "SOURCE_AUDIT_UNAVAILABLE" {
		t.Fatalf("stopped on a refused read, then out of room = failed %q reason %q source %q; want the audit named by why the reads stopped",
			s.FailedRead, s.Reason, s.SourceReason)
	}
}
