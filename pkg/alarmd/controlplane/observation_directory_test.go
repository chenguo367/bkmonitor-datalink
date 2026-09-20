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
	h, d, at := directoryFixture(t, 4)
	// Latest publication, activation, manifest,
	// then exactly one cold object. A later tick reuses that identity projection.
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
