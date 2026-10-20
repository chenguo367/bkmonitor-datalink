// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
)

const identityLive = `{"bk_host_id":720002,"bk_host_innerip":"192.0.2.172","bk_cloud_id":0,"bk_biz_id":999,"bk_agent_id":"agent-live",` +
	`"bk_state":"运营中[需告警]","display_name":"l","topo_link":{"module|91":[{"bk_obj_id":"module","bk_inst_id":91}]}}`

const identityOther = `{"bk_host_id":720003,"bk_host_innerip":"192.0.2.173","bk_cloud_id":0,"bk_biz_id":999,` +
	`"bk_state":"运营中[需告警]","display_name":"o","topo_link":{"module|91":[{"bk_obj_id":"module","bk_inst_id":91}]}}`

// identityLoad is one load of the writer's cache: the hosts it holds, the
// agent hash (nil for one that could not be read), when the writer
// published it, and records refused beside them.
func identityLoad(hosts []string, agents map[string]string, published time.Time, refused int) func() (*Index, error) {
	return func() (*Index, error) {
		builder := newIndexBuilder(published)
		builder.addFields(hosts)
		index := builder.index
		if agents == nil {
			index.byAgent, index.agentsUnreadable = nil, true
		} else {
			index.byAgent = agents
		}
		index.publishedAt = published
		index.refused.Hosts += refused
		return index, nil
	}
}

type identityCase struct {
	t      *testing.T
	now    time.Time
	store  *Store
	loads  *scriptedLoads
	chain  *admission.Chain
	record map[string]json.RawMessage
}

func newIdentityCase(t *testing.T, record string) *identityCase {
	t.Helper()
	c := &identityCase{t: t, now: time.Unix(1_800_000_000, 0), loads: &scriptedLoads{}}
	store, err := NewStore(c.loads, StoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute,
		PublishedMaxAge: 15 * time.Minute, Now: func() time.Time { return c.now }})
	if err != nil {
		t.Fatal(err)
	}
	c.store, c.chain = store, instanceChain(t, store)
	c.record = jsonDims(t, record)
	return c
}

// refresh hands the store its next load.
func (c *identityCase) refresh(load func() (*Index, error)) {
	c.t.Helper()
	c.loads.loads = append(c.loads.loads, load)
	if err := c.store.Refresh(context.Background()); err != nil {
		c.t.Fatal(err)
	}
}

// overlay is what the record's alert identity reads now, and whether the
// facts the filters decide on were there.
func (c *identityCase) overlay() (string, bool) {
	c.t.Helper()
	facts := c.chain.Enrich(c.record)
	if facts.HostIdentityUnavailable {
		c.t.Fatalf("the record's identity was said unreadable with an identity index held")
	}
	encoded, _ := json.Marshal(facts.HostOverlay)
	return string(encoded), !facts.HostFactsUnavailable
}

// wantLiveOverlay is what an agent-named record's identity reads at host
// 720002; an id-named record carries its own bk_host_id, which is not
// written over (wantLiveOverlayByID).
const (
	wantLiveOverlay     = `{"bk_host_id":"720002","bk_target_cloud_id":"0","bk_target_ip":"192.0.2.172"}`
	wantLiveOverlayByID = `{"bk_target_cloud_id":"0","bk_target_ip":"192.0.2.172"}`
)

// A writer measured at one deployment published 16.5 minutes apart against
// a 15-minute bound. For the minute and a half between the bound and the
// next publish the held index was stale;
// the record's alert identity still reads the host CMDB placed it at, while
// the filters see the facts unavailable, as they did.
func TestAStaleIndexKeepsAHostPlacedRecordsIdentity(t *testing.T) {
	for _, by := range []struct{ name, record, want string }{
		{"by agent", `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`, wantLiveOverlay},
		{"by id", `{"bk_host_id":"720002","bk_target_ip":"198.51.100.9"}`, wantLiveOverlayByID},
	} {
		t.Run(by.name, func(t *testing.T) {
			c := newIdentityCase(t, by.record)
			published := c.now
			c.refresh(identityLoad([]string{"720002", identityLive, "192.0.2.172|0", identityLive}, map[string]string{"agent-live": "720002"}, published, 0))
			fresh, decided := c.overlay()
			if fresh != by.want || !decided {
				t.Fatalf("fresh index: overlay %s (facts there %t), want %s", fresh, decided, by.want)
			}
			c.now = published.Add(15*time.Minute + 17*time.Second)
			if _, reason := c.store.Usable(); reason != IndexStale {
				t.Fatalf("the index at 15m17s is %q, want stale", reason)
			}
			stale, decided := c.overlay()
			if stale != fresh {
				t.Fatalf("stale index: overlay %s, want the same as fresh, %s", stale, fresh)
			}
			if decided {
				t.Fatal("stale index: the filters were handed facts as if the index were usable")
			}
		})
	}
}

// An empty load replacing a populated one leaves the identity where the
// populated one placed it.
func TestAnEmptyLoadKeepsAHostPlacedRecordsIdentity(t *testing.T) {
	c := newIdentityCase(t, `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`)
	c.refresh(identityLoad([]string{"720002", identityLive}, map[string]string{"agent-live": "720002"}, c.now, 0))
	before, _ := c.overlay()
	c.refresh(identityLoad(nil, map[string]string{}, c.now, 0))
	if _, reason := c.store.Usable(); reason != IndexEmpty {
		t.Fatalf("the empty load is %q, want empty", reason)
	}
	if after, _ := c.overlay(); after != before || after != wantLiveOverlay {
		t.Fatalf("after an empty load: overlay %s, want %s", after, before)
	}
}

// A load whose agent hash could not be read leaves an agent-named record's
// identity where the last readable hash placed it, and is read again on the
// next refresh rather than kept on an unmoved publish time.
func TestAnUnreadableAgentHashKeepsTheRecordsIdentity(t *testing.T) {
	c := newIdentityCase(t, `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`)
	c.refresh(identityLoad([]string{"720002", identityLive}, map[string]string{"agent-live": "720002"}, c.now, 0))
	before, _ := c.overlay()
	c.refresh(identityLoad([]string{"720002", identityLive}, nil, c.now, 0))
	after, decided := c.overlay()
	if after != before || after != wantLiveOverlay || decided {
		t.Fatalf("unreadable agent hash: overlay %s (facts there %t), want %s with the facts unavailable", after, decided, before)
	}
}

// A load that refused host records keeps the identity behind it: a host it
// refused is still found where the last load placed it. A clean load that no
// longer holds a host means the host is gone, and its record keeps its own
// address, as Python's would.
func TestARefusedHostKeepsItsIdentityAndACleanlyAbsentOneDoesNot(t *testing.T) {
	c := newIdentityCase(t, `{"bk_host_id":"720002","bk_target_ip":"198.51.100.9"}`)
	both := []string{"720002", identityLive, "720003", identityOther}
	c.refresh(identityLoad(both, map[string]string{}, c.now, 0))
	before, _ := c.overlay()
	c.refresh(identityLoad([]string{"720003", identityOther}, map[string]string{}, c.now, 1))
	if after, _ := c.overlay(); after != before || after != wantLiveOverlayByID {
		t.Fatalf("host refused in one load: overlay %s, want %s", after, before)
	}
	c.refresh(identityLoad([]string{"720003", identityOther}, map[string]string{}, c.now, 0))
	if gone, _ := c.overlay(); gone != "null" {
		t.Fatalf("host absent from a clean load: overlay %s, want none", gone)
	}
}

// Without any index that ever held hosts, a record naming a true id or
// agent says its identity cannot be read; one naming neither does not.
func TestWithoutAnyIdentityIndexAHostPlacedRecordSaysSo(t *testing.T) {
	for _, record := range []struct {
		dims string
		want bool
	}{
		{`{"bk_host_id":"720002","bk_target_ip":"198.51.100.9"}`, true},
		{`{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`, true},
		{`{"bk_target_ip":"198.51.100.9"}`, false},
	} {
		c := newIdentityCase(t, record.dims)
		c.refresh(identityLoad(nil, map[string]string{}, c.now, 0))
		facts := c.chain.Enrich(c.record)
		if facts.HostIdentityUnavailable != record.want || facts.HostOverlay != nil {
			t.Errorf("%s: identity unavailable %t overlay %v, want %t and none", record.dims, facts.HostIdentityUnavailable, facts.HostOverlay, record.want)
		}
	}
}

// Where the identity came from is named: the index in force when it may be
// decided on (nothing), the stale index, or the last populated one.
func TestTheIdentityOverlaySourceIsNamed(t *testing.T) {
	c := newIdentityCase(t, `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`)
	published := c.now
	c.refresh(identityLoad([]string{"720002", identityLive}, map[string]string{"agent-live": "720002"}, published, 0))
	if source := c.chain.Enrich(c.record).HostOverlaySource; source != "" {
		t.Fatalf("usable index: source %q, want none", source)
	}
	c.now = published.Add(16 * time.Minute)
	if source := c.chain.Enrich(c.record).HostOverlaySource; source != admission.HostOverlayFromStaleIndex {
		t.Fatalf("stale index: source %q, want %s", source, admission.HostOverlayFromStaleIndex)
	}
	c.now = published
	c.refresh(identityLoad(nil, map[string]string{}, published, 0))
	if source := c.chain.Enrich(c.record).HostOverlaySource; source != admission.HostOverlayFromLastPopulatedIndex {
		t.Fatalf("empty index: source %q, want %s", source, admission.HostOverlayFromLastPopulatedIndex)
	}
}
