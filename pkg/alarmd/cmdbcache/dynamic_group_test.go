// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

// groupClient serves MGET from a map and can fail the transport on demand.
type groupClient struct {
	values map[string]string
	err    error
	calls  [][]string
}

func (client *groupClient) MGet(_ context.Context, keys ...string) *redis.SliceCmd {
	client.calls = append(client.calls, append([]string(nil), keys...))
	if client.err != nil {
		return redis.NewSliceResult(nil, client.err)
	}
	values := make([]any, len(keys))
	for index, key := range keys {
		if value, found := client.values[key]; found {
			values[index] = value
		}
	}
	return redis.NewSliceResult(values, nil)
}

const hostGroup = `{"model_id":"cw-Host","bk_obj_id":"cw-Host","model_inst_ids":["101","102","103"],
	"member_list":[
		{"model_id":"cw-Host","model_inst_id":"101","bk_host_id":101,"ip_list":[]},
		{"model_id":"cw-Host","model_inst_id":"102","bk_host_id":"102"},
		{"model_id":"cw-Host","model_inst_id":"103"},
		{"model_id":"cw-MySQL","model_inst_id":"db-1"},
		{"model_id":"cw-Host","model_inst_id":"","bk_host_id":9},
		{"model_id":"cw-Host","model_inst_id":"104","bk_host_id":104}]}`

func hostPlan(rule contract.TargetPlanRule) *contract.TargetPlanV1 {
	identity := contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}}
	if rule == contract.TargetPlanRuleModelInstID {
		identity = contract.TargetPlanIdentityV1{Dimensions: []string{"cw_object_model_inst_id"}, ModelDimension: "cw_object_model_id", ModelValue: "17"}
	}
	return &contract.TargetPlanV1{SchemaVersion: 1, ModelID: "cw-Host", Rule: rule, Identity: identity, StaticKeys: []string{}, DynamicGroups: []string{"1001"}}
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// A group document is read per the protocol: members of another model, of
// no instance, or absent from the summary are dropped and counted; the rest
// are kept with their host id when the writer put one on them. The keys a
// plan reads from the snapshot follow its rule - a host member without a
// host id is dropped under host_id and kept under model_inst_id - and are
// built once per plan identity per snapshot.
func TestAGroupDocumentIsReadPerTheProtocolAndKeyedPerThePlan(t *testing.T) {
	snapshot := decodeGroup("1001", []byte(hostGroup), time.Unix(1000, 0))
	if snapshot.Unavailable != "" || snapshot.ModelID != "cw-Host" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if len(snapshot.Members) != 3 || snapshot.Dropped != 3 {
		t.Fatalf("members = %+v dropped = %d; want 101, 102, 103 kept and the other model, the nameless and the unlisted dropped", snapshot.Members, snapshot.Dropped)
	}
	byHost, dropped := snapshot.Keys(hostPlan(contract.TargetPlanRuleHostID))
	if !reflect.DeepEqual(sortedKeys(byHost), []string{"101", "102"}) || dropped != 1 {
		t.Fatalf("host keys = %v dropped %d; want 101 and 102, 103 dropped for carrying no host id", sortedKeys(byHost), dropped)
	}
	byInstance, dropped := snapshot.Keys(hostPlan(contract.TargetPlanRuleModelInstID))
	if !reflect.DeepEqual(sortedKeys(byInstance), []string{"101", "102", "103"}) || dropped != 0 {
		t.Fatalf("instance keys = %v dropped %d", sortedKeys(byInstance), dropped)
	}
	again, _ := snapshot.Keys(hostPlan(contract.TargetPlanRuleHostID))
	if reflect.ValueOf(again).Pointer() != reflect.ValueOf(byHost).Pointer() {
		t.Fatal("the key set was rebuilt for the same plan identity")
	}
	for name, test := range map[string]struct {
		payload string
		reason  string
	}{
		"not json":             {payload: `{`, reason: targetplan.ReasonJSONInvalid},
		"no member_list":       {payload: `{"model_id":"cw-Host","model_inst_ids":[]}`, reason: targetplan.ReasonStructureInvalid},
		"no model":             {payload: `{"member_list":[]}`, reason: targetplan.ReasonStructureInvalid},
		"member_list not list": {payload: `{"model_id":"cw-Host","member_list":{}}`, reason: targetplan.ReasonJSONInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			if got := decodeGroup("1", []byte(test.payload), time.Unix(1000, 0)); got.Unavailable != test.reason {
				t.Fatalf("unavailable = %q, want %q", got.Unavailable, test.reason)
			}
		})
	}
	empty := decodeGroup("1", []byte(`{"model_id":"cw-Host","model_inst_ids":[],"member_list":[]}`), time.Unix(1000, 0))
	if empty.Unavailable != "" || len(empty.Members) != 0 || empty.Dropped != 0 {
		t.Fatalf("an explicit empty member list = %+v, want a usable empty snapshot", empty)
	}
}

// The store reads a group the first time it is asked for it, then on its
// cadence; a transport failure keeps every snapshot and marks the lookups
// that follow as served past a failed refresh; a key that went missing is
// an answer and replaces the snapshot.
func TestTheGroupStoreReadsOnFirstReferenceAndKeepsSnapshotsAcrossAFailedRefresh(t *testing.T) {
	client := &groupClient{values: map[string]string{"cw_prefix:dynamic_group:1001": hostGroup}}
	reader, err := NewGroupReader(client, "cw_prefix:")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	store, err := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	lookup := store.Group(context.Background(), "1001", time.Minute)
	if lookup.ReadErr != nil || lookup.Snapshot == nil || lookup.Snapshot.Unavailable != "" || lookup.Age != 0 || lookup.RefreshFailed {
		t.Fatalf("first lookup = %+v", lookup)
	}
	if len(client.calls) != 1 || !reflect.DeepEqual(client.calls[0], []string{"cw_prefix:dynamic_group:1001"}) {
		t.Fatalf("calls = %v, want one MGET of the writer's key", client.calls)
	}
	now = now.Add(30 * time.Second)
	if again := store.Group(context.Background(), "1001", time.Minute); len(client.calls) != 1 || again.Age != 30*time.Second {
		t.Fatalf("second lookup read again or misreported its age: calls=%d age=%s", len(client.calls), again.Age)
	}
	missing := store.Group(context.Background(), "2002", time.Minute)
	if missing.Snapshot == nil || missing.Snapshot.Unavailable != targetplan.ReasonKeyMissing {
		t.Fatalf("missing key lookup = %+v, want unavailable key_missing", missing)
	}

	client.err = errors.New("connection refused")
	now = now.Add(time.Minute)
	if err := store.Refresh(context.Background()); err == nil {
		t.Fatal("a failed refresh reported success")
	}
	served := store.Group(context.Background(), "1001", time.Minute)
	if served.Snapshot == nil || served.Snapshot.Unavailable != "" || !served.RefreshFailed || served.Age != 90*time.Second {
		t.Fatalf("lookup after a failed refresh = %+v, want the old snapshot, marked as served past a failed refresh", served)
	}
	if health := store.Health(); !health.RefreshFailed || health.Referenced != 2 || health.Loaded != 1 || health.Unavailable != 1 {
		t.Fatalf("health = %+v", health)
	}

	client.err = nil
	delete(client.values, "cw_prefix:dynamic_group:1001")
	now = now.Add(time.Minute)
	if err := store.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	gone := store.Group(context.Background(), "1001", time.Minute)
	if gone.Snapshot.Unavailable != targetplan.ReasonKeyMissing || gone.RefreshFailed || gone.Age != 0 {
		t.Fatalf("lookup after the key went missing = %+v, want unavailable key_missing from a fresh read", gone)
	}
	if last := client.calls[len(client.calls)-1]; !reflect.DeepEqual(last, []string{"cw_prefix:dynamic_group:1001", "cw_prefix:dynamic_group:2002"}) {
		t.Fatalf("refresh read %v, want every referenced id in one MGET", last)
	}
}

// A reference ages out when nobody has asked for it within its horizon -
// max(two refresh intervals, twice the longest interval of the Plans that
// asked) - so a withdrawn reference is read once more at most and stops
// counting in the health, while a Plan on a ten-minute period never reads
// on its Slot for a group that aged out between two of its Slots: three
// Slots of such a Plan cost one synchronous read, the first.
func TestAGroupAgesOutByTheReferencingPlansIntervalAndNeverCostsItsSlotARead(t *testing.T) {
	client := &groupClient{values: map[string]string{"cw:dynamic_group:slow": hostGroup, "cw:dynamic_group:gone": hostGroup}}
	reader, _ := NewGroupReader(client, "cw:")
	now := time.Unix(1000, 0)
	store, err := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	store.Group(context.Background(), "gone", time.Minute)
	slots := 0
	for minute := 0; minute <= 30; minute++ {
		if minute%10 == 0 {
			// The ten-minute Plan's Slot: it asks, and never reads.
			if lookup := store.Group(context.Background(), "slow", 10*time.Minute); lookup.Snapshot == nil || lookup.Snapshot.Unavailable != "" {
				t.Fatalf("minute %d: slow lookup = %+v", minute, lookup)
			}
			slots++
		}
		if err := store.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if slots != 4 || store.Health().SyncReads != 2 {
		t.Fatalf("slots %d sync reads %d, want four Slots and two synchronous reads: the first reference of each group and no other", slots, store.Health().SyncReads)
	}
	// The one-minute reference nobody asked for again was read at the
	// refreshes within its two-minute horizon and dropped at the first one
	// past it, never read again after that; the ten-minute one was read at
	// every refresh.
	readsOfGone, readsOfSlow := 0, 0
	for _, call := range client.calls {
		for _, key := range call {
			switch key {
			case "cw:dynamic_group:gone":
				readsOfGone++
			case "cw:dynamic_group:slow":
				readsOfSlow++
			}
		}
	}
	if readsOfGone != 4 || readsOfSlow != 32 {
		t.Fatalf("reads of the withdrawn reference %d (want its first read and the three refreshes within its horizon), of the slow Plan's %d (want its first read and every refresh)", readsOfGone, readsOfSlow)
	}
	if health := store.Health(); health.Referenced != 1 || health.Loaded != 1 {
		t.Fatalf("health = %+v, want the withdrawn reference gone", health)
	}
	// The slow Plan stops asking: its reference outlives two of its Slots
	// and is gone at the refresh after that.
	for minute := 0; minute <= 21; minute++ {
		if err := store.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if health := store.Health(); health.Referenced != 0 {
		t.Fatalf("health after the slow Plan stopped asking = %+v", health)
	}
	// Read at the twenty refreshes within its horizon and at none after; a
	// refresh with nothing referenced issues no command at all.
	readsOfSlow = 0
	for _, call := range client.calls {
		for _, key := range call {
			if key == "cw:dynamic_group:slow" {
				readsOfSlow++
			}
		}
	}
	if readsOfSlow != 52 {
		t.Fatalf("reads of the slow Plan's group after it stopped asking = %d, want 32 + 20", readsOfSlow)
	}
}
