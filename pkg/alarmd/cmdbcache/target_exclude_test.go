// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

func TestExclusionsSubtractFromStaticGroupsAndTopologyWithoutChangingSharedCaches(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	client := &groupClient{values: map[string]string{"cw:dynamic_group:g": `{"model_id":"cw-Host","model_inst_ids":["501","502"],"member_list":[{"model_id":"cw-Host","model_inst_id":"501","bk_host_id":501},{"model_id":"cw-Host","model_inst_id":"502","bk_host_id":502}]}`}}
	reader, _ := NewGroupReader(client, "cw:")
	groups, _ := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, Now: clock})
	hosts := hostStore(t, clock, []string{"501", hostUnderSet, "502", hostUnderSetNoIdentity}, []string{"set|12"})
	resolver := NewTargetResolver(groups, hosts, clock)
	plan := hostPlan(contract.TargetPlanRuleHostID)
	plan.StaticKeys, plan.ExcludeKeys = []string{"501"}, []string{"501"}
	plan.DynamicGroups = []string{"g"}
	plan.DynamicTopologies = []contract.TargetPlanTopologyV1{{BusinessID: "2", ObjectID: "set", InstanceID: "12"}}
	got := resolver.Resolve(context.Background(), plan, time.Minute)
	if got.State != targetplan.ResolutionComplete || got.Contains("501") || !reflect.DeepEqual(got.Members(), []string{"502"}) {
		t.Fatalf("resolution %+v members %v", got, got.Members())
	}
	// Another plan still sees every cached member; subtraction never mutates
	// a shared selector snapshot.
	plan.ExcludeKeys = nil
	other := resolver.Resolve(context.Background(), plan, time.Minute)
	if !reflect.DeepEqual(other.Members(), []string{"501", "502"}) {
		t.Fatalf("shared members lost: %v", other.Members())
	}
	plan.ExcludeKeys = []string{"501", "502"}
	empty := resolver.Resolve(context.Background(), plan, time.Minute)
	if empty.State != targetplan.ResolutionComplete || len(empty.Members()) != 0 {
		t.Fatalf("fully excluded target = %+v", empty)
	}
	client.values["cw:dynamic_group:g"] = `{"model_id":"cw-Host","model_inst_ids":["503"],"member_list":[{"model_id":"cw-Host","model_inst_id":"503","bk_host_id":503}]}`
	now = now.Add(2 * time.Minute)
	if err := groups.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	newMember := resolver.Resolve(context.Background(), plan, time.Minute)
	if newMember.State != targetplan.ResolutionComplete || !reflect.DeepEqual(newMember.Members(), []string{"503"}) {
		t.Fatalf("empty target did not admit a later dynamic member: %v", newMember.Members())
	}
	// A failed inclusion selector keeps the known remainder, still excluded,
	// while absence remains unavailable.
	plan.ExcludeKeys, plan.DynamicGroups = []string{"501"}, []string{"missing"}
	partial := resolver.Resolve(context.Background(), plan, time.Minute)
	if partial.State != targetplan.ResolutionUnavailable || partial.Contains("501") || !partial.Contains("502") {
		t.Fatalf("partial target widened or lost known members: %+v", partial)
	}
}

type refreshDuringGroupRead struct {
	groupClient
	refresh func()
}

func (client *refreshDuringGroupRead) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	client.refresh()
	return client.groupClient.MGet(ctx, keys...)
}

func TestOneSlotPinsTheHostSnapshotForInclusionAndExclusion(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	hosts := hostStore(t, clock, []string{"501", hostUnderSet}, []string{"set|12"})
	newHost := strings.Replace(hostUnderSet, `"bk_host_id":501`, `"bk_host_id":601`, 1)
	updated := hostStore(t, clock, []string{"601", newHost}, []string{"set|12"})
	client := &refreshDuringGroupRead{groupClient: groupClient{values: map[string]string{"cw:dynamic_group:g": `{"model_id":"cw-Host","model_inst_ids":[],"member_list":[]}`}}, refresh: func() {
		hosts.mutex.Lock()
		hosts.index = updated.index
		hosts.mutex.Unlock()
	}}
	reader, _ := NewGroupReader(client, "cw:")
	groups, _ := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, Now: clock})
	plan := &contract.TargetPlanV1{SchemaVersion: 1, ModelID: "cw-Host", Rule: contract.TargetPlanRuleModelInstID,
		Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true}, StaticKeys: []string{},
		StaticMembers: []contract.TargetPlanMemberV1{{ModelID: "cw-Host", ModelInstID: "501"}}, ExcludeMembers: []contract.TargetPlanMemberV1{{ModelID: "cw-Host", ModelInstID: "501"}}, DynamicGroups: []string{"g"},
		DynamicTopologies: []contract.TargetPlanTopologyV1{{BusinessID: "2", ObjectID: "set", InstanceID: "12"}}}
	got := NewTargetResolver(groups, hosts, clock).Resolve(context.Background(), plan, time.Minute)
	if got.State != targetplan.ResolutionComplete || len(got.Members()) != 0 {
		t.Fatalf("mixed host snapshots widened target: %+v members %v", got, got.Members())
	}
	if _, found := got.Excluded["501"]; !found {
		t.Fatalf("exclusion used the later host snapshot: %v", got.Excluded)
	}
}

func TestUnresolvedExcludedHostIdentityBlocksTheWholePlan(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	hosts := hostStore(t, clock, []string{"501", hostUnderSet}, []string{"set|12"})
	resolver := NewTargetResolver(nil, hosts, clock)
	plan := &contract.TargetPlanV1{SchemaVersion: 1, ModelID: "cw-Host", Rule: contract.TargetPlanRuleModelInstID,
		Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true}, StaticKeys: []string{},
		StaticMembers:  []contract.TargetPlanMemberV1{{ModelID: "cw-Host", ModelInstID: "501"}},
		ExcludeMembers: []contract.TargetPlanMemberV1{{ModelID: "cw-Host", ModelInstID: "unmapped"}}}
	got := resolver.Resolve(context.Background(), plan, time.Minute)
	if got.State != targetplan.ResolutionUnavailable || got.Contains("501") || len(got.Members()) != 0 {
		t.Fatalf("unresolved exclusion admitted targets: %+v", got)
	}
	if failure := selector(got, targetplan.SelectorKindExclude, "cw-Host"); failure.Reason != targetplan.ReasonModelUnresolved {
		t.Fatalf("missing exclusion evidence: %+v", failure)
	}
	plan.ExcludeMembers[0].ModelInstID = "501"
	got = resolver.Resolve(context.Background(), plan, time.Minute)
	if got.State != targetplan.ResolutionComplete || len(got.Members()) != 0 {
		t.Fatalf("resolved exclusion = %+v", got)
	}
	// A partly mapped exclusion is not safe to apply partially.
	plan.ExcludeMembers = append(plan.ExcludeMembers, contract.TargetPlanMemberV1{ModelID: "cw-Host", ModelInstID: "unmapped"})
	got = resolver.Resolve(context.Background(), plan, time.Minute)
	if got.State != targetplan.ResolutionUnavailable || !got.ExclusionUnavailable {
		t.Fatalf("partial exclusion accepted: %+v", got)
	}
}
