// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

// hostGroupOf is a host group document with the given host ids as members.
func hostGroupOf(hosts ...int) string {
	ids := make([]string, 0, len(hosts))
	members := make([]string, 0, len(hosts))
	for _, host := range hosts {
		ids = append(ids, fmt.Sprintf(`"%d"`, host))
		members = append(members, fmt.Sprintf(`{"model_id":"cw-Host","model_inst_id":"%d","bk_host_id":%d}`, host, host))
	}
	return `{"model_id":"cw-Host","model_inst_ids":[` + strings.Join(ids, ",") + `],"member_list":[` + strings.Join(members, ",") + `]}`
}

const emptyHostGroup = `{"model_id":"cw-Host","model_inst_ids":[],"member_list":[]}`

// failingGroupClient serves MGET from a map and fails the call numbered failAt
// (from 1), counting every call and the keys each asked for.
type failingGroupClient struct {
	groupClient
	failAt int
}

func (client *failingGroupClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	if len(client.calls)+1 == client.failAt {
		client.calls = append(client.calls, append([]string(nil), keys...))
		return redis.NewSliceResult(nil, errors.New("connection reset"))
	}
	return client.groupClient.MGet(ctx, keys...)
}

// A read over many groups is 64 keys an MGET, in order, and a transport
// failure on any of them returns nothing: the caller keeps what it held
// rather than some groups from this round and none of the rest. The batch is
// written here as the number it is, not as the constant, so a change to it
// has to change this test.
func TestAGroupReadIsBatchedAndAFailurePartWayReturnsNothing(t *testing.T) {
	client := &failingGroupClient{groupClient: groupClient{values: map[string]string{}}}
	ids := make([]string, 0, 130)
	for index := 0; index < 130; index++ {
		id := fmt.Sprint(1000 + index)
		ids = append(ids, id)
		client.values["p:dynamic_group:"+id] = hostGroupOf(index + 1)
	}
	reader, err := NewGroupReader(client, "p:")
	if err != nil {
		t.Fatal(err)
	}
	reads, err := reader.Read(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	sizes := make([]int, 0, len(client.calls))
	for _, call := range client.calls {
		sizes = append(sizes, len(call))
	}
	if fmt.Sprint(sizes) != "[64 64 2]" {
		t.Fatalf("MGETs of %v keys, want [64 64 2]", sizes)
	}
	if client.calls[1][0] != "p:dynamic_group:"+ids[64] || client.calls[2][1] != "p:dynamic_group:"+ids[129] {
		t.Fatalf("batches out of order: %v ... %v", client.calls[1][:1], client.calls[2])
	}
	if len(reads) != len(ids) || string(reads[ids[len(ids)-1]].Payload) != hostGroupOf(len(ids)) {
		t.Fatalf("read %d groups, the last %q", len(reads), reads[ids[len(ids)-1]].Payload)
	}

	client.calls, client.failAt = nil, 2
	if reads, err := reader.Read(context.Background(), ids); err == nil || reads != nil {
		t.Fatalf("a read whose second batch failed = %d groups, %v; want nothing and the error", len(reads), err)
	}
}

// groupFixture is a store over count host groups, each referenced and read
// with a member, and the client behind it. Its refreshes are a minute apart
// and every group is asked for after each, as its Plans ask every Slot.
type groupFixture struct {
	client *groupClient
	store  *GroupStore
	now    time.Time
	ids    []string
	said   [][2]int
}

func newGroupFixture(t *testing.T, count int) *groupFixture {
	t.Helper()
	fixture := &groupFixture{client: &groupClient{values: map[string]string{}}, now: time.Unix(1000, 0)}
	reader, err := NewGroupReader(fixture.client, "p:")
	if err != nil {
		t.Fatal(err)
	}
	fixture.store, err = NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute,
		Now:            func() time.Time { return fixture.now },
		EmptiedChanged: func(held, candidates int) { fixture.said = append(fixture.said, [2]int{held, candidates}) }})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprint(2000 + index)
		fixture.ids = append(fixture.ids, id)
		fixture.client.values["p:dynamic_group:"+id] = hostGroupOf(index + 1)
		if lookup := fixture.store.Group(context.Background(), id, time.Minute); lookup.Snapshot == nil || len(lookup.Snapshot.Members) != 1 {
			t.Fatalf("setup: group %s = %+v", id, lookup)
		}
	}
	return fixture
}

// write writes each of the ids' documents: empty, or its members back.
func (fixture *groupFixture) write(ids []string, empty bool) {
	for _, id := range ids {
		if empty {
			fixture.client.values["p:dynamic_group:"+id] = emptyHostGroup
			continue
		}
		var host int
		_, _ = fmt.Sscan(id, &host)
		fixture.client.values["p:dynamic_group:"+id] = hostGroupOf(host - 1999)
	}
}

// refresh refreshes times times, a minute apart, asking for every group
// after each.
func (fixture *groupFixture) refresh(t *testing.T, times int) {
	t.Helper()
	for step := 0; step < times; step++ {
		fixture.now = fixture.now.Add(time.Minute)
		if err := fixture.store.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, id := range fixture.ids {
			fixture.store.Group(context.Background(), id, time.Minute)
		}
	}
}

// selector is how a Plan of the one group resolves it now.
func (fixture *groupFixture) selector(id string) targetplan.SelectorResult {
	resolver := NewTargetResolver(fixture.store, nil, func() time.Time { return fixture.now })
	plan := &contract.TargetPlanV1{SchemaVersion: 1, ModelID: "cw-Host", Rule: contract.TargetPlanRuleHostID,
		Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true}, StaticKeys: []string{},
		DynamicGroups: []string{id}}
	return resolver.Resolve(context.Background(), plan, time.Minute).Selectors[0]
}

// servesMembers is whether the store still serves the group's members, held
// back, rather than its empty read.
func (fixture *groupFixture) servesMembers(t *testing.T, id string) bool {
	t.Helper()
	lookup := fixture.store.Group(context.Background(), id, time.Minute)
	if len(lookup.Snapshot.Members) == 1 && !lookup.EmptiedHeld {
		t.Fatalf("group %s serves members without being held: %+v", id, lookup)
	}
	return len(lookup.Snapshot.Members) == 1
}

// Every group that had members read empty at once is the writer answering
// nothing: the store holds back the reads, serves the snapshots before as
// past a failed refresh, says so once, and past the staleness bound names
// the groups emptied_held. It holds past a writer cycle. When the writer
// writes the members back, they are published and the end is said once.
func TestEveryGroupEmptyingAtOnceIsHeldAsTheWritersAndReleasedWhenItComesBack(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids, true)
	fixture.refresh(t, 1)
	for _, id := range fixture.ids {
		lookup := fixture.store.Group(context.Background(), id, time.Minute)
		if len(lookup.Snapshot.Members) != 1 || !lookup.EmptiedHeld || !lookup.RefreshFailed {
			t.Fatalf("group %s after every group emptied = %+v, want the snapshot before, held and served past the refresh", id, lookup)
		}
	}
	if health := fixture.store.Health(); health.EmptiedHeld != 3 || health.EmptiedPending != 3 || health.EmptiedHolds != 1 {
		t.Fatalf("health = %+v, want 3 held by 1 refresh", health)
	}
	if len(fixture.said) != 1 || fixture.said[0] != [2]int{3, 3} {
		t.Fatalf("said %v, want once: 3 held of 3 that had members", fixture.said)
	}

	fixture.refresh(t, 10)
	if selector := fixture.selector(fixture.ids[0]); selector.State != targetplan.SelectorUnavailable || selector.Reason != targetplan.ReasonEmptiedHeld {
		t.Fatalf("selector past the staleness bound = %+v, want unavailable as emptied_held", selector)
	}
	if fixture.store.Health().EmptiedHeld != 3 || len(fixture.said) != 1 {
		t.Fatalf("a hold past a writer cycle let go or was said again: %+v, %v", fixture.store.Health(), fixture.said)
	}

	fixture.write(fixture.ids, false)
	fixture.refresh(t, 1)
	lookup := fixture.store.Group(context.Background(), fixture.ids[0], time.Minute)
	if lookup.EmptiedHeld || lookup.RefreshFailed || len(lookup.Snapshot.Members) != 1 {
		t.Fatalf("group after the writer came back = %+v, want a fresh read of its members", lookup)
	}
	if health := fixture.store.Health(); health.EmptiedHeld != 0 || health.EmptiedPending != 0 {
		t.Fatalf("health after the writer came back = %+v", health)
	}
	if len(fixture.said) != 2 || fixture.said[1] != [2]int{0, 3} {
		t.Fatalf("said %v, want the hold's end once", fixture.said)
	}
}

// One group emptying among several is that group emptying - a service
// retired, a condition that matches nothing any more. It waits one writer
// cycle with the snapshot before served, never resolving OKEmpty in that
// time, then is published empty so its former members' alerts can close. The
// groups beside it are untouched and nothing is said.
func TestOneGroupEmptyingIsBelievedAfterAWriterCycle(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids[:1], true)
	// First read empty at minute 1; the cycle is out at minute 11.
	for minute := 1; minute <= 10; minute++ {
		fixture.refresh(t, 1)
		if !fixture.servesMembers(t, fixture.ids[0]) {
			t.Fatalf("minute %d: the group that emptied was believed before a writer cycle", minute)
		}
	}
	if health := fixture.store.Health(); health.EmptiedPending != 1 || health.EmptiedHeld != 0 {
		t.Fatalf("health while it waits = %+v, want 1 pending and none held", health)
	}
	fixture.refresh(t, 1)
	if fixture.servesMembers(t, fixture.ids[0]) {
		t.Fatal("the group that emptied was not believed after a writer cycle")
	}
	if selector := fixture.selector(fixture.ids[0]); selector.State != targetplan.SelectorOKEmpty {
		t.Fatalf("selector = %+v, want OKEmpty", selector)
	}
	if beside := fixture.store.Group(context.Background(), fixture.ids[1], time.Minute); beside.EmptiedHeld || len(beside.Snapshot.Members) != 1 {
		t.Fatalf("the group beside it moved: %+v", beside)
	}
	if len(fixture.said) != 0 || fixture.store.Health().EmptiedHolds != 0 {
		t.Fatalf("a group emptying alone was said or held: %v, %+v", fixture.said, fixture.store.Health())
	}
}

// An emptying whose writes land over two refreshes is judged as one: of five
// groups, three empty and then two, and all five are held, none read empty
// first; of fifty, ten a refresh, every one is held from the second refresh
// on and none is believed.
func TestAnEmptyingLandingOverSeveralRefreshesIsJudgedAsOne(t *testing.T) {
	fixture := newGroupFixture(t, 5)
	fixture.write(fixture.ids[:3], true)
	fixture.refresh(t, 1)
	fixture.write(fixture.ids[3:], true)
	fixture.refresh(t, 1)
	if held := fixture.store.Health().EmptiedHeld; held != 5 {
		t.Fatalf("held %d of 5 emptied over two refreshes, want all", held)
	}
	fixture.refresh(t, 10)
	for _, id := range fixture.ids {
		if !fixture.servesMembers(t, id) {
			t.Fatalf("group %s was read empty", id)
		}
	}

	fixture = newGroupFixture(t, 50)
	for step := 0; step < 5; step++ {
		fixture.write(fixture.ids[10*step:10*step+10], true)
		fixture.refresh(t, 1)
		if held := fixture.store.Health().EmptiedHeld; step >= 1 && held != 10*(step+1) {
			t.Fatalf("after %d refreshes held %d, want %d", step+1, held, 10*(step+1))
		}
	}
	fixture.refresh(t, 10)
	for _, id := range fixture.ids {
		if !fixture.servesMembers(t, id) {
			t.Fatalf("group %s of fifty was read empty", id)
		}
	}
}

// Some held groups coming back lets only those go: the rest are still held,
// served with their members and never read empty, and their wait starts
// over, since the writer writing members back is partway through a cycle.
// If they come back too, they are published; if they really are empty, they
// are believed a writer cycle after the last group came back.
func TestHeldGroupsComingBackOneByOneLetOnlyThoseGo(t *testing.T) {
	for _, test := range []struct {
		groups, back int
	}{{groups: 3, back: 1}, {groups: 50, back: 35}} {
		t.Run(fmt.Sprintf("%d of %d", test.back, test.groups), func(t *testing.T) {
			fixture := newGroupFixture(t, test.groups)
			fixture.write(fixture.ids, true)
			fixture.refresh(t, 5)
			fixture.write(fixture.ids[:test.back], false)
			fixture.refresh(t, 1)
			rest := fixture.ids[test.back:]
			for _, id := range rest {
				if !fixture.servesMembers(t, id) {
					t.Fatalf("group %s still out was read empty when others came back", id)
				}
			}
			fixture.refresh(t, 9)
			if !fixture.servesMembers(t, rest[0]) {
				t.Fatal("the wait did not start over when others came back")
			}

			// They really are empty: believed a cycle after the others came back.
			fixture.refresh(t, 1)
			if fixture.servesMembers(t, rest[0]) {
				t.Fatal("a group still empty a writer cycle after the rest came back was never believed")
			}
		})
	}

	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids, true)
	fixture.refresh(t, 2)
	fixture.write(fixture.ids[:1], false)
	fixture.refresh(t, 1)
	fixture.write(fixture.ids[1:], false)
	fixture.refresh(t, 1)
	if health := fixture.store.Health(); health.EmptiedPending != 0 || health.EmptiedHeld != 0 {
		t.Fatalf("after every group came back = %+v, want nothing held", health)
	}
}

// A group first read empty while others are held has no snapshot before it
// and may be one of them: it is emptied_held, not empty. Once the hold ends
// and it has read empty a writer cycle, it is believed.
func TestAGroupFirstReadDuringAHoldIsNotReadEmpty(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids, true)
	fixture.refresh(t, 1)
	fixture.client.values["p:dynamic_group:9999"] = emptyHostGroup
	fixture.ids = append(fixture.ids, "9999")
	if selector := fixture.selector("9999"); selector.State != targetplan.SelectorUnavailable || selector.Reason != targetplan.ReasonEmptiedHeld {
		t.Fatalf("a group first read during a hold = %+v, want unavailable as emptied_held", selector)
	}
	// The hold outlasts a writer cycle: the group waits with it.
	fixture.refresh(t, 11)
	if selector := fixture.selector("9999"); selector.Reason != targetplan.ReasonEmptiedHeld {
		t.Fatalf("while the hold outlasts a cycle = %+v, want still emptied_held", selector)
	}
	// The others come back, and its wait starts over from that refresh: it
	// is still waiting a cycle later less a minute, and read empty after.
	fixture.write(fixture.ids[:3], false)
	fixture.refresh(t, 10)
	if selector := fixture.selector("9999"); selector.Reason != targetplan.ReasonEmptiedHeld {
		t.Fatalf("before a writer cycle from the return = %+v, want still emptied_held", selector)
	}
	fixture.refresh(t, 1)
	if selector := fixture.selector("9999"); selector.State != targetplan.SelectorOKEmpty {
		t.Fatalf("a writer cycle after the return = %+v, want OKEmpty", selector)
	}
}

// A group first read empty while another group's emptying is pending -
// under the line, not held - waits with it: the one pending may be the start
// of an emptying whose rest has not landed yet.
func TestAGroupFirstReadWhileAnEmptyingIsPendingIsNotReadEmpty(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids[:1], true)
	fixture.refresh(t, 1)
	if health := fixture.store.Health(); health.EmptiedPending != 1 || health.EmptiedHeld != 0 {
		t.Fatalf("setup: health = %+v, want one pending and none held", health)
	}
	fixture.client.values["p:dynamic_group:9999"] = emptyHostGroup
	fixture.ids = append(fixture.ids, "9999")
	if selector := fixture.selector("9999"); selector.State != targetplan.SelectorUnavailable || selector.Reason != targetplan.ReasonEmptiedHeld {
		t.Fatalf("a group first read while an emptying is pending = %+v, want unavailable as emptied_held", selector)
	}
}

// The line, on both sides of each of its numbers: every group that had
// members (at least two), or at least 20 and more than a fifth of them. Past
// a writer cycle, a held emptying still serves its members and one below the
// line is read empty.
func TestAnEmptyingIsHeldOnlyPastTheLine(t *testing.T) {
	for _, test := range []struct {
		groups, emptied int
		held            bool
	}{
		{groups: 100, emptied: 20, held: false}, // a fifth exactly is not more than a fifth
		{groups: 100, emptied: 21, held: true},
		{groups: 90, emptied: 19, held: false}, // more than a fifth, under the floor
		{groups: 90, emptied: 20, held: true},
		{groups: 2, emptied: 2, held: true}, // every one, and at least two
		{groups: 3, emptied: 2, held: false},
		{groups: 1, emptied: 1, held: false}, // a store of one group cannot tell
	} {
		t.Run(fmt.Sprintf("%d of %d", test.emptied, test.groups), func(t *testing.T) {
			fixture := newGroupFixture(t, test.groups)
			fixture.write(fixture.ids[:test.emptied], true)
			fixture.refresh(t, 1)
			held := fixture.store.Health().EmptiedHeld
			if (held == test.emptied) != test.held || (held != 0 && held != test.emptied) {
				t.Fatalf("held %d, want held %v", held, test.held)
			}
			fixture.refresh(t, 10)
			if serves := fixture.servesMembers(t, fixture.ids[0]); serves != test.held {
				t.Fatalf("past a writer cycle serves members %v, want %v", serves, test.held)
			}
		})
	}
}

// Known boundary, pinned so that changing it is a decision: when every group
// a store references really empties, the hold does not end on its own. It
// ends when members come back (above), when nothing references the groups
// any more, or when the process restarts and reads what the writer has.
func TestEveryGroupReallyEmptyingIsHeldUntilItsReferencesOrTheProcessGo(t *testing.T) {
	fixture := newGroupFixture(t, 2)
	fixture.write(fixture.ids, true)
	fixture.refresh(t, 180)
	if health := fixture.store.Health(); health.EmptiedHeld != 2 || health.EmptiedHolds != 180 {
		t.Fatalf("after 180 refreshes = %+v, want still held, counted every refresh", health)
	}

	// Nothing references them any more.
	for step := 0; step < 11; step++ {
		fixture.now = fixture.now.Add(time.Minute)
		if err := fixture.store.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if health := fixture.store.Health(); health.EmptiedPending != 0 || health.EmptiedHeld != 0 || health.Referenced != 0 {
		t.Fatalf("after the references went = %+v, want nothing held", health)
	}

	// A new process reads what the writer has.
	restarted := newGroupFixture(t, 0)
	restarted.client.values["p:dynamic_group:1"] = emptyHostGroup
	if selector := restarted.selector("1"); selector.State != targetplan.SelectorOKEmpty {
		t.Fatalf("after a restart = %+v, want OKEmpty", selector)
	}
}

// A group whose members were all refused is not empty - it resolves
// incomplete - so it is not counted towards an emptying and not held back.
func TestAGroupWhoseMembersWereAllRefusedIsNotAnEmptying(t *testing.T) {
	fixture := newGroupFixture(t, 2)
	fixture.client.values["p:dynamic_group:"+fixture.ids[0]] = emptyHostGroup
	fixture.client.values["p:dynamic_group:"+fixture.ids[1]] = `{"model_id":"cw-Host","member_list":[{"model_id":"cw-MySQL","model_inst_id":"db-1"}]}`
	fixture.refresh(t, 1)
	if health := fixture.store.Health(); health.EmptiedHeld != 0 || health.EmptiedPending != 1 {
		t.Fatalf("health = %+v: one emptied and one refused is one pending and nothing held", health)
	}
	if lookup := fixture.store.Group(context.Background(), fixture.ids[1], time.Minute); lookup.EmptiedHeld || lookup.Snapshot.Dropped != 1 || len(lookup.Snapshot.Members) != 0 {
		t.Fatalf("the refused group = %+v, want its read published with the member dropped", lookup)
	}
}
