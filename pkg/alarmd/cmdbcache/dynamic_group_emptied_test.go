// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisbatch"
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

// A read over many groups is a window at a time: a window's documents add
// up to at most the bound, or are the one larger document, and each is
// handed over, in order, before the next window is read - the next pipeline
// overwrites a window's replies, so a document handed over late would not
// decode. With documents of one size and a bound of four and a half of
// them, a window is four documents, one pipeline of lengths and one of
// documents, whatever the number of groups: doubling the groups doubles the
// round trips, not what is held at once. A transport failure part way
// returns the error after the windows before it were handed over.
func TestAGroupReadIsAWindowAtATimeAndAFailurePartWayReturnsTheError(t *testing.T) {
	for _, count := range []int{40, 80} {
		client := &groupClient{values: map[string]string{}}
		ids := make([]string, 0, count)
		for index := 0; index < count; index++ {
			id := fmt.Sprint(1000 + index)
			ids = append(ids, id)
			client.values["p:dynamic_group:"+id] = hostGroupOf(100 + index)
		}
		size := len(client.values["p:dynamic_group:1000"])
		reader, err := NewGroupReader(client, "p:")
		if err != nil {
			t.Fatal(err)
		}
		visited, ahead := []string{}, 0
		visit := func(id string, read GroupRead) {
			if snapshot := snapshotOf(id, read, time.Time{}); len(snapshot.Members) != 1 {
				t.Fatalf("%d groups: %s handed over as %+v", count, id, snapshot)
			}
			visited = append(visited, id)
			ahead = max(ahead, client.gets()-len(visited))
		}
		if err := reader.Read(context.Background(), ids, size*9/2, visit); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(visited) != fmt.Sprint(ids) || ahead != 3 {
			t.Fatalf("%d groups: handed over %d, at most %d read ahead; want all in order, 3 ahead", count, len(visited), ahead)
		}
		if len(client.calls) != count/4 || client.strlens != count {
			t.Fatalf("%d groups: %d pipelines of documents, %d lengths; want %d and %d", count, len(client.calls), client.strlens, count/4, count)
		}
		for _, call := range client.calls {
			if len(call) != 4 {
				t.Fatalf("%d groups: a window of %d documents, want 4", count, len(call))
			}
		}

		client.calls, client.failAt, visited = nil, 2, nil
		if err := reader.Read(context.Background(), ids, size*9/2, visit); err == nil || len(visited) != 4 {
			t.Fatalf("%d groups: a read whose second window failed handed over %d and returned %v; want 4 and the error", count, len(visited), err)
		}
	}
}

// A document larger than the bound is read alone, and read: every read
// makes progress. A key Redis answers with an error says nothing about its
// group: it is not handed over, the groups beside it are, and the read
// returns an error counting it.
func TestALargeDocumentIsReadAloneAndAnAnsweredKeyIsNotHandedOver(t *testing.T) {
	client := &groupClient{values: map[string]string{
		"p:dynamic_group:1": hostGroupOf(101), "p:dynamic_group:2": hostGroupOf(102), "p:dynamic_group:3": hostGroupOf(103)},
		answered: map[string]error{"p:dynamic_group:2": answeredError("WRONGTYPE Operation against a key holding the wrong kind of value")}}
	reader, err := NewGroupReader(client, "p:")
	if err != nil {
		t.Fatal(err)
	}
	handed := []string{}
	err = reader.Read(context.Background(), []string{"1", "2", "3"}, 1, func(id string, read GroupRead) {
		handed = append(handed, fmt.Sprintf("%s=%d", id, len(snapshotOf(id, read, time.Time{}).Members)))
	})
	var unanswered *redisbatch.UnansweredError
	if !errors.As(err, &unanswered) || unanswered.Keys != 1 || fmt.Sprint(handed) != "[1=1 3=1]" || len(client.calls) != 3 {
		t.Fatalf("handed %v in %d pipelines of documents and returned %v; want 1 and 3, one a pipeline, and group 2 unanswered",
			handed, len(client.calls), err)
	}
}

// answerEvery has Redis answer every group's key with err, or, nil, answer
// them again.
func (fixture *groupFixture) answerEvery(err error) {
	fixture.client.answered = map[string]error{}
	if err == nil {
		return
	}
	for _, id := range fixture.ids {
		fixture.client.answered["p:dynamic_group:"+id] = err
	}
}

// Redis answering every key with an error - LOADING while it restarts -
// says nothing about the groups: the refresh keeps every snapshot, serves
// each as past a failed refresh, fails, and counts the groups it could not
// read. Answered again, the groups are read as before.
func TestARefreshRedisAnswersEveryKeyWithAnErrorKeepsEverySnapshot(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.answerEvery(answeredError("LOADING Redis is loading the dataset in memory"))
	fixture.now = fixture.now.Add(time.Minute)
	err := fixture.store.Refresh(context.Background())
	var unanswered *redisbatch.UnansweredError
	if !errors.As(err, &unanswered) || unanswered.Keys != 3 {
		t.Fatalf("the refresh returned %v, want every group unanswered", err)
	}
	for _, id := range fixture.ids {
		lookup := fixture.store.Group(context.Background(), id, time.Minute)
		if lookup.Snapshot == nil || lookup.Snapshot.Unavailable != "" || len(lookup.Snapshot.Members) != 1 || !lookup.RefreshFailed {
			t.Fatalf("group %s after an unanswered refresh = %+v, %+v; want its members, past a failed refresh", id, lookup, lookup.Snapshot)
		}
	}
	if health := fixture.store.Health(); !health.RefreshFailed || health.Unanswered != 3 || health.UnansweredReads != 3 ||
		health.Loaded != 3 {
		t.Fatalf("health after an unanswered refresh = %+v", health)
	}

	fixture.answerEvery(nil)
	fixture.now = fixture.now.Add(time.Minute)
	if err := fixture.store.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lookup := fixture.store.Group(context.Background(), fixture.ids[0], time.Minute); lookup.RefreshFailed || lookup.Age != 0 {
		t.Fatalf("group after an answered refresh = %+v", lookup)
	}
	if health := fixture.store.Health(); health.RefreshFailed || health.Unanswered != 0 || health.UnansweredReads != 3 {
		t.Fatalf("health after an answered refresh = %+v", health)
	}
}

// The health names each group served past refreshes that could not read
// it: since the first of them after its last read, which a second does not
// move, and why the latest could not, in closed words - the code Redis
// answered its key with, or transport. A group never read is not served past anything and is not
// named; a read of the group, or its reference ageing out, ends its run.
func TestTheHealthNamesEachGroupServedPastAFailedRefresh(t *testing.T) {
	fixture := newGroupFixture(t, 2)
	loading := answeredError("LOADING Redis is loading the dataset in memory")
	// A group whose key Redis answers with an error throughout is never read.
	fixture.client.values["p:dynamic_group:never"] = hostGroupOf(9)
	answer := func(answered map[string]error) {
		answered["p:dynamic_group:never"] = loading
		fixture.client.answered = answered
	}
	answer(map[string]error{})
	if lookup := fixture.store.Group(context.Background(), "never", time.Minute); lookup.ReadErr == nil {
		t.Fatal("setup: a group never read was read")
	}
	answer(map[string]error{"p:dynamic_group:" + fixture.ids[0]: loading, "p:dynamic_group:" + fixture.ids[1]: loading})
	fixture.now = fixture.now.Add(time.Minute)
	started := fixture.now
	_ = fixture.store.Refresh(context.Background())
	failing := fixture.store.Health().Failing
	if len(failing) != 2 || failing[0].ID != fixture.ids[0] || failing[1].ID != fixture.ids[1] || !failing[0].Since.Equal(started) ||
		failing[0].Reason != "LOADING" {
		t.Fatalf("failing after an unanswered refresh = %+v", failing)
	}

	answer(map[string]error{})
	fixture.client.err = errors.New("connection refused")
	fixture.now = fixture.now.Add(time.Minute)
	_ = fixture.store.Refresh(context.Background())
	failing = fixture.store.Health().Failing
	if len(failing) != 2 || !failing[1].Since.Equal(started) || failing[1].Reason != "transport" {
		t.Fatalf("failing after a refresh that failed at the transport = %+v, want the run's start kept and the latest reason", failing)
	}

	fixture.client.err = nil
	answer(map[string]error{"p:dynamic_group:" + fixture.ids[1]: answeredError("WRONGTYPE Operation against a key holding the wrong kind of value")})
	fixture.now = fixture.now.Add(time.Minute)
	_ = fixture.store.Refresh(context.Background())
	if failing = fixture.store.Health().Failing; len(failing) != 1 || failing[0].ID != fixture.ids[1] || failing[0].Reason != "WRONGTYPE" {
		t.Fatalf("failing after the first group read again = %+v, want only the second", failing)
	}

	// Nobody asks for the second group any more: past its horizon it leaves
	// the refresh and the health, run and all.
	for step := 0; step < 12; step++ {
		fixture.now = fixture.now.Add(time.Minute)
		_ = fixture.store.Refresh(context.Background())
		fixture.store.Group(context.Background(), fixture.ids[0], time.Minute)
	}
	if failing = fixture.store.Health().Failing; len(failing) != 0 {
		t.Fatalf("failing after the group aged out = %+v, want none", failing)
	}
}

// One group Redis answers with an error keeps its snapshot and is served as
// past a failed refresh; the groups beside it are refreshed as usual.
func TestAGroupRedisAnswersWithAnErrorKeepsOnlyItsOwnSnapshot(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	for _, id := range fixture.ids {
		fixture.client.values["p:dynamic_group:"+id] = hostGroupOf(500, 501)
	}
	fixture.client.answered = map[string]error{
		"p:dynamic_group:" + fixture.ids[1]: answeredError("WRONGTYPE Operation against a key holding the wrong kind of value")}
	fixture.now = fixture.now.Add(time.Minute)
	if err := fixture.store.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh with a group unanswered did not fail")
	}
	for index, id := range fixture.ids {
		lookup := fixture.store.Group(context.Background(), id, time.Minute)
		want, failed := 2, false
		if index == 1 {
			want, failed = 1, true
		}
		if len(lookup.Snapshot.Members) != want || lookup.RefreshFailed != failed {
			t.Fatalf("group %s = %d members, refresh failed %v; want %d, %v", id, len(lookup.Snapshot.Members), lookup.RefreshFailed, want, failed)
		}
	}
}

// A group's first read that Redis answers with an error publishes nothing:
// the lookup says the read failed, and the next ask reads it again.
func TestAFirstReadRedisAnswersWithAnErrorPublishesNothing(t *testing.T) {
	client := &groupClient{values: map[string]string{"p:dynamic_group:7": hostGroupOf(7)},
		answered: map[string]error{"p:dynamic_group:7": answeredError("LOADING Redis is loading the dataset in memory")}}
	reader, err := NewGroupReader(client, "p:")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, ReadBound: testGroupReadBound})
	if err != nil {
		t.Fatal(err)
	}
	if lookup := store.Group(context.Background(), "7", time.Minute); lookup.ReadErr == nil || lookup.Snapshot != nil {
		t.Fatalf("a first read Redis answered with an error = %+v, want the error and no snapshot", lookup)
	}
	if health := store.Health(); health.Loaded != 0 || health.Unavailable != 0 {
		t.Fatalf("health after it = %+v, want nothing held", health)
	}
	client.answered = nil
	if lookup := store.Group(context.Background(), "7", time.Minute); lookup.ReadErr != nil || lookup.Snapshot == nil ||
		len(lookup.Snapshot.Members) != 1 || store.Health().SyncReads != 2 {
		t.Fatalf("the next ask = %+v after %d reads, want the group read again", lookup, store.Health().SyncReads)
	}
}

// A refresh decodes each window as it is read: with a bound of four and a
// half documents over forty groups, every group comes out with its member,
// none decoded from a window the next one overwrote.
func TestARefreshDecodesEachWindowBeforeReadingTheNext(t *testing.T) {
	fixture := newGroupFixture(t, 40)
	size := len(fixture.client.values["p:dynamic_group:"+fixture.ids[0]])
	fixture.store.readBound = size * 9 / 2
	fixture.client.calls = nil
	fixture.refresh(t, 1)
	if len(fixture.client.calls) != 10 {
		t.Fatalf("the refresh read %d windows, want 10 of 4", len(fixture.client.calls))
	}
	for _, id := range fixture.ids {
		if lookup := fixture.store.Group(context.Background(), id, time.Minute); lookup.Snapshot == nil ||
			lookup.Snapshot.Unavailable != "" || len(lookup.Snapshot.Members) != 1 {
			t.Fatalf("group %s after the refresh = %+v", id, lookup.Snapshot)
		}
	}
}

// A store without a read bound is refused: a bound of zero would read one
// document a round trip, and no bound at all every document at once.
func TestAGroupStoreNeedsAReadBound(t *testing.T) {
	reader, err := NewGroupReader(&groupClient{}, "p:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute}); err == nil {
		t.Fatal("a store without a read bound was built")
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
}

func newGroupFixture(t *testing.T, count int) *groupFixture {
	t.Helper()
	fixture := &groupFixture{client: &groupClient{values: map[string]string{}}, now: time.Unix(1000, 0)}
	reader, err := NewGroupReader(fixture.client, "p:")
	if err != nil {
		t.Fatal(err)
	}
	fixture.store, err = NewGroupStore(reader, GroupStoreOptions{RefreshInterval: time.Minute, MaxAge: 10 * time.Minute, ReadBound: testGroupReadBound,
		Now: func() time.Time { return fixture.now }})
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

// A group whose members were all refused is not empty: its read is published
// with the members dropped, and it resolves incomplete, not as the normal
// empty answer an explicit empty list is.
func TestAGroupWhoseMembersWereAllRefusedIsNotAnEmptying(t *testing.T) {
	fixture := newGroupFixture(t, 2)
	fixture.client.values["p:dynamic_group:"+fixture.ids[0]] = emptyHostGroup
	fixture.client.values["p:dynamic_group:"+fixture.ids[1]] = `{"model_id":"cw-Host","member_list":[{"model_id":"cw-MySQL","model_inst_id":"db-1"}]}`
	fixture.refresh(t, 1)
	if lookup := fixture.store.Group(context.Background(), fixture.ids[1], time.Minute); lookup.Snapshot.Dropped != 1 || len(lookup.Snapshot.Members) != 0 {
		t.Fatalf("the refused group = %+v, want its read published with the member dropped", lookup)
	}
	if selector := fixture.selector(fixture.ids[1]); selector.State != targetplan.SelectorIncomplete {
		t.Fatalf("the refused group resolves %s/%s, want incomplete", selector.State, selector.Reason)
	}
	if selector := fixture.selector(fixture.ids[0]); selector.State != targetplan.SelectorOKEmpty {
		t.Fatalf("the emptied group beside it resolves %s/%s, want OKEmpty", selector.State, selector.Reason)
	}
}

// Why a refresh could not read a group is said in closed words, never the
// error's text: the code Redis answered with, redis_error for a reply that
// leads with none, timed_out past a deadline or on a cancel, and transport
// for the rest - a dial failure among them, whose text names the endpoint.
func TestAGroupFailureIsNamedInClosedWords(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
		Err: errors.New("connect: connection refused")}
	for _, test := range []struct {
		err  error
		want string
	}{
		{err: answeredError("LOADING Redis is loading the dataset in memory"), want: "LOADING"},
		{err: fmt.Errorf("p:dynamic_group:7: %w", answeredError("WRONGTYPE Operation against a key holding the wrong kind of value")), want: "WRONGTYPE"},
		{err: answeredError("something went wrong at 127.0.0.1:1"), want: "redis_error"},
		{err: fmt.Errorf("alarmd cmdbcache: read dynamic groups: %w", context.DeadlineExceeded), want: "timed_out"},
		{err: context.Canceled, want: "timed_out"},
		{err: &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, want: "timed_out"},
		{err: fmt.Errorf("alarmd cmdbcache: read dynamic groups: %w", dial), want: "transport"},
	} {
		if got := groupFailureReason(test.err); got != test.want {
			t.Fatalf("%v = %q, want %q", test.err, got, test.want)
		}
	}
}

// A group whose writer wrote an explicit empty member list is a normal empty
// answer at the next refresh: no members, OKEmpty (decision-017 section 4,
// the E ruling: a key that is there, whose structure and root model check
// out and that says member_list: [] with no contradicting model_inst_ids).
// Nothing waits a writer cycle for it. Written back, its members return.
func TestAGroupWrittenEmptyIsANormalEmptyAnswer(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids[:1], true)
	fixture.refresh(t, 1)
	if selector := fixture.selector(fixture.ids[0]); selector.State != targetplan.SelectorOKEmpty || len(selector.Members) != 0 {
		t.Fatalf("group written empty resolves %s/%s with %d members, want OKEmpty with none", selector.State, selector.Reason, len(selector.Members))
	}
	if beside := fixture.selector(fixture.ids[1]); beside.State != targetplan.SelectorOK || len(beside.Members) != 1 {
		t.Fatalf("the group beside it moved: %s with %d members", beside.State, len(beside.Members))
	}
	fixture.write(fixture.ids[:1], false)
	fixture.refresh(t, 1)
	if selector := fixture.selector(fixture.ids[0]); selector.State != targetplan.SelectorOK || len(selector.Members) != 1 {
		t.Fatalf("group written back resolves %s with %d members", selector.State, len(selector.Members))
	}
}

// Groups emptied together are each that answer too: the user ruled out
// judging a share of groups emptying as the writer's fault (10-08), and a
// writer failure arrives as a missing key, a document that does not decode,
// or a read that fails - each its own state - not as member_list: [].
func TestGroupsWrittenEmptyTogetherAreEachANormalEmptyAnswer(t *testing.T) {
	fixture := newGroupFixture(t, 3)
	fixture.write(fixture.ids, true)
	fixture.refresh(t, 1)
	for _, id := range fixture.ids {
		if selector := fixture.selector(id); selector.State != targetplan.SelectorOKEmpty || len(selector.Members) != 0 {
			t.Errorf("group %s emptied with the others resolves %s/%s with %d members", id, selector.State, selector.Reason, len(selector.Members))
		}
	}
}

// A group whose first read is an explicit empty list is the same answer.
func TestAGroupFirstReadEmptyIsANormalEmptyAnswer(t *testing.T) {
	fixture := newGroupFixture(t, 2)
	fixture.write(fixture.ids[:1], true)
	fixture.refresh(t, 1)
	fixture.client.values["p:dynamic_group:3000"] = emptyHostGroup
	fixture.ids = append(fixture.ids, "3000")
	if selector := fixture.selector("3000"); selector.State != targetplan.SelectorOKEmpty || len(selector.Members) != 0 {
		t.Fatalf("a group first read empty resolves %s/%s with %d members", selector.State, selector.Reason, len(selector.Members))
	}
}
