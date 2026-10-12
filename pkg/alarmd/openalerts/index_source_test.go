// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

func indexRedis(t *testing.T) *redis.Client {
	t.Helper()
	executable := redistest.Server(t)
	address := reserveTCPAddress(t)
	startRedisServer(t, executable, address)
	client := redis.NewClient(&redis.Options{Addr: address, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })
	waitRedisReady(t, client)
	return client
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	return redistest.FreeAddress(t)
}

func startRedisServer(t *testing.T, _ string, address string) {
	t.Helper()
	redistest.StartAt(t, address)
}

func waitRedisReady(t *testing.T, client *redis.Client) {
	t.Helper()
	// The server answered before StartAt returned; what is left is this
	// client's own connection, under the same bound.
	deadline := time.Now().Add(redistest.ReadyWithin)
	for time.Now().Before(deadline) {
		if err := client.Ping(context.Background()).Err(); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("redis-server did not become ready")
}

func TestSetSourceReadsWithoutHeartbeatAndBoundsPayload(t *testing.T) {
	client := indexRedis(t)
	ctx := context.Background()
	source, err := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 2, MaxBytes: 20, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := "test:index:" + keyA.TenantID + ":" + keyA.StrategyID
	if err := client.SAdd(ctx, key, "one").Err(); err != nil {
		t.Fatal(err)
	}
	members, err := source.ReadSet(ctx, keyA)
	if err != nil || !reflect.DeepEqual(members, []string{"one"}) {
		t.Fatalf("members %v error %v", members, err)
	}
	if err := client.SAdd(ctx, key, "two", "three").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadSet(ctx, keyA); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("member limit: %v", err)
	}
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if members, err := source.ReadSet(ctx, keyA); err != nil || len(members) != 0 {
		t.Fatalf("deleted key = %v, %v", members, err)
	}
	if err := client.SAdd(ctx, key, "this-member-exceeds-twenty-bytes").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadSet(ctx, keyA); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("byte limit: %v", err)
	}
	if _, err := source.ReadSet(ctx, StrategyKey{TenantID: "a:b", StrategyID: "c"}); err == nil {
		t.Fatal("tenant alias accepted")
	}
}

func TestRedisSubscriberAcknowledgesReconnectAndFiltersInvalidNotices(t *testing.T) {
	client := indexRedis(t)
	subscriber, err := NewRedisSubscriber(client, "test:index", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan bool, 20)
	changes := make(chan StrategyKey, 20)
	done := make(chan error, 1)
	var refusedMu sync.Mutex
	refused := map[NoticeRefusal]int{}
	go func() {
		done <- subscriber.Watch(ctx, func(value bool) { ready <- value }, func(key StrategyKey) { changes <- key },
			func(reason NoticeRefusal) { refusedMu.Lock(); refused[reason]++; refusedMu.Unlock() })
	}()
	waitReady := func(wanted bool) {
		t.Helper()
		select {
		case actual := <-ready:
			if actual != wanted {
				t.Fatalf("ready %v want %v", actual, wanted)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no subscription acknowledgement")
		}
	}
	waitReady(true)
	// Every notice dropped is named by why, never silently: a field this
	// build does not know, a wrong type, content after the notice, a key
	// that is not a valid tenant and strategy, and a payload past the bound.
	oversized := `{"bk_tenant_id":"x","strategy_id":"` + strings.Repeat("y", 64<<10) + `"}`
	for _, payload := range []string{`{"bk_tenant_id":"x","strategy_id":"y","key":"foreign"}`, `{"bk_tenant_id":"x","strategy_id":1}`,
		`{"bk_tenant_id":"x","strategy_id":"y"} {}`, `{"bk_tenant_id":"a:b","strategy_id":"c"}`, oversized} {
		if err := client.Publish(ctx, "test:index:changes", payload).Err(); err != nil {
			t.Fatal(err)
		}
	}
	valid := `{"bk_tenant_id":"default","strategy_id":"1001"}`
	if err := client.Publish(ctx, "test:index:changes", valid).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case key := <-changes:
		if key != keyA {
			t.Fatalf("unexpected notice %+v", key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("valid notice lost")
	}
	refusedMu.Lock()
	if want := map[NoticeRefusal]int{NoticeUndecodable: 3, NoticeInvalidKey: 1, NoticeOversized: 1}; !reflect.DeepEqual(refused, want) {
		t.Fatalf("refused = %v, want %v", refused, want)
	}
	refusedMu.Unlock()
	if err := client.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}
	waitReady(false)
	// Pub/Sub has no replay. A reconnect acknowledgement is the signal the
	// cache uses to reread this mutation, which intentionally has no notice.
	if err := client.SAdd(ctx, "test:index:default:1001", "missed").Err(); err != nil {
		t.Fatal(err)
	}
	waitReady(true)
	source, _ := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 10, MaxBytes: 100, PageSize: 10})
	if members, err := source.ReadSet(ctx, keyA); err != nil || !reflect.DeepEqual(members, []string{"missed"}) {
		t.Fatalf("reconnect read %v, %v", members, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("subscriber did not close")
	}
}

// The read's size bounds refuse a set as too large - more members than the
// read may hold before or while it reads, more bytes, more pages - and a
// member no alert id can be refuses it as incomplete only: the copy counts
// the first apart (UnavailableTooLarge) and the second as a failed read.
func TestASetBeyondTheReadsBoundsIsRefusedAsTooLarge(t *testing.T) {
	client := indexRedis(t)
	ctx := context.Background()
	key := "test:index:" + keyA.TenantID + ":" + keyA.StrategyID
	read := func(limits ReadLimits, members ...string) error {
		t.Helper()
		if err := client.Del(ctx, key).Err(); err != nil {
			t.Fatal(err)
		}
		values := make([]any, 0, len(members))
		for _, member := range members {
			values = append(values, member)
		}
		if err := client.SAdd(ctx, key, values...).Err(); err != nil {
			t.Fatal(err)
		}
		source, err := NewSetSource(client, "test:index", limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = source.ReadSet(ctx, keyA)
		return err
	}
	wide := ReadLimits{MaxMembers: 10, MaxBytes: 1 << 20, PageSize: 10}
	for name, c := range map[string]struct {
		limits   ReadLimits
		members  []string
		tooLarge bool
	}{
		"more members than the read holds": {ReadLimits{MaxMembers: 2, MaxBytes: 1 << 20, PageSize: 10}, []string{"a", "b", "c"}, true},
		"more bytes than the read holds":   {ReadLimits{MaxMembers: 10, MaxBytes: 4, PageSize: 10}, []string{"abc", "def"}, true},
		"a member no alert id can be":      {wide, []string{strings.Repeat("x", 4097)}, false},
	} {
		err := read(c.limits, c.members...)
		if !errors.Is(err, ErrIncomplete) || errors.Is(err, ErrSetTooLarge) != c.tooLarge {
			t.Errorf("%s: %v, want incomplete and too large %t", name, err, c.tooLarge)
		}
	}
}

// A set is read until the scan's cursor wraps, however many pages that is:
// past the size a set is kept compact in, 2000 members scanned a member at a
// time - far more than a hundred pages - are read whole, each once. A page
// count is no bound on a read.
func TestASetIsReadWholeHoweverManyPages(t *testing.T) {
	client := indexRedis(t)
	ctx := context.Background()
	key := "test:index:" + keyA.TenantID + ":" + keyA.StrategyID
	values := make([]any, 0, 2000)
	for index := 0; index < 2000; index++ {
		values = append(values, "member-"+strconv.Itoa(index))
	}
	if err := client.SAdd(ctx, key, values...).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 10000, MaxBytes: 1 << 20, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	members, err := source.ReadSet(ctx, keyA)
	if err != nil || len(members) != 2000 {
		t.Fatalf("read %d members, %v; want all 2000", len(members), err)
	}
}

// Once a complete read has measured what a member weighs, a set whose
// estimated bytes exceed the budget is refused before it is scanned, and the
// refusal says how many members and the estimate; before any is measured
// the member count alone decides.
func TestASetIsRefusedOnItsEstimateBeforeItIsRead(t *testing.T) {
	client := indexRedis(t)
	ctx := context.Background()
	small, large := "test:index:"+keyA.TenantID+":"+keyA.StrategyID, "test:index:"+keyB.TenantID+":"+keyB.StrategyID
	for index := 0; index < 4; index++ {
		client.SAdd(ctx, small, fmt.Sprintf("member-%04d", index))
	}
	for index := 0; index < 20; index++ {
		client.SAdd(ctx, large, fmt.Sprintf("member-%04d", index))
	}
	source, _ := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 100, MaxBytes: 150, PageSize: 100})
	if _, err := source.ReadSet(ctx, keyA); err != nil {
		t.Fatal(err)
	}
	_, err := source.ReadSet(ctx, keyB)
	var refusal *SetTooLargeError
	if !errors.As(err, &refusal) || !errors.Is(err, ErrSetTooLarge) || refusal.Members != 20 || refusal.Bytes != 220 || refusal.MaxBytes != 150 {
		t.Fatalf("refusal %+v (%v), want 20 members estimated at 220 bytes against 150", refusal, err)
	}
	fresh, _ := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 10, MaxBytes: 1 << 20, PageSize: 100})
	if _, err := fresh.ReadSet(ctx, keyB); !errors.As(err, &refusal) || refusal.Members != 20 || refusal.Bytes != 0 {
		t.Fatalf("an unmeasured source refused %+v (%v), want the 20 members against 10", refusal, err)
	}
}

// A set that fits but would not be read inside the caller's deadline is
// handed back to be read on its own, with a budget for its pages; read on its
// own, it is read whole.
func TestASetTooSlowForTheRoundIsReadOnItsOwn(t *testing.T) {
	client := indexRedis(t)
	ctx := context.Background()
	key := "test:index:" + keyA.TenantID + ":" + keyA.StrategyID
	for index := 0; index < 50; index++ {
		client.SAdd(ctx, key, "member-"+strconv.Itoa(index))
	}
	source, _ := NewSetSource(client, "test:index", ReadLimits{MaxMembers: 1000, MaxBytes: 1 << 20, PageSize: 1, MaxLargeRead: time.Minute})
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := source.ReadSet(short, keyA)
	var large *LargeSetError
	if !errors.As(err, &large) || large.Members != 50 || large.Budget < 2500*time.Millisecond || large.Budget > time.Minute {
		t.Fatalf("a slow set in a short round = %+v (%v), want handed back with its budget", large, err)
	}
	own, cancelOwn := context.WithTimeout(WithLargeRead(ctx), large.Budget)
	defer cancelOwn()
	if members, err := source.ReadSet(own, keyA); err != nil || len(members) != 50 {
		t.Fatalf("read on its own: %d members, %v", len(members), err)
	}
}
