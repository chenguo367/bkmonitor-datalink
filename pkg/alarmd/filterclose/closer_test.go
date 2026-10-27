// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package filterclose

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
)

// Every builder puts the tenant, the event source and the strategy in the
// filter, and a filter missing any of the three is refused before a
// request is made.
func TestEveryFilterCarriesTenantSourceAndStrategy(t *testing.T) {
	builders := map[string]func() (Filter, error){
		"strategy": func() (Filter, error) { return StrategyFilter(testScope) },
		"version":  func() (Filter, error) { return VersionFilter(testScope, "4") },
		"bucket": func() (Filter, error) {
			return BucketFilter(testScope, Bucket{Version: "4", Digest: strings.Repeat("a", 32)})
		},
		"no digest": func() (Filter, error) {
			return BucketFilter(testScope, Bucket{Version: "4", NoData: true})
		},
		"level":       func() (Filter, error) { return LevelFilter(testScope, []string{"3", "4"}, []string{"info"}) },
		"enumeration": func() (Filter, error) { return enumerationFilter(testScope, "5", []Bucket{{Version: "4"}}) },
		"targets": func() (Filter, error) {
			filters, err := TargetFilters(testScope, []Target{{"bk_target_ip": "192.0.2.1"}})
			if err != nil {
				return Filter{}, err
			}
			return filters[0], nil
		},
	}
	for name, build := range builders {
		filter, err := build()
		if err != nil || requireScope(filter) != nil {
			t.Fatalf("%s: %v / %v", name, err, requireScope(filter))
		}
	}
	for _, scope := range []Scope{{EventSourceID: "built_in_bk", StrategyID: "41"}, {TenantID: "tenant-a", StrategyID: "41"},
		{TenantID: "tenant-a", EventSourceID: "built_in_bk"}, {TenantID: "tenant-a", EventSourceID: "built_in_bk", StrategyID: "041"}} {
		if _, err := StrategyFilter(scope); err == nil {
			t.Fatalf("scope %+v built", scope)
		}
	}
	h := newHarness(t)
	closer := h.closer(t)
	missing := map[string]string{
		"tenant":   `{"bool":{"filter":[{"term":{"event_source_id":"built_in_bk"}},{"term":{"labels.strategy_id":"41"}}]}}`,
		"source":   `{"bool":{"filter":[{"term":{"bk_tenant_id":"tenant-a"}},{"term":{"labels.strategy_id":"41"}}]}}`,
		"strategy": `{"bool":{"filter":[{"term":{"bk_tenant_id":"tenant-a"}},{"term":{"event_source_id":"built_in_bk"}}]}}`,
		"another":  `{"bool":{"filter":[{"term":{"bk_tenant_id":"tenant-a"}},{"term":{"event_source_id":"built_in_bk"}},{"term":{"labels.strategy_id":"42"}}]}}`,
		"nested":   `{"bool":{"should":[{"bool":{"filter":[{"term":{"bk_tenant_id":"tenant-a"}},{"term":{"event_source_id":"built_in_bk"}},{"term":{"labels.strategy_id":"41"}}]}}]}}`,
	}
	for name, body := range missing {
		word := closer.Run(context.Background(), Job{Filter: Filter{scope: testScope, body: json.RawMessage(body)}, Decision: DecisionAbsent,
			Expect: Expectation{Count: 10, Source: ExpectDecidedIDs}, Reason: "r"})
		if word != OutcomeFilterRefused {
			t.Fatalf("%s: %s", name, word)
		}
	}
	if len(h.link.requests) != 0 {
		t.Fatalf("requests %v, want none for an unscoped filter", h.link.requests)
	}
}

// A dry run that matches more than the expectation allows closes nothing;
// within it, the strategy's alerts close and another strategy's, another
// source's and another tenant's do not.
func TestADryRunPastTheExpectationClosesNothing(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 50; i++ {
		h.link.add(&fakeAlert{id: alertID(i), labels: map[string]string{"strategy_id": "41", "strategy_version": "3"}})
	}
	h.link.add(&fakeAlert{id: "other-strategy", labels: map[string]string{"strategy_id": "42"}})
	h.link.add(&fakeAlert{id: "other-source", source: "another", labels: map[string]string{"strategy_id": "41"}})
	h.link.add(&fakeAlert{id: "other-tenant", tenant: "tenant-b", labels: map[string]string{"strategy_id": "41"}})
	closer := h.closer(t)
	if word := closer.Absent(context.Background(), testScope, DecisionAbsent, 3, Expectation{Count: 10, Source: ExpectDecidedIDs}); word != OutcomeExpectExceeded {
		t.Fatalf("word %s, want expect_exceeded", word)
	}
	if h.link.requests[linkdmanage.RouteCloseFilter] == 0 || h.link.alert(alertID(0)).closes != 0 {
		t.Fatalf("closed past the expectation: %+v", h.link.alert(alertID(0)))
	}
	if word := closer.Absent(context.Background(), testScope, DecisionAbsent, 3, Expectation{Count: 48, Source: ExpectDecidedIDs}); word != OutcomeJobDone {
		t.Fatalf("word %s, want job_done", word)
	}
	for i := 0; i < 50; i++ {
		if h.link.alert(alertID(i)).closes != 1 {
			t.Fatalf("%s closes %d", alertID(i), h.link.alert(alertID(i)).closes)
		}
	}
	for _, id := range []string{"other-strategy", "other-source", "other-tenant"} {
		if h.link.alert(id).closes != 0 {
			t.Fatalf("%s closed", id)
		}
	}
	// The fixed ceiling is a bound as it is, with no margin.
	if (Expectation{Count: FixedCeiling, Source: ExpectFixedCeiling}).allowed() != FixedCeiling {
		t.Fatal("the fixed ceiling has a margin")
	}
}

// Removing targets (A, 1) and (B, 0) of a strategy whose alerts are keyed
// by IP and cloud area closes those two and not the kept (A, 0): every key
// of a target matches together. Many targets are cut within the clause and
// body bounds.
func TestARemovedMultiKeyTargetClosesOnlyItsOwnAlerts(t *testing.T) {
	h := newHarness(t)
	for id, target := range map[string][2]string{"a1": {"192.0.2.1", "1"}, "b0": {"192.0.2.2", "0"}, "a0": {"192.0.2.1", "0"}} {
		h.link.add(&fakeAlert{id: id, labels: map[string]string{"strategy_id": "41"},
			dimensions: map[string]string{"bk_target_ip": target[0], "bk_target_cloud_id": target[1]}})
	}
	closer := h.closer(t)
	words := closer.RemovedTargets(context.Background(), testScope, []Target{
		{"bk_target_ip": "192.0.2.1", "bk_target_cloud_id": "1"}, {"bk_target_ip": "192.0.2.2", "bk_target_cloud_id": "0"}}, 1, 7, DecisionScope)
	if len(words) != 1 || words[0] != OutcomeJobDone {
		t.Fatalf("words %v", words)
	}
	if h.link.alert("a1").closes != 1 || h.link.alert("b0").closes != 1 || h.link.alert("a0").closes != 0 {
		t.Fatalf("a1 %d b0 %d a0 %d", h.link.alert("a1").closes, h.link.alert("b0").closes, h.link.alert("a0").closes)
	}
	targets := make([]Target, 3000)
	for i := range targets {
		targets[i] = Target{"bk_target_ip": "192.0.2." + strings.Repeat("9", 1+i%3), "bk_target_cloud_id": strings.Repeat("1", 1+i%50)}
	}
	filters, err := TargetFilters(testScope, targets)
	if err != nil || len(filters) < 2 {
		t.Fatalf("%d filters, %v", len(filters), err)
	}
	for _, filter := range filters {
		if len(filter.body) > maxTargetBodyBytes+1024 || strings.Count(string(filter.body), `"term"`) > maxTargetClauses+3 {
			t.Fatalf("a filter of %d bytes and %d terms", len(filter.body), strings.Count(string(filter.body), `"term"`))
		}
	}
}

// A link that cannot search answers 501: filtered close is unsupported,
// counted and warned, every job until the recheck answers the same word
// without a request, and nothing is closed another way. After the recheck
// it is asked again.
func TestALinkThatCannotSearchIsFilteredCloseUnsupported(t *testing.T) {
	h := newHarness(t)
	h.link.unsupported = true
	closer := h.closer(t)
	warned := 0
	closer.warn = func(error) { warned++ }
	at := time.Unix(1_800_000_000, 0)
	closer.now = func() time.Time { return at }
	if word := closer.Absent(context.Background(), testScope, DecisionAbsent, 3, Expectation{Count: 1, Source: ExpectDecidedIDs}); word != OutcomeUnsupported {
		t.Fatalf("word %s", word)
	}
	sent := h.link.requests[linkdmanage.RouteCloseFilter] + h.link.requests[linkdmanage.RouteSearch]
	if !closer.Unsupported() || warned != 1 {
		t.Fatalf("unsupported %v, warned %d", closer.Unsupported(), warned)
	}
	if word := closer.Inactive(context.Background(), testScope, "3", 3, Expectation{Count: 1, Source: ExpectFixedCeiling}); word != OutcomeUnsupported {
		t.Fatalf("word %s", word)
	}
	if word, _ := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan(), Expect: Expectation{Count: 1, Source: ExpectFixedCeiling}}); word != OutcomeUnsupported {
		t.Fatalf("edit word %s", word)
	}
	if got := h.link.requests[linkdmanage.RouteCloseFilter] + h.link.requests[linkdmanage.RouteSearch]; got != sent {
		t.Fatalf("%d requests inside the recheck", got-sent)
	}
	if n := closer.Stats()[OutcomeUnsupported]; n != 3 {
		t.Fatalf("%s = %d", OutcomeUnsupported, n)
	}
	at = at.Add(unsupportedRecheck)
	if closer.Unsupported() {
		t.Fatal("not asked again after the recheck")
	}
	if err := closer.Probe(context.Background(), "tenant-a", "built_in_bk"); err == nil || warned != 2 {
		t.Fatalf("probe %v, warned %d", err, warned)
	}
}

// One decision has one command: two processes resolving it - a restart, a
// new leader - get the same bytes, and the link sees one operation id with
// one content. Past the command's life the same decision mints a new one.
func TestADecisionsCommandIsTheSameBytesAcrossProcesses(t *testing.T) {
	h := newHarness(t)
	first, second := h.closer(t), h.closer(t)
	at := time.Unix(1_800_000_000, 0)
	key := decisionKey(testScope, "4", strings.Repeat("a", 32), "metric_changed", 5)
	a, err := first.commands.Resolve(context.Background(), key, "tenant-a", "reason", at)
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.commands.Resolve(context.Background(), key, "tenant-a", "another reason", at.Add(time.Minute))
	if err != nil || a.ID != b.ID || string(a.Bytes) != string(b.Bytes) {
		t.Fatalf("%s %s / %s %s (%v)", a.ID, a.Bytes, b.ID, b.Bytes, err)
	}
	var fields commandFields
	if json.Unmarshal(a.Bytes, &fields) != nil || fields.OperationID != a.ID || fields.OperationSource != "strategy_change" ||
		fields.OperatorKind != "system" || fields.EffectiveAt != "2027-01-15T08:00:00Z" {
		t.Fatalf("command %s", a.Bytes)
	}
	h.redis.Client().Del(context.Background(), first.commands.key(key))
	c, err := first.commands.Resolve(context.Background(), key, "tenant-a", "reason", at.Add(commandLife))
	if err != nil || c.ID == a.ID {
		t.Fatalf("a new life kept the operation id %s (%v)", c.ID, err)
	}
	if ttl := h.redis.Client().TTL(context.Background(), first.commands.key(key)).Val(); ttl <= 0 || ttl > commandLife {
		t.Fatalf("command ttl %v", ttl)
	}
}

// The pending store keeps the first command for an alert, holds to its
// bound, and drops an item past its attempts.
func TestThePendingStoreIsBounded(t *testing.T) {
	h := newHarness(t)
	pending, err := NewPending(h.redis.Client(), h.prefix, 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	item := func(id, op string) pendingItem {
		return pendingItem{TenantID: "tenant-a", AlertID: id, OperationID: op, Command: []byte(`{"operation_id":"` + op + `"}`), FirstAt: 1}
	}
	if pending.Keep(ctx, item("a", "first")) != nil || pending.Keep(ctx, item("a", "second")) != nil || pending.Keep(ctx, item("b", "x")) != nil {
		t.Fatal("keep failed")
	}
	if err := pending.Keep(ctx, item("c", "x")); err != errPendingFull {
		t.Fatalf("past the bound: %v", err)
	}
	items, _, err := pending.Next(ctx, 10)
	if err != nil || len(items) != 2 {
		t.Fatalf("items %+v (%v)", items, err)
	}
	for _, kept := range items {
		if kept.AlertID == "a" && kept.OperationID != "first" {
			t.Fatalf("the first command was replaced: %+v", kept)
		}
	}
	if !(pendingItem{Attempts: pendingAttempts, FirstAt: time.Now().Unix()}).expired(time.Now()) ||
		!(pendingItem{FirstAt: time.Now().Add(-pendingAge).Unix()}).expired(time.Now()) ||
		(pendingItem{FirstAt: time.Now().Unix()}).expired(time.Now()) {
		t.Fatal("expiry")
	}
}

// Each item of a filtered close is counted by its status first and its
// error code second: done, ended another way, found again by the filter,
// or kept to retry by id - only the ones whose close may have been saved.
func TestEachItemIsCountedByItsKind(t *testing.T) {
	h := newHarness(t)
	closer := h.closer(t)
	filter, _ := StrategyFilter(testScope)
	items := []linkdmanage.Item{
		{AlertID: "a", Status: 200}, {AlertID: "b", Status: 200, AlreadyClosed: true},
		{AlertID: "c", Status: 409, ErrorCode: "invalid_transition"}, {AlertID: "d", Status: 409, ErrorCode: "version_conflict"},
		{AlertID: "e", Status: 409, ErrorCode: "source_unpublished"}, {AlertID: "f", Status: 409, ErrorCode: "another"},
		{AlertID: "g", Status: 429, ErrorCode: "busy"}, {AlertID: "h", Status: 504, ErrorCode: "not_attempted"},
		{AlertID: "i", Status: 502, ErrorCode: "result_uncertain"}, {AlertID: "j", Status: 503, ErrorCode: "unavailable"},
		{AlertID: "k", Status: 504, ErrorCode: "timeout"}, {AlertID: "l", Status: 400, ErrorCode: "invalid_command"},
	}
	closer.items(context.Background(), Job{Filter: filter}, Command{ID: "op", Bytes: []byte(`{}`)}, items)
	want := map[string]uint64{OutcomeItemClosed: 1, OutcomeItemAlreadyClosed: 1, OutcomeItemEndedOtherwise: 1, OutcomeItemRederived: 3,
		OutcomeItemSourceUnpublished: 1, OutcomeItemConflictUnknown: 1, OutcomeItemPending: 3, OutcomeItemRefused: 1}
	if got := nonZero(closer.Stats()); len(got) != len(want) {
		t.Fatalf("stats %v, want %v", got, want)
	}
	for word, n := range want {
		if closer.Stats()[word] != n {
			t.Fatalf("%s = %d, want %d (%v)", word, closer.Stats()[word], n, nonZero(closer.Stats()))
		}
	}
	kept, _, _ := closer.pending.Next(context.Background(), 100)
	ids := map[string]bool{}
	for _, item := range kept {
		ids[item.AlertID] = true
	}
	if len(ids) != 3 || !ids["i"] || !ids["j"] || !ids["k"] {
		t.Fatalf("kept %v, want the three 5xx other than not_attempted", ids)
	}
}

// A close decided per series closes the strategy's active alerts of those
// fingerprints and no other, under a command of its own decision.
func TestAPerSeriesCloseClosesOnlyItsFingerprints(t *testing.T) {
	h := newHarness(t)
	for i, fingerprint := range []string{"f1", "f2", "f3"} {
		h.link.add(&fakeAlert{id: alertID(i), fingerprint: fingerprint, labels: map[string]string{"strategy_id": "41"}})
	}
	h.link.add(&fakeAlert{id: "other", fingerprint: "f1", labels: map[string]string{"strategy_id": "42"}})
	closer := h.closer(t)
	if word := closer.Fingerprints(context.Background(), testScope, []string{"f1", "f2"}, "target_out_of_scope", 7); word != OutcomeJobDone {
		t.Fatalf("word %s", word)
	}
	for id, want := range map[string]int{alertID(0): 1, alertID(1): 1, alertID(2): 0, "other": 0} {
		if h.link.alert(id).closes != want {
			t.Fatalf("%s closed %d, want %d", id, h.link.alert(id).closes, want)
		}
	}
}
