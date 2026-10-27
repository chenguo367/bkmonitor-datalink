// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scopeclose

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/filterclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openepisodes"
)

// closeRequest is one request the fake link received on its filtered close
// route, as the link's API documents it.
type closeRequest struct {
	Filter json.RawMessage `json:"filter"`
	Limit  int             `json:"limit"`
	DryRun bool            `json:"dry_run"`
}

// scopeLink is the link's filtered close route. Each request matches
// `active` alerts (as many as the request's limit allows); a real close
// closes them. status, when set, answers every request with that status
// instead.
type scopeLink struct {
	mu     sync.Mutex
	active int
	status int
	// itemStatus, when set, answers each item of a real close with it:
	// nothing is closed.
	itemStatus int
	requests   []closeRequest
}

func (link *scopeLink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	link.mu.Lock()
	defer link.mu.Unlock()
	if r.URL.Path != linkdmanage.RouteCloseFilter {
		http.NotFound(w, r)
		return
	}
	var request closeRequest
	_ = json.Unmarshal(body, &request)
	link.requests = append(link.requests, request)
	if link.status != 0 {
		http.Error(w, "refused", link.status)
		return
	}
	n := min(link.active, request.Limit)
	items := make([]map[string]any, 0, n)
	status := http.StatusOK
	if !request.DryRun && link.itemStatus != 0 {
		status = link.itemStatus
	}
	for i := 0; i < n; i++ {
		items = append(items, map[string]any{"alert_id": fmt.Sprintf("alert-%d", i), "status": status})
	}
	if !request.DryRun && status == http.StatusOK {
		link.active -= n
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "next": "", "dry_run": request.DryRun, "closed": n})
}

func (link *scopeLink) set(active, status int) {
	link.mu.Lock()
	link.active, link.status = active, status
	link.mu.Unlock()
}

func (link *scopeLink) sent() (dry, real []closeRequest) {
	link.mu.Lock()
	defer link.mu.Unlock()
	for _, request := range link.requests {
		if request.DryRun {
			dry = append(dry, request)
		} else {
			real = append(real, request)
		}
	}
	return dry, real
}

// knownSet is the copy's word per fingerprint; a fingerprint not listed is
// judged not held.
type knownSet map[string]bool

func (set knownSet) KnowsOpen(string, string) bool { return true }

func (set knownSet) Known(_, _, fingerprint string) (bool, bool) {
	return set[fingerprint], true
}

// unjudged is a copy with no word on anything.
type unjudged struct{}

func (unjudged) KnowsOpen(string, string) bool             { return true }
func (unjudged) Known(string, string, string) (bool, bool) { return false, false }

const (
	testTenant   = "system"
	testSource   = "source-a"
	testStrategy = "42"
)

// newScopeHarness is the production closer (filterclose over the link's
// management client and a real Redis) behind a scope close bound to set.
func newScopeHarness(t *testing.T, set OpenSet) (*Closer, *scopeLink, *time.Time) {
	t.Helper()
	server := redistest.Start(t)
	if server == nil {
		t.Skip("no redis-server")
	}
	link := &scopeLink{}
	plane := httptest.NewServer(link)
	t.Cleanup(plane.Close)
	client, err := linkdmanage.New(linkdmanage.Options{BaseURL: plane.URL, Secret: "example-secret", Username: "alarmd", Client: plane.Client()})
	if err != nil {
		t.Fatal(err)
	}
	commands, err := filterclose.NewCommands(server.Client(), "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	pending, err := filterclose.NewPending(server.Client(), "alarmd-test", filterclose.MaxPending)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	record, err := openepisodes.NewStore(server.Client(), "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	link2, err := filterclose.New(filterclose.Options{Link: client, Commands: commands, Pending: pending, Record: record, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	closer := New(Options{Now: clock})
	closer.Bind(set, link2, testSource)
	return closer, link, &now
}

func hostTarget(ip string) map[string]string {
	return map[string]string{"bk_target_ip": ip, "bk_target_cloud_id": "0"}
}

// confirm observes the drop in two different Slots: a confirmed entry.
func confirm(closer *Closer, drop Drop) {
	drop.TenantID, drop.StrategyID, drop.StrategyRevision = testTenant, testStrategy, 3
	drop.Round = 600
	closer.Observe(drop)
	drop.Round = 660
	closer.Observe(drop)
}

// termsOf lists every term and terms clause of a filter as "field=value"
// texts, and every bool.filter group of terms as one joined text.
func termsOf(t *testing.T, raw json.RawMessage) (fields map[string]bool, groups []string) {
	t.Helper()
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("filter %s: %v", raw, err)
	}
	fields = map[string]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			for key, child := range value {
				switch key {
				case "term":
					for field, v := range child.(map[string]any) {
						fields[fmt.Sprintf("%s=%v", field, v)] = true
					}
				case "terms":
					for field, v := range child.(map[string]any) {
						for _, one := range v.([]any) {
							fields[fmt.Sprintf("%s=%v", field, one)] = true
						}
					}
				case "filter":
					if list, ok := child.([]any); ok {
						var group []string
						for _, clause := range list {
							if term, ok := clause.(map[string]any)["term"].(map[string]any); ok {
								for field, v := range term {
									group = append(group, fmt.Sprintf("%s=%v", field, v))
								}
							}
						}
						groups = append(groups, strings.Join(group, ","))
					}
				}
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(root)
	return fields, groups
}

// A confirmed target rejection is closed by the target: the filter names
// the strategy's tenant, this source and the strategy, and the target's
// keys together in one clause (an IP with its cloud area), and no
// fingerprint. A rejection without target keys is closed by its
// fingerprint instead.
func TestARemovedTargetIsClosedByItsPairsAndASeriesWithoutOneByFingerprint(t *testing.T) {
	closer, link, _ := newScopeHarness(t, knownSet{"fp-target": true, "fp-bare": true})
	link.set(1, 0)
	confirm(closer, Drop{Fingerprint: "fp-target", Target: hostTarget("192.0.2.10")})
	closer.Step(context.Background())

	dry, real := link.sent()
	if len(dry) != 1 || len(real) != 1 {
		t.Fatalf("dry runs %d, closes %d; want one each", len(dry), len(real))
	}
	fields, groups := termsOf(t, real[0].Filter)
	for _, want := range []string{"bk_tenant_id=" + testTenant, "event_source_id=" + testSource, "labels.strategy_id=" + testStrategy,
		"dimensions.bk_target_ip=192.0.2.10", "dimensions.bk_target_cloud_id=0"} {
		if !fields[want] {
			t.Fatalf("close filter %s lacks %s", real[0].Filter, want)
		}
	}
	paired := false
	for _, group := range groups {
		if strings.Contains(group, "dimensions.bk_target_ip=192.0.2.10") && strings.Contains(group, "dimensions.bk_target_cloud_id=0") {
			paired = true
		}
	}
	if !paired {
		t.Fatalf("the IP and its cloud area are not one clause: %s", real[0].Filter)
	}
	for field := range fields {
		if strings.HasPrefix(field, "fingerprint=") {
			t.Fatalf("a target close names a fingerprint: %s", real[0].Filter)
		}
	}
	if stats := closer.Stats(); stats[OutcomeCloseSent] != 1 || stats[OutcomeTargetCloseSent] != 1 {
		t.Fatalf("stats %v", stats)
	}

	link.set(1, 0)
	confirm(closer, Drop{Fingerprint: "fp-bare"})
	closer.Step(context.Background())
	_, real = link.sent()
	if len(real) != 2 {
		t.Fatalf("closes %d, want 2", len(real))
	}
	fields, _ = termsOf(t, real[1].Filter)
	if !fields["fingerprint=fp-bare"] || !fields["labels.strategy_id="+testStrategy] {
		t.Fatalf("fingerprint close filter %s", real[1].Filter)
	}
	for field := range fields {
		if strings.HasPrefix(field, "dimensions.") {
			t.Fatalf("a fingerprint close names a dimension: %s", real[1].Filter)
		}
	}
	if stats := closer.Stats(); stats[OutcomeCloseSent] != 2 || stats[OutcomeTargetCloseSent] != 1 {
		t.Fatalf("stats %v", stats)
	}
}

// The dry run is checked against what the observations account for: one
// target observed with two series may match a handful of alerts, never
// hundreds. Past it nothing is closed and the entry is kept, counted; a
// count inside it is closed.
func TestADryRunPastTheTargetsExpectationClosesNothing(t *testing.T) {
	closer, link, _ := newScopeHarness(t, knownSet{"fp-1": true, "fp-2": true})
	link.set(400, 0)
	target := hostTarget("192.0.2.20")
	confirm(closer, Drop{Fingerprint: "fp-1", Target: target})
	confirm(closer, Drop{Fingerprint: "fp-2", Target: target})
	closer.Step(context.Background())
	dry, real := link.sent()
	if len(dry) != 1 || len(real) != 0 {
		t.Fatalf("over the expectation: dry runs %d, closes %d", len(dry), len(real))
	}
	if stats := closer.Stats(); stats[OutcomeSendFailed] != 1 || stats[OutcomeCloseSent] != 0 {
		t.Fatalf("stats %v", stats)
	}

	link.set(2, 0)
	closer.Step(context.Background())
	dry, real = link.sent()
	if len(dry) != 2 || len(real) != 1 {
		t.Fatalf("inside the expectation: dry runs %d, closes %d", len(dry), len(real))
	}
	if stats := closer.Stats(); stats[OutcomeCloseSent] != 1 || stats[OutcomeTargetCloseSent] != 1 {
		t.Fatalf("stats %v", stats)
	}
}

// A link that is busy or fails holds the entry: it is counted send_failed
// and closed on a later step once the link answers. 501 is counted as
// filtered close unsupported, not closed.
func TestAFailedCloseIsHeldAndRetried(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			closer, link, _ := newScopeHarness(t, knownSet{"fp-1": true})
			link.set(1, status)
			confirm(closer, Drop{Fingerprint: "fp-1", Target: hostTarget("192.0.2.30")})
			closer.Step(context.Background())
			if _, real := link.sent(); len(real) != 0 {
				t.Fatalf("closes %d while the link answers %d", len(real), status)
			}
			if stats := closer.Stats(); stats[OutcomeSendFailed] != 1 || stats[OutcomeCloseSent] != 0 {
				t.Fatalf("stats %v", stats)
			}
			link.set(1, 0)
			closer.Step(context.Background())
			if _, real := link.sent(); len(real) != 1 {
				t.Fatalf("the held entry was not closed after the link recovered: closes %d", len(real))
			}
			if stats := closer.Stats(); stats[OutcomeCloseSent] != 1 {
				t.Fatalf("stats %v", stats)
			}
		})
	}
	// Items the link certainly did not close (429 each): the batch is not
	// settled, the entry is kept and closed on the next step.
	closer, link, _ := newScopeHarness(t, knownSet{"fp-1": true})
	link.set(1, 0)
	link.itemStatus = http.StatusTooManyRequests
	confirm(closer, Drop{Fingerprint: "fp-1", Target: hostTarget("192.0.2.32")})
	closer.Step(context.Background())
	if stats := closer.Stats(); stats[OutcomeSendFailed] != 1 || stats[OutcomeCloseSent] != 0 {
		t.Fatalf("stats %v, want the rederived batch kept", stats)
	}
	link.mu.Lock()
	link.itemStatus = 0
	link.mu.Unlock()
	closer.Step(context.Background())
	if stats := closer.Stats(); stats[OutcomeCloseSent] != 1 {
		t.Fatalf("stats %v, want the kept entry closed", stats)
	}

	closer, link, _ = newScopeHarness(t, knownSet{"fp-1": true})
	link.set(1, http.StatusNotImplemented)
	confirm(closer, Drop{Fingerprint: "fp-1", Target: hostTarget("192.0.2.31")})
	closer.Step(context.Background())
	if stats := closer.Stats(); stats[OutcomeUnsupported] != 1 || stats[OutcomeCloseSent] != 0 {
		t.Fatalf("stats %v", stats)
	}
}

// Nothing is closed on the copy's silence or on a single Slot: a
// fingerprint the copy has no word on, or judged not open, is counted and
// forgotten; one observation waits for a second.
func TestNoCloseWithoutTheCopysWordAndASecondSlot(t *testing.T) {
	closer, link, _ := newScopeHarness(t, unjudged{})
	link.set(1, 0)
	confirm(closer, Drop{Fingerprint: "fp-1", Target: hostTarget("192.0.2.40")})
	closer.Step(context.Background())
	if stats := closer.Stats(); stats[OutcomeSetUnavailable] != 2 || len(link.requests) != 0 {
		t.Fatalf("unjudged: stats %v, requests %d", stats, len(link.requests))
	}

	closer, link, _ = newScopeHarness(t, knownSet{"fp-1": false, "fp-2": true})
	link.set(1, 0)
	confirm(closer, Drop{Fingerprint: "fp-1", Target: hostTarget("192.0.2.41")})
	closer.Observe(Drop{TenantID: testTenant, StrategyID: testStrategy, StrategyRevision: 3, Round: 600, Fingerprint: "fp-2",
		Target: hostTarget("192.0.2.42")})
	closer.Step(context.Background())
	if stats := closer.Stats(); stats[OutcomeNotMember] != 2 || stats[OutcomeUnconfirmed] != 1 || len(link.requests) != 0 {
		t.Fatalf("not held / one Slot: stats %v, requests %d", stats, len(link.requests))
	}
}
