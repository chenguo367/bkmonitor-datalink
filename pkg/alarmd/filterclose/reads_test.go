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
	"testing"
)

// The link's search reads its history as well as its active alerts
// (alert-search.md, "覆盖当前部署的 Active 与 History 查询目标"). A recovered
// alert of an older version sorts first (alert ids are time-prefixed) and
// is neither a bucket nor read one by one: the per-alert read of a bucket
// asks for active alerts only, so a history it does not own never uses up
// the turn's read bound.
func TestAnEditReadsABucketsActiveAlertsOnly(t *testing.T) {
	h := newHarness(t)
	recovered := opened("a-history", "3", nil, false, "critical", map[string]string{"bk_target_ip": "192.0.2.1", "removed_dim": "x"})
	recovered.status = "recovered"
	h.link.add(recovered)
	h.link.add(opened("b-active", "3", nil, false, "critical", map[string]string{"bk_target_ip": "192.0.2.2", "removed_dim": "x"}))
	closer := h.closer(t)
	if word, buckets := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan()}); word != "" || buckets != 1 {
		t.Fatalf("word %q, buckets %d", word, buckets)
	}
	stats := closer.Stats()
	if stats[OutcomeAlertsRead] != 1 || stats[OutcomeAlertDecidedClose] != 1 {
		t.Fatalf("stats %v, want one alert read and decided", nonZero(stats))
	}
	if h.link.alert("b-active").closes != 1 || h.link.alert("a-history").closes != 0 {
		t.Fatalf("closes: active %d, history %d", h.link.alert("b-active").closes, h.link.alert("a-history").closes)
	}
}

// The close of a strategy without a record reads its active alerts only.
func TestThePerAlertCloseReadsActiveAlertsOnly(t *testing.T) {
	h := newHarness(t)
	h.link.add(&fakeAlert{id: alertID(0), status: "recovered", labels: map[string]string{"strategy_id": "41", "strategy_version": "3"}})
	h.link.add(&fakeAlert{id: alertID(1), labels: map[string]string{"strategy_id": "41", "strategy_version": "3"}})
	closer := h.closer(t)
	if word := closer.Absent(context.Background(), testScope, DecisionAbsent, 3); word != OutcomeJobDone {
		t.Fatalf("word %s (stats %v)", word, nonZero(closer.Stats()))
	}
	if read := closer.Stats()[OutcomeAlertsRead]; read != 1 {
		t.Fatalf("read %d alerts, want the one active", read)
	}
}

// A bucket read one by one reads at most maxAlertsPerTurn alerts a round,
// says it stopped short, and the next round goes on from where it stopped:
// the alerts it keeps open are not read again before the one past the
// bound is reached and closed.
func TestABucketReadOneByOneIsBoundedPerTurnAndGoesOn(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < maxAlertsPerTurn; i++ {
		h.link.add(opened(alertID(i), "3", nil, false, "critical", nil))
	}
	last := alertID(maxAlertsPerTurn)
	h.link.add(opened(last, "3", nil, false, "critical", map[string]string{"bk_target_ip": "192.0.2.1", "removed_dim": "x"}))
	closer := h.closer(t)
	if word, _ := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan()}); word != "" {
		t.Fatalf("word %s", word)
	}
	stats := closer.Stats()
	if stats[OutcomeAlertsRead] != uint64(maxAlertsPerTurn) || stats[OutcomeAlertsTruncated] != 1 || stats[OutcomeAlertDecidedClose] != 0 {
		t.Fatalf("first round stats %v", nonZero(stats))
	}
	if h.link.alert(last).closes != 0 {
		t.Fatal("the alert past the bound was closed in the first round")
	}
	if word, _ := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan()}); word != "" {
		t.Fatalf("word %s", word)
	}
	if h.link.alert(last).closes != 1 || h.link.alert(alertID(0)).closes != 0 {
		t.Fatalf("closes: past the bound %d, kept %d", h.link.alert(last).closes, h.link.alert(alertID(0)).closes)
	}
	if stats := closer.Stats(); stats[OutcomeAlertsRead] != uint64(maxAlertsPerTurn+1) || stats[OutcomeAlertsTruncated] != 1 {
		t.Fatalf("second round stats %v", nonZero(stats))
	}
}

// A sample whose digest label is not the digest of the facts beside it in
// extra_data does not speak for its bucket: no decision, nothing closed,
// counted digest_mismatch.
func TestASampleWhoseFactsAreNotItsDigestDecidesNothing(t *testing.T) {
	h := newHarness(t)
	alert := opened("x1", "4", factsOf("m4"), false, "critical", nil)
	// The label of the current facts beside the older facts: the older
	// facts alone would decide metric_changed and close it.
	alert.labels["strategy_facts_digest"] = factsOf("m5").ForEvent(false).Digest()
	h.link.add(alert)
	roster(t, h)
	closer := h.closer(t)
	if word, buckets := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan()}); word != "" || buckets != 1 {
		t.Fatalf("word %q, buckets %d", word, buckets)
	}
	stats := closer.Stats()
	if stats[OutcomeDigestMismatch] != 1 || h.link.alert("x1").closes != 0 || len(h.link.closeBodies) != 0 {
		t.Fatalf("closes %d, stats %v", h.link.alert("x1").closes, nonZero(stats))
	}
	// The same alert with its own digest is decided and closed.
	alert.labels["strategy_facts_digest"] = factsOf("m4").ForEvent(false).Digest()
	h.link.add(alert)
	if word, _ := closer.Edit(context.Background(), EditInput{Scope: testScope, Plan: currentPlan()}); word != "" {
		t.Fatalf("word %q", word)
	}
	if h.link.alert("x1").closes != 1 {
		t.Fatalf("the agreeing sample's bucket was not closed (stats %v)", nonZero(closer.Stats()))
	}
}

// The closes of a job stop at the count its dry run checked: matches that
// grow between the dry run and the close are not closed under it, and the
// job says it is incomplete, so the next round checks again.
func TestClosesStopAtWhatTheDryRunChecked(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 30; i++ {
		h.link.add(&fakeAlert{id: alertID(i), labels: map[string]string{"strategy_id": "41", "strategy_version": "3"}})
	}
	h.opened(t, "41", "3", nil, fingerprints(0, 30)...)
	grown := false
	h.link.beforeClose = func(link *fakeLink) {
		if grown {
			return
		}
		grown = true
		for i := 30; i < 80; i++ {
			// Ids that sort before the checked ones: the close's page
			// meets them first.
			id := "a-" + alertID(i)
			link.alerts[id] = &fakeAlert{id: id, status: "active", tenant: "tenant-a", source: "built_in_bk",
				labels: map[string]string{"strategy_id": "41", "strategy_version": "3"}}
		}
		link.sorted = nil
	}
	closer := h.closer(t)
	if word := closer.Inactive(context.Background(), testScope, "3", 3, fakeOpenedAt); word != OutcomeJobIncomplete {
		t.Fatalf("word %s, want job_incomplete (stats %v)", word, nonZero(closer.Stats()))
	}
	closed := 0
	for _, alert := range h.link.alerts {
		closed += alert.closes
	}
	if closed != 30 {
		t.Fatalf("closed %d, want the 30 the dry run checked", closed)
	}
}
