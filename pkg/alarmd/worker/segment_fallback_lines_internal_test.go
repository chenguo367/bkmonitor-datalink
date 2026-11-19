// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// fallbackRig is the renewal rig's Plan with its rounds folded by a
// coordinator, as a Slot's are, its log written to output.
type fallbackRig struct {
	*renewalRig
	output      bytes.Buffer
	coordinator *SlotExecutionCoordinator
}

func newFallbackRig(t *testing.T, ttl time.Duration) *fallbackRig {
	t.Helper()
	rig := &fallbackRig{renewalRig: newRenewalRig(t, ttl)}
	limiter, err := observability.NewScopedLogLimiter(observability.ScopedLogLimiterConfig{Window: time.Hour, MaxEvents: 1000, MaxScopes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := observability.NewScopedBoundedLogPolicy(limiter)
	if err != nil {
		t.Fatal(err)
	}
	rig.coordinator = &SlotExecutionCoordinator{ports: Ports{State: segmentStateStore{memorySegments: rig.store},
		Now: func() time.Time { return time.UnixMilli(rig.wall) }, Observer: observability.NewLoggingObserver(observability.New("alarmd", &rig.output), policy)}}
	return rig
}

// round is the renewal rig's round, folded by the coordinator.
func (rig *fallbackRig) round(step time.Duration, present []int) {
	rig.t.Helper()
	rig.wall += step.Milliseconds()
	open := map[execution.SeriesIdentityDigest]bool{}
	if rig.store.marks[rig.key] != nil {
		rig.store.book(rig.t, rig.key.Plan, rig.key.Generation).Each(func(entry segment.Entry) {
			if entry.Open() {
				open[entry.Digest] = true
			}
		})
	}
	at := rig.store.at + 60
	var written []execution.SeriesIdentityDigest
	var rounds []shadowRoundOf
	for _, index := range present {
		digest := shadowSeries(index)
		rounds = append(rounds, steady(rig.t, digest, at-60))
		if !open[digest] {
			written = append(written, digest)
		}
	}
	rig.store.writeState(rig.key, written)
	rig.store.at = at
	request := shadowRequest(execution.OperationNormal, at)
	round := newSegmentShadowRound(request, shadowSlot(rig.t, rig.store, request, rig.due))
	for _, series := range rounds {
		round.observe(rig.due, series.view, series.write, series.class, series.eventsSent)
	}
	rig.coordinator.foldSegmentShadow(context.Background(), request, round, []execution.DuePlan{rig.due}, nil)
	rig.holdR()
}

// lines is the fallback lines written so far.
func (rig *fallbackRig) lines() []map[string]any {
	rig.t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(rig.output.Bytes()))
	scanner.Buffer(make([]byte, 0, 1<<16), 1<<20)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			rig.t.Fatalf("log line %q: %v", scanner.Text(), err)
		}
		if line["stage"] == observability.StageSegmentRenewalFallback {
			lines = append(lines, line)
		}
	}
	return lines
}

// A page whose renewals keep failing is named on a line once it falls back
// - fallback_write, its Plan, generation and page, the times it was judged
// by and the round that last wrote its mark - and its next round's fallback,
// inside the page's lifetime, writes none.
func TestARenewalFallbackNamesItsPlanOnALine(t *testing.T) {
	const ttl = 10 * time.Minute
	rig := newFallbackRig(t, ttl)
	rig.round(time.Minute, series(3))
	rig.store.failPage = func(segment.PlanKey, int) bool { return true }
	for round := 0; len(rig.lines()) == 0; round++ {
		if round > 8 {
			t.Fatal("no fallback line within the page's lifetime")
		}
		rig.round(time.Minute, series(3))
	}
	line := rig.lines()[0]
	since, _ := line["segment_fallback_since_renewal_ms"].(float64)
	if line["segment_fallback_result"] != observability.SegmentRenewalFallbackWrite || line["segment_fallback_strategy_id"] != "9" ||
		line["segment_fallback_generation"] != "g" || line["segment_fallback_page"] != float64(0) || since < float64(ttl.Milliseconds()/2) ||
		line["segment_fallback_renewal_ttl_ms"] != float64(ttl.Milliseconds()) || line["segment_fallback_ttl_now_ms"] != float64(ttl.Milliseconds()) ||
		line["segment_fallback_interval_s"] != float64(60) || line["segment_fallback_last_mark_evaluation_time"] == float64(0) ||
		line["segment_fallback_suppressed"] != float64(0) || line["reason_code"] != string(observability.ReasonSegmentRenewalFallback) ||
		line["level"] != "WARN" {
		t.Fatalf("the fallback line %v, want a WARN fallback_write naming the Plan's page past half its lifetime, the round its mark was last written", line)
	}
	rig.round(time.Minute, series(3))
	if lines := rig.lines(); len(lines) != 1 {
		t.Fatalf("%d lines after the next round's fallback inside the lifetime, want the one", len(lines))
	}
}

// A Plan whose pages live two of its intervals falls back by design every
// round: its first fallback_unrenewable writes a line, the next one inside
// the page's lifetime is held back, and the first past it writes the next
// line, counting the one held back.
func TestAPlansFallbackLinesAreOneALifetimeCountingWhatWasHeldBack(t *testing.T) {
	rig := newFallbackRig(t, 3*time.Hour)
	rig.due.ScheduleSpec.EvaluationIntervalSeconds = 7200
	rig.round(2*time.Hour, series(3))
	rig.round(2*time.Hour, series(3))
	if lines := rig.lines(); len(lines) != 1 || lines[0]["segment_fallback_result"] != observability.SegmentRenewalFallbackUnrenewable ||
		lines[0]["segment_fallback_suppressed"] != float64(0) {
		t.Fatalf("after the first fallback: %v, want one fallback_unrenewable line, nothing held back", lines)
	}
	rig.round(2*time.Hour, series(3))
	if lines := rig.lines(); len(lines) != 1 {
		t.Fatalf("a fallback two hours after the line, inside the page's three: %d lines, want the one", len(lines))
	}
	rig.round(2*time.Hour, series(3))
	lines := rig.lines()
	if len(lines) != 2 || lines[1]["segment_fallback_result"] != observability.SegmentRenewalFallbackUnrenewable ||
		lines[1]["segment_fallback_suppressed"] != float64(1) {
		t.Fatalf("four hours after the first line: %v, want a second line counting the one held back", lines)
	}
}

// A Plan that stopped and came back names its fallback page on a line under
// fallback_stalled.
func TestAStalledPlansFallbackLineSaysStalled(t *testing.T) {
	rig := newFallbackRig(t, 10*time.Minute)
	rig.round(time.Minute, series(3))
	rig.round(time.Minute, series(3))
	rig.store.at += 5 * 60
	rig.round(6*time.Minute, series(3))
	if lines := rig.lines(); len(lines) != 1 || lines[0]["segment_fallback_result"] != observability.SegmentRenewalFallbackStalled {
		t.Fatalf("after six minutes without a round: %v, want one fallback_stalled line", lines)
	}
}
