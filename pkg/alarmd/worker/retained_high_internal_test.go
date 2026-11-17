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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The high-water is the most held at once within the hour, taken at every
// reservation - a reservation made and given back between two minutes is in
// it - no less than what is held now, and the window it covers grows from
// the coordinator's start to an hour.
func TestTheRetainedHighWaterIsTheMostHeldAtOnceWithinTheHour(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	coordinator := &SlotExecutionCoordinator{ports: Ports{Now: func() time.Time { return now }},
		budget: ProvisionalBudget{MaxSeries: 1 << 20, MaxRetainedBytes: 1 << 30}}
	coordinator.reservations.highSince = now
	if high, window := RetainedHigh(coordinator); high != 0 || window != 0 {
		t.Fatalf("at the start: %d over %v", high, window)
	}
	// A short peak: 300 MiB reserved and given back within the minute.
	if err := coordinator.acquireProvisional(0, 300<<20, nil, "normal_input"); err != nil {
		t.Fatal(err)
	}
	coordinator.releaseProvisional(0, 300<<20)
	if !coordinator.tryAcquireRetained(10 << 20) {
		t.Fatal("a read ahead's bytes were refused")
	}
	now = now.Add(30 * time.Minute)
	if high, window := RetainedHigh(coordinator); high != 300<<20 || window != 30*time.Minute {
		t.Fatalf("half an hour on: %d MiB over %v, want the 300 MiB peak over 30 min", high>>20, window)
	}
	now = now.Add(31 * time.Minute)
	if high, window := RetainedHigh(coordinator); high != 10<<20 || window != time.Hour {
		t.Fatalf("past the hour: %d MiB over %v, want what is held now, 10 MiB, over an hour", high>>20, window)
	}
	if share := RetainedShare(coordinator); share != ((1<<30)-(1<<30)/otherGroupsPoolDivisor)/largeQueryGroupsPerWorker {
		t.Fatalf("share %d, want the coordinator's own", share)
	}
}

// Every place the retained reservation grows folds it into the high-water:
// a path that adds bytes without it would hold more than the Leader reads.
func TestEveryRetainedReservationFeedsTheHighWater(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(source), "\n")
		for index, line := range lines {
			if !strings.Contains(line, "retainedBytes +=") || !strings.Contains(line, "reservations") {
				continue
			}
			found++
			if index+1 >= len(lines) || !strings.Contains(lines[index+1], "noteRetainedHighLocked()") {
				t.Errorf("%s:%d adds to the retained reservation without noting the high-water on the next line", file, index+1)
			}
		}
	}
	if found < 4 {
		t.Fatalf("found %d retained reservations, want the four known", found)
	}
}
