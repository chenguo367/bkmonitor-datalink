// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package scheduler

import (
	"testing"
	"time"
)

func TestHourlyDurationsQuantileReadsThePooledSlowTail(t *testing.T) {
	var history HourlyDurations
	at := time.Unix(36_000*3600, 0)
	if got := history.Quantile(at, 0.99); got != 0 {
		t.Fatalf("empty history: got %v", got)
	}
	// 99 fast tails this hour, one slow one an hour earlier: pooled, the slow one is the 99th percentile's upper
	// neighbour, so p99 still reads the fast bucket and p100 the slow one
	for i := 0; i < 99; i++ {
		history.Record(at, 100*time.Millisecond)
	}
	history.Record(at.Add(-time.Hour), 12*time.Second)
	if got := history.Quantile(at, 0.99); got != holdBucketHigh(holdBucket(100*time.Millisecond)) {
		t.Fatalf("p99: got %v", got)
	}
	if got := history.Quantile(at, 1); got != holdBucketHigh(holdBucket(12*time.Second)) {
		t.Fatalf("p100: got %v, want the slow hour's bucket", got)
	}
	// ten slow tails among the hundred move p99 into the slow bucket
	for i := 0; i < 10; i++ {
		history.Record(at, 12*time.Second)
	}
	if got := history.Quantile(at, 0.99); got != holdBucketHigh(holdBucket(12*time.Second)) {
		t.Fatalf("p99 with a slow tenth: got %v", got)
	}
	// the reading rounds up to the bucket's upper edge, never below the durations it holds
	if got := history.Quantile(at, 1); got < 12*time.Second {
		t.Fatalf("upper edge %v is below the slowest tail", got)
	}
	// hours older than a day are not read
	if got := history.Quantile(at.Add(25*time.Hour), 0.99); got != 0 {
		t.Fatalf("a day later: got %v", got)
	}
	if got := history.Quantile(at, 0); got != 0 {
		t.Fatalf("q=0: got %v", got)
	}
}

func TestFlightCoordinatorPostQueryTail(t *testing.T) {
	flights := NewFlightCoordinator()
	if got := flights.PostQueryTail(); got != 0 {
		t.Fatalf("before any tail: got %v", got)
	}
	flights.RecordPostQueryTail(time.Now(), 0)
	if got := flights.PostQueryTail(); got != 0 {
		t.Fatalf("a zero tail is not recorded: got %v", got)
	}
	flights.RecordPostQueryTail(time.Now(), 3*time.Second)
	if got := flights.PostQueryTail(); got < 3*time.Second {
		t.Fatalf("got %v, want at least the recorded tail", got)
	}
	var none *FlightCoordinator
	none.RecordPostQueryTail(time.Now(), time.Second)
	if none.PostQueryTail() != 0 {
		t.Fatal("nil coordinator")
	}
}
