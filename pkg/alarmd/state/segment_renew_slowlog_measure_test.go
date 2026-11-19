// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// A measurement, not a check: ALARMD_MEASURE_RENEW_SCRIPT=1 runs it
// (ALARMD_MEASURE_RENEW_ROUNDS rounds, 300 by default). How much longer the
// server's slow log says each renewal script ran than the script's own clock
// - its first TIME to its last, what over_budget and
// state_segment_renew_script_seconds read: the EVALSHA call, its arguments,
// the script's setup before its first line and its reply after its last.
// Every script is in the slow log at a threshold of zero; each round's are
// paired with the run times the store named for them, in the order they
// ran. The largest difference is the margin the share past 2.5 ms is read
// less.
func TestMeasureTheRenewScriptAgainstTheSlowLog(t *testing.T) {
	if os.Getenv("ALARMD_MEASURE_RENEW_SCRIPT") != "1" {
		t.Skip("a measurement: set ALARMD_MEASURE_RENEW_SCRIPT=1")
	}
	rounds := 300
	if raw := os.Getenv("ALARMD_MEASURE_RENEW_ROUNDS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("ALARMD_MEASURE_RENEW_ROUNDS=%q", raw)
		}
		rounds = parsed
	}
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	ctx := context.Background()
	client := rig.clients[0]
	for _, setting := range [][2]string{{"slowlog-log-slower-than", "0"}, {"slowlog-max-len", "10000"}} {
		if err := client.ConfigSet(ctx, setting[0], setting[1]).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for _, line := range strings.Split(client.Info(ctx, "server").Val(), "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			t.Logf("%s, %d rounds", strings.TrimSpace(line), rounds)
		}
	}
	plan := segmentPlanOf("9")
	members, _ := memberKeys(t, rig, 0, plan, 0, 768)
	sizes := []int{1, 8, 32, 64, 128, 192, 256, 384, 512, 768}
	type sample struct {
		keys         int
		script, slow time.Duration
	}
	var samples []sample
	for round := 0; round < rounds; round++ {
		size := sizes[round%len(sizes)]
		if err := client.Do(ctx, "SLOWLOG", "RESET").Err(); err != nil {
			t.Fatal(err)
		}
		renewed := renewSegments(t, store, testApplyFence(), segment.PlanRenewal{Key: plan, Lifetime: segmentLifetime(),
			Pages: []segment.PageRenewal{{Page: 0, Members: members[:size]}}})
		logs, err := client.SlowLogGet(ctx, 10000).Result()
		if err != nil {
			t.Fatal(err)
		}
		var scripts []redis.SlowLog
		for index := len(logs) - 1; index >= 0; index-- {
			args := logs[index].Args
			if len(args) >= 2 && strings.EqualFold(args[0], "evalsha") && args[1] == segmentRenewSHA {
				scripts = append(scripts, logs[index])
			}
		}
		if len(scripts) != len(renewed[0].Elapsed) {
			t.Fatalf("round %d: %d renewal scripts in the slow log, %d run times named", round, len(scripts), len(renewed[0].Elapsed))
		}
		for position, entry := range scripts {
			keys := min(segment.RenewKeys, size-position*segment.RenewKeys)
			samples = append(samples, sample{keys: keys, script: renewed[0].Elapsed[position], slow: entry.Duration})
		}
	}
	report := func(label string, picked []sample) {
		if len(picked) == 0 {
			return
		}
		deltas := make([]time.Duration, 0, len(picked))
		var longest time.Duration
		for _, s := range picked {
			deltas = append(deltas, s.slow-s.script)
			longest = max(longest, s.script)
		}
		sort.Slice(deltas, func(i, j int) bool { return deltas[i] < deltas[j] })
		rank := func(q float64) time.Duration {
			index := int(q*float64(len(deltas)) + 0.999999999)
			return deltas[max(0, min(len(deltas)-1, index-1))]
		}
		t.Logf("%-12s n=%-5d delta p50=%-8v p99=%-8v p99.99=%-8v max=%-8v min=%-8v (longest script by its own clock %v)",
			label, len(deltas), rank(0.5), rank(0.99), rank(0.9999), deltas[len(deltas)-1], deltas[0], longest)
	}
	report("all", samples)
	for _, keys := range []int{1, 8, 32, 64, 128, 192, segment.RenewKeys} {
		var picked []sample
		for _, s := range samples {
			if s.keys == keys {
				picked = append(picked, s)
			}
		}
		report("keys="+strconv.Itoa(keys), picked)
	}
}
