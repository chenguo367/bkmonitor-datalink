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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Every MGET the State store sends is counted by its purpose (014 next9 1):
// no file of the package but the counted one sends one, the Redis backend's
// own aside.
func TestEveryMGETTheStoreSendsGoesThroughItsCount(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || file == "state_reads.go" || file == "redis_backend.go" {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(source), "\n") {
			if strings.Contains(line, ".MGet(") {
				t.Errorf("%s:%d sends an MGET by itself, uncounted: %s", file, number+1, strings.TrimSpace(line))
			}
		}
	}
}

// A preflight's MGETs are counted under runtime, a call a batch and its keys
// each: a round the cache serves whole sends none; a purpose the store does
// not name is counted as other, which should never be.
func TestAPreflightsReadsAreCountedAsRuntimeAndAnUnnamedPurposeAsOther(t *testing.T) {
	const series = 6
	rig := newCacheRig(t, 1<<20)
	before := rig.store.StateReadStats()
	rig.round(first, series, 0)
	after := rig.store.StateReadStats()
	if calls, keys := after.Calls[StateReadRuntime]-before.Calls[StateReadRuntime],
		after.Keys[StateReadRuntime]-before.Keys[StateReadRuntime]; calls != 1 || keys != series {
		t.Fatalf("a round of %d series nothing served counted %d runtime reads of %d keys, want 1 of %d", series, calls, keys, series)
	}
	rig.now = rig.now.Add(time.Minute)
	rig.round(second, series, 1)
	if served := rig.store.StateReadStats(); served.Calls[StateReadRuntime] != after.Calls[StateReadRuntime] {
		t.Fatalf("the round the cache served counted %d runtime reads more, want none", served.Calls[StateReadRuntime]-after.Calls[StateReadRuntime])
	}
	rig.store.reads.count("not-a-purpose", 3)
	if stats := rig.store.StateReadStats(); stats.Calls[StateReadOther] != 1 || stats.Keys[StateReadOther] != 3 {
		t.Fatalf("a read of an unnamed purpose counted %d other reads of %d keys, want 1 of 3", stats.Calls[StateReadOther],
			stats.Keys[StateReadOther])
	}
}
