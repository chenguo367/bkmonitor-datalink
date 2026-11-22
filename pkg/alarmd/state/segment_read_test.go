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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// coverByIdentity is a Plan's segments as the store is told them: a series'
// coverage by its key, generation and all.
type coverByIdentity map[execution.StateKeyIdentity]segment.Coverage

func (cover coverByIdentity) Coverage(identity execution.StateKeyIdentity) (segment.Coverage, bool) {
	coverage, member := cover[identity]
	return coverage, member
}

// segmentRig is newCacheRig's store told segments, over the backend given.
func segmentRig(t *testing.T, backend *pipelineMemoryBackend, budget int64, segments SegmentCoverage) *cacheRig {
	t.Helper()
	rig := &cacheRig{t: t, backend: backend, now: time.Unix(1_700_000_000, 0), begun: true}
	router, err := NewFixedRouter("state-01", rig.backend)
	if err != nil {
		t.Fatal(err)
	}
	rig.store, err = NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute,
		FenceKeys: fixedFenceKeys{testFenceKeys()}, StateCacheBudgetBytes: budget, StateCacheReplayAge: time.Minute,
		Now: func() time.Time { return rig.now }, Segments: segments,
		RestoreProgressCheck: func(context.Context, execution.QueryGroupIdentity, execution.EvaluationTime) (bool, error) {
			return rig.begun, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

func (rig *cacheRig) views(s slot, items []execution.StatePreflightItem) []execution.RuntimeStateView {
	rig.t.Helper()
	loaded, err := rig.store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: s.contract(), Items: items, OwnerFence: s.fence})
	if err != nil {
		rig.t.Fatalf("LoadRuntime() error = %v", err)
	}
	return loaded.Items
}

func sourceTimes(view execution.RuntimeStateView) []int64 {
	times := make([]int64, 0, len(view.History))
	for _, point := range view.History {
		times = append(times, point.SourceTime)
	}
	return times
}

// A member's record reads the same however it is read - from the cache, from
// the store, and as the generation a Plan's next one carries from - its points
// and then a normal point every interval up to the mark; a series that is no
// member reads as stored, and a store told no segments reads every record as
// stored (014 B3 3.2: the rule is in the one place a record becomes a view).
func TestAMembersRecordReadsTheSameFromTheCacheTheStoreAndACarry(t *testing.T) {
	member, other := seriesIdentity(0), seriesIdentity(1)
	cover := coverByIdentity{member: {Entry: segment.Entry{Digest: member.SeriesIdentityDigest, Start: 60},
		Position: 240, Interval: 60, Retention: 5}}
	items := []execution.StatePreflightItem{{Identity: member, ApplyVersion: second.version()}, {Identity: other, ApplyVersion: second.version()}}
	cached := segmentRig(t, newPipelineMemoryBackend(), 1<<20, cover)
	cached.round(first, 2, 0)
	cached.now = cached.now.Add(time.Minute)
	fromCache := cached.views(second, items)
	if cached.lookups(StateCacheHit) != 2 {
		t.Fatalf("%d cache hits, want both series served from the cache", cached.lookups(StateCacheHit))
	}
	if got := sourceTimes(fromCache[0]); !reflect.DeepEqual(got, []int64{60, 120, 180, 240}) || fromCache[0].LastProcessedEventTime != 240 {
		t.Fatalf("member read from the cache at %v, processed to %d; want its point and three made up to the mark",
			got, fromCache[0].LastProcessedEventTime)
	}
	if got := sourceTimes(fromCache[1]); !reflect.DeepEqual(got, []int64{60}) {
		t.Fatalf("a series that is no member read at %v, want its one stored point", got)
	}
	fromStore := segmentRig(t, cached.backend, 0, cover).views(second, items)
	for index := range items {
		if !reflect.DeepEqual(fromStore[index].History, fromCache[index].History) || !reflect.DeepEqual(fromStore[index].Levels, fromCache[index].Levels) ||
			fromStore[index].LastProcessedEventTime != fromCache[index].LastProcessedEventTime || fromStore[index].Status != fromCache[index].Status {
			t.Fatalf("series %d read from the store %+v,\nfrom the cache %+v", index, fromStore[index], fromCache[index])
		}
	}
	next := member
	next.StateGeneration = "next-generation"
	carried := segmentRig(t, cached.backend, 0, cover).views(second,
		[]execution.StatePreflightItem{{Identity: next, ApplyVersion: second.version(), CarryFrom: member.StateGeneration}})
	if carried[0].Carried == nil || !reflect.DeepEqual(carried[0].Carried.History, fromStore[0].History) {
		t.Fatalf("the carried generation read %+v, want the member's record as the store reads it", carried[0].Carried)
	}
	if plain := segmentRig(t, cached.backend, 0, nil).views(second, items); !reflect.DeepEqual(sourceTimes(plain[0]), []int64{60}) {
		t.Fatalf("a store told no segments read the member at %v, want as stored", sourceTimes(plain[0]))
	}
}

// One function turns a stored record into a view: decodeRuntimePacked is
// called from decodeRuntimeCovered alone, and that from the store's
// decodeRuntime alone, which hands it the store's segments. A reader added
// beside them would read a member as its stored points only - its segment's
// normal points a gap - and nothing else would say so.
func TestOneFunctionTurnsAStoredRecordIntoAView(t *testing.T) {
	// decodedForCompare reads a record's bytes for the State cache to compare
	// what it keeps with, and hands its view to no reader.
	allowed := map[string][]string{"decodeRuntimePacked": {"decodeRuntimeCovered", "decodedForCompare"},
		"decodeRuntimeCovered": {"decodeRuntime"}}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(function, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				callers, ruled := allowed[identifier.Name]
				if !ruled {
					return true
				}
				if !slices.Contains(callers, function.Name.Name) {
					t.Errorf("%s:%d: %s calls %s; a stored record becomes a view in %s alone",
						name, fileSet.Position(call.Pos()).Line, function.Name.Name, identifier.Name, callers[0])
				}
				found[identifier.Name] = true
				return true
			})
		}
	}
	for callee := range allowed {
		if !found[callee] {
			t.Errorf("nothing calls %s; the scan is keeping a rule about nothing", callee)
		}
	}
}
