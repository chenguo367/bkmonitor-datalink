// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// One strategy's standing, from the Leader's memory of what it published:
// the accepted one answers its Plan with the Query Group and the three
// revisions and the object digest the Plan is read from; the withheld one
// answers no Plan and the disposition with its reason; a strategy the source
// never listed is not found; and a process that has published nothing has
// no answer to give. No Redis is read to answer.
func TestLookupStrategyAnswersAcceptedAndWithheldStrategiesFromTheLastPublication(t *testing.T) {
	client := newControlplaneRedis(t)
	ctx := context.Background()
	documents := realThresholdDocuments(t)
	if err := client.Set(ctx, "bkmonitor.cache.strategy_ids", `[1001,1002]`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	for id, document := range map[string]json.RawMessage{"1001": documents[0], "1002": documents[1]} {
		if err := client.Set(ctx, "bkmonitor.cache.strategy_"+id, string(document), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	source := newRedisStrategySource(t, client)
	planner, err := controlplane.NewLegacyPrimaryQueryCompiler("uq-primary-v1", "UTC", testLegacyQueryRuntimeFacts())
	if err != nil {
		t.Fatal(err)
	}
	repository, err := controlplane.NewRedisCatalogRepository(client, "alarmd:control:lookup", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	compiler, stateSemantics := runtimePlanCompiler(t)
	reconciler, err := controlplane.NewSourceReconciler(repository, compiler, stateSemantics,
		func(catalog controlplane.Catalog) (controlplane.Catalog, error) {
			groups := make([]controlplane.QueryGroup, 0, len(catalog.QueryGroups))
			for _, group := range catalog.QueryGroups {
				kept := make([]controlplane.FrozenPlan, 0, len(group.Plans))
				for _, plan := range group.Plans {
					if plan.Identity.StrategyID == "1001" {
						catalog.Dispositions = append(catalog.Dispositions, controlplane.ObjectDisposition{
							SourceID: "1001", Scope: "PLAN", Disposition: controlplane.DispositionUnsupported,
							Reason: "SNAPSHOT_RETENTION_INSUFFICIENT", FieldPath: "items[0]",
						})
						continue
					}
					kept = append(kept, plan)
				}
				if len(kept) == 0 {
					continue
				}
				group.Plans = kept
				groups = append(groups, group)
			}
			catalog.QueryGroups = groups
			return catalog, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if before := reconciler.LookupStrategy("1002"); before.Available {
		t.Fatalf("a process that has published nothing answered %+v, want not available", before)
	}
	if result, err := reconciler.Refresh(ctx, source, planner); err != nil || result.Status != controlplane.SourceRefreshPendingConfirmation {
		t.Fatalf("first refresh = (%#v, %v)", result, err)
	}
	published, err := reconciler.Refresh(ctx, source, planner)
	if err != nil || published.Status != controlplane.SourceRefreshPublished {
		t.Fatalf("publish = (%#v, %v)", published, err)
	}
	snapshot, err := loadPublishedSnapshot(ctx, repository, published.Publication)
	if err != nil {
		t.Fatal(err)
	}

	accepted := reconciler.LookupStrategy("1002")
	if !accepted.Available || !accepted.Found || accepted.Retained || accepted.Publication != published.Publication {
		t.Fatalf("accepted strategy = %+v, want available, found, not retained, from the publication", accepted)
	}
	if len(accepted.Plans) != 1 {
		t.Fatalf("accepted strategy plans = %+v, want its one Plan", accepted.Plans)
	}
	plan := accepted.Plans[0]
	var group controlplane.QueryGroup
	for _, candidate := range snapshot.QueryGroups {
		if candidate.Identity == plan.QueryGroup {
			group = candidate
		}
	}
	digest, err := controlplane.DeriveQueryGroupObjectDigest(group)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Plan.StrategyID != "1002" || plan.ObjectDigest != digest || plan.SnapshotRevision != published.Publication.SnapshotRevision ||
		plan.QueryRevision != group.QueryPlan.QueryRevision || plan.ScheduleRevision != group.ScheduleRevision {
		t.Fatalf("plan ref = %+v, want the published group's digest %s and revisions (query %s, schedule %s)", plan, digest, group.QueryPlan.QueryRevision, group.ScheduleRevision)
	}
	if len(accepted.Dispositions) != 1 || accepted.Dispositions[0].Disposition != controlplane.DispositionAccepted {
		t.Fatalf("accepted strategy dispositions = %+v, want the one ACCEPTED", accepted.Dispositions)
	}

	withheld := reconciler.LookupStrategy("1001")
	if !withheld.Available || !withheld.Found || withheld.Retained || len(withheld.Plans) != 0 {
		t.Fatalf("withheld strategy = %+v, want found with no Plan", withheld)
	}
	var named bool
	for _, disposition := range withheld.Dispositions {
		if disposition.Disposition == controlplane.DispositionUnsupported && disposition.Reason == "SNAPSHOT_RETENTION_INSUFFICIENT" && disposition.FieldPath == "items[0]" {
			named = true
		}
	}
	if !named {
		t.Fatalf("withheld strategy dispositions = %+v, want the refusal with its reason and field path", withheld.Dispositions)
	}

	if missing := reconciler.LookupStrategy("4242"); !missing.Available || missing.Found || len(missing.Plans) != 0 || len(missing.Dispositions) != 0 {
		t.Fatalf("a strategy the source never listed = %+v, want available and not found", missing)
	}
}
