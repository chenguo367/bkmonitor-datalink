// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func retentionTestPlan(strategyID string, intervalSeconds int64) controlplane.FrozenPlan {
	return controlplane.FrozenPlan{
		Identity:     execution.PlanIdentity{TenantID: "tenant-a", BusinessID: "2", StrategyID: strategyID},
		ScheduleSpec: execution.ScheduleSpec{EvaluationIntervalSeconds: intervalSeconds, Timezone: "UTC"},
	}
}

func retentionTestCatalog(plans ...controlplane.FrozenPlan) controlplane.Catalog {
	groups := make([]controlplane.QueryGroup, 0, len(plans))
	for _, plan := range plans {
		groups = append(groups, controlplane.QueryGroup{Plans: []controlplane.FrozenPlan{plan}})
	}
	return controlplane.Catalog{QueryGroups: groups}
}

func withheldReasons(catalog controlplane.Catalog) map[string]controlplane.ObjectDisposition {
	withheld := map[string]controlplane.ObjectDisposition{}
	for _, disposition := range catalog.Dispositions {
		withheld[disposition.SourceID] = disposition
	}
	return withheld
}

func publishedStrategies(catalog controlplane.Catalog) []string {
	var published []string
	for _, group := range catalog.QueryGroups {
		for _, plan := range group.Plans {
			published = append(published, plan.Identity.StrategyID)
		}
	}
	return published
}

// A Plan this deployment cannot serve is withheld. Every other Plan publishes.
//
// This is the whole point of the change, and the counterexample is the test:
// before it, one strategy asking for more Snapshot retention than the
// deployment keeps refused the entire Catalog -- every round, for as long as
// that strategy existed. The fleet went stale, no cutover was attempted,
// nothing was written, and the account of it was a single sentence naming a
// strategy nobody had touched. Retention is the deployment's capacity and does
// not follow a Plan; the Plan that asks for more than there is, is the Plan
// that cannot run.
func TestAPlanBeyondTheDeploymentsReachIsWithheldAndItsSiblingsPublish(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	reserve := cfg.PhaseTwo.Access.DownstreamExecutionReserve.Duration()
	supported := int64(phaseTwoMaxSupportedEvaluationInterval / time.Second)
	// A cadence the retention cannot cover, one whose completion does not
	// clear the reserve, and two ordinary ones on either side of them.
	catalog := retentionTestCatalog(
		retentionTestPlan("4710", 60),
		retentionTestPlan("4711", int64(reserve/time.Second)),
		retentionTestPlan("4713", supported+60),
		retentionTestPlan("4714", supported),
	)

	admitted, err := phaseTwoCatalogRetentionAdmission(cfg)(catalog)
	if err != nil {
		t.Fatalf("admission error = %v; a Plan the deployment cannot serve must not refuse the round", err)
	}

	published := publishedStrategies(admitted)
	if len(published) != 2 || published[0] != "4710" || published[1] != "4714" {
		t.Fatalf("published %v, want the two Plans this deployment can serve. One Plan it cannot must not "+
			"stop the others: that is a whole deployment not detecting anything because of one strategy",
			published)
	}

	withheld := withheldReasons(admitted)
	if got := withheld["4713"].Reason; got != contract.ReasonSnapshotRetentionInsufficient {
		t.Fatalf("the over-retention Plan is withheld as %q, want %q",
			got, contract.ReasonSnapshotRetentionInsufficient)
	}
	if got := withheld["4711"].Reason; got != contract.ReasonCompletionOffsetBelowReserve {
		t.Fatalf("the Plan with no time to query is withheld as %q, want %q",
			got, contract.ReasonCompletionOffsetBelowReserve)
	}
	if _, refused := withheld["4710"]; refused {
		t.Fatalf("an ordinary Plan was withheld: %+v", withheld["4710"])
	}

	// Both numbers, because the two readings are different actions: shorten
	// the strategy's cadence, or raise the deployment's retention. A refusal
	// naming one of them leaves the reader to work out which it was.
	field := withheld["4713"].FieldPath
	for _, want := range []string{"required_retention=", "catalog_retention="} {
		if !strings.Contains(field, want) {
			t.Fatalf("withheld field %q does not carry %q", field, want)
		}
	}
	if !strings.Contains(withheld["4711"].FieldPath, reserve.String()) {
		t.Fatalf("withheld field %q does not carry the reserve it failed to clear", withheld["4711"].FieldPath)
	}
}

// A Query Group whose every Plan is withheld is not published empty.
//
// An empty Query Group is one a worker is assigned, freezes Slots for, and can
// do nothing with -- work that looks like work and detects nothing.
func TestAQueryGroupWithNothingLeftIsNotPublished(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	supported := int64(phaseTwoMaxSupportedEvaluationInterval / time.Second)
	catalog := controlplane.Catalog{QueryGroups: []controlplane.QueryGroup{{
		Plans: []controlplane.FrozenPlan{retentionTestPlan("4713", supported+60)},
	}}}

	admitted, err := phaseTwoCatalogRetentionAdmission(cfg)(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.QueryGroups) != 0 {
		t.Fatalf("Query Groups = %+v, want none: every Plan in it was withheld", admitted.QueryGroups)
	}
	if len(admitted.Dispositions) != 1 {
		t.Fatalf("dispositions = %+v, want the one withheld Plan named", admitted.Dispositions)
	}
}
