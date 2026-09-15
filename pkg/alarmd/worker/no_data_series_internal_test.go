// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/nodata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

func noDataWiredPlan(t *testing.T) execution.DuePlan {
	t.Helper()
	return execution.DuePlan{
		Identity: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"},
		CompiledPlan: noDataPreflightPlan(t, "7", &contract.NoDataConfigV1{
			Continuous: 1, Level: 2, AggDimension: []string{"bk_target_ip", "bk_target_cloud_id"},
		}),
		StateGeneration: "state-v1", StateApplyEpoch: 1, ScheduleRevision: "plan-schedule-v1",
	}
}

func noDataWiredStream(t *testing.T, due execution.DuePlan, store execution.PlanNoDataStore) *streamedExecution {
	t.Helper()
	duePlans := []execution.DuePlan{due}
	return &streamedExecution{
		coordinator: &SlotExecutionCoordinator{
			ports:  Ports{NoData: store, Hosts: SharedHostBusiness},
			budget: ProvisionalBudget{MaxSeries: 100, MaxRetainedBytes: 1 << 20, MaxGapMutations: 10},
		},
		header: execution.InternalExecutionHeader{
			Contract: noDataPreflightContract(t, duePlans), DuePlans: duePlans,
		},
	}
}

// A round with no target and nothing reported judges the item as a whole, and
// that verdict becomes a series the ordinary evaluation can read: one record,
// one primary binding, and the kind that says which level it is judged against.
func TestANoDataRoundProducesASeriesTheEvaluationCanRead(t *testing.T) {
	due := noDataWiredPlan(t)
	stream := noDataWiredStream(t, due, &emptyNoDataStore{})
	if err := stream.loadNoDataMemory(context.Background()); err != nil {
		t.Fatal(err)
	}

	round, err := stream.noDataRoundFor(due, nil, execution.CompletenessFull)
	if err != nil {
		t.Fatal(err)
	}
	if round.outcome != nodata.OutcomeEvaluated {
		t.Fatalf("outcome = %q, want the round to have judged", round.outcome)
	}
	if len(round.series) != 1 {
		t.Fatalf("series = %d, want the whole item judged once", len(round.series))
	}

	entry := round.series[0]
	if entry.kind() != execution.SeriesKindNoData {
		t.Fatalf("kind = %q, want %q; without it the series is judged against the declared levels",
			entry.kind(), execution.SeriesKindNoData)
	}
	// The Plan it carries is the no-data view, so everything downstream that
	// asks for levels gets the one this series is judged against.
	if levels := entry.due.CompiledPlan.Levels(); len(levels) != 1 ||
		levels[0].Definition().LevelID != due.CompiledPlan.NoDataLevel().Definition().LevelID {
		t.Fatalf("the series carries levels %+v, want only the no-data level", levels)
	}
	if entry.item.Identity.SeriesIdentityDigest != entry.series {
		t.Fatalf("state key series %q does not match the series %q",
			entry.item.Identity.SeriesIdentityDigest, entry.series)
	}

	binding := entry.inputs[0].Inputs[0]
	if binding.Role != execution.InputRolePrimary || binding.View == nil {
		t.Fatalf("binding = %+v, want one primary binding with a view", binding)
	}
	record, ok := binding.View.Record(0)
	if !ok {
		t.Fatal("the synthetic binding carries no record")
	}
	// The value is the answer: one for absent. The tag is a dimension, which is
	// what keeps this series' identity away from the item's real ones.
	if got := string(record.Values()[strategy.NoDataValueField]); got != "1" {
		t.Fatalf("synthetic value = %s, want 1 for an absent group", got)
	}
	if _, tagged := record.Dimensions()[contract.NoDataDimensionTag]; !tagged {
		t.Fatalf("synthetic dimensions = %v, want the no-data tag", record.Dimensions())
	}
	// Exactly one period behind, not merely behind: the point is the period this
	// Slot decided, and the backend's anomaly_id is built from that timestamp -
	// off by a second and no Python-written record matches it.
	period := int64(due.CompiledPlan.EvaluationSemantics().EvaluationInterval)
	if want := int64(stream.header.Contract.Slot.EvaluationTime) - period; record.SourceTime() != want {
		t.Fatalf("source time = %d, want %d: the Slot's time less one period",
			record.SourceTime(), want)
	}
}

// The memory a round produces is stored, and stored once.
func TestANoDataRoundStoresWhatItRemembered(t *testing.T) {
	due := noDataWiredPlan(t)
	store := &emptyNoDataStore{}
	stream := noDataWiredStream(t, due, store)
	if err := stream.loadNoDataMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	round, err := stream.noDataRoundFor(due, nil, execution.CompletenessFull)
	if err != nil {
		t.Fatal(err)
	}
	if round.mutation == nil {
		t.Fatal("a round that judged the item absent stored nothing")
	}
	if err := stream.coordinator.applyNoDataMemory(context.Background(),
		execution.SlotExecutionRequest{Contract: stream.header.Contract},
		[]execution.PlanNoDataMutation{*round.mutation}); err != nil {
		t.Fatal(err)
	}
	if len(store.applied) != 1 {
		t.Fatalf("stored %d memories, want one", len(store.applied))
	}
	if store.applied[0].Identity.Plan != due.Identity {
		t.Fatalf("stored memory for %+v, want %+v", store.applied[0].Identity.Plan, due.Identity)
	}
}

// A Plan's own completeness decides whether its absence is evidence. Another
// Plan's partial query in the same Slot says nothing about this one, and
// reading the Slot as a whole would silence every no-data Plan whenever any
// query anywhere came back short.
func TestNoDataCompletenessIsPerPlan(t *testing.T) {
	due := noDataWiredPlan(t)
	other := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "8"}
	stream := noDataWiredStream(t, due, &emptyNoDataStore{})
	stream.bindings = []execution.NamedInputBinding{
		{
			Consumer:     execution.ConsumerRef{Plan: due.Identity, LevelID: 5, HasLevel: true},
			Completeness: execution.CompletenessFull,
		},
		{
			Consumer:     execution.ConsumerRef{Plan: other, LevelID: 5, HasLevel: true},
			Completeness: execution.CompletenessPartial,
		},
	}
	if got := stream.noDataCompleteness(due); got != execution.CompletenessFull {
		t.Fatalf("completeness = %q, want %q: another Plan's short query is not this Plan's",
			got, execution.CompletenessFull)
	}

	stream.bindings[0].Completeness = execution.CompletenessPartial
	if got := stream.noDataCompleteness(due); got != execution.CompletenessPartial {
		t.Fatalf("completeness = %q, want %q when this Plan's own query came back short",
			got, execution.CompletenessPartial)
	}
}

// A synthetic series is bound to the no-data level's own EffectiveTime fact.
//
// Two things have to hold and neither is visible without asking. The fact has
// to have been prepared - the preparation walks the declared levels, and the
// no-data level is not one of them - and the binding has to ask for the level
// this series is judged against rather than the declared ones. Either one wrong
// and the evaluation refuses the series for a missing fact, every round,
// forever.
func TestASyntheticSeriesIsBoundToTheNoDataLevelsOwnEffectiveTime(t *testing.T) {
	due := noDataWiredPlan(t)
	header := execution.InternalExecutionHeader{
		Contract: noDataPreflightContract(t, []execution.DuePlan{due}),
		DuePlans: []execution.DuePlan{due},
	}
	prepared, err := prepareAlwaysEffectiveTimeFacts(context.Background(), header)
	if err != nil {
		t.Fatal(err)
	}
	noDataLevel := due.CompiledPlan.NoDataLevel().Definition().LevelID
	consumer := execution.ConsumerRef{Plan: due.Identity, LevelID: noDataLevel, HasLevel: true}
	if _, ok := prepared[consumer]; !ok {
		t.Fatalf("no fact was prepared for the no-data level; prepared %d facts for the declared ones",
			len(prepared))
	}

	items := []execution.StatePreflightItem{{
		Identity: execution.StateKeyIdentity{
			Plan: due.Identity, StateGeneration: due.StateGeneration, SeriesIdentityDigest: "synthetic",
		},
	}}
	bound, err := bindAlwaysEffectiveTimeFacts(header, items, prepared, execution.SeriesKindNoData)
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.EffectiveTimeFacts) != 1 {
		t.Fatalf("bound %d facts, want only the no-data level's", len(bound.EffectiveTimeFacts))
	}
	if got := bound.EffectiveTimeFacts[0].Consumer.LevelID; got != noDataLevel {
		t.Fatalf("bound the fact for level %d, want the no-data level %d", got, noDataLevel)
	}

	// And a real series still binds the declared levels, so the kind is what
	// separates them rather than the no-data level simply winning.
	real, err := bindAlwaysEffectiveTimeFacts(header, items, prepared, execution.SeriesKindReal)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range real.EffectiveTimeFacts {
		if fact.Consumer.LevelID == noDataLevel {
			t.Fatalf("a real series was bound to the no-data level: %+v", fact)
		}
	}
	if len(real.EffectiveTimeFacts) == 0 {
		t.Fatal("a real series was bound to no levels at all")
	}
}

// The dimensions a Slot saw come back as text, because that is what a group is.
func TestSeenSeriesDimensionsComeBackAsText(t *testing.T) {
	text := dimensionText(map[string]json.RawMessage{
		"bk_target_ip": json.RawMessage(`"10.0.0.1"`),
		"port":         json.RawMessage(`8080`),
	})
	if text["bk_target_ip"] != "10.0.0.1" {
		t.Fatalf("a string dimension came back as %q, want the text inside it", text["bk_target_ip"])
	}
	// A dimension that is not a string keeps its JSON form: rendering it any
	// other way would be inventing a spelling the backend never used.
	if text["port"] != "8080" {
		t.Fatalf("a numeric dimension came back as %q, want its JSON form", text["port"])
	}
}
