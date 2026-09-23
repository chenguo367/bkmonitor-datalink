// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

type definitiveSet struct {
	memberSet
	definitive bool
}

func (set definitiveSet) Definitive() bool { return set.definitive }

func scopeDropAdapter(t *testing.T, chain *admission.Chain, context admission.PlanContext, drops *[]ScopeDrop) (*seriesAdapter, execution.PlanIdentity) {
	t.Helper()
	_, frozen := frozenExecution(t)
	requirement := frozen.Requirements[0]
	plan := requirement.Consumers[0].Consumer.Plan
	context.StrategyID = plan.StrategyID
	return &seriesAdapter{
		consumer: &admissionConsumer{}, query: plannedQueryForTest(requirement), attemptNo: 1,
		admission: chain, scopes: planScopes{plan: context},
		scopeDrop: func(drop ScopeDrop) { *drops = append(*drops, drop) },
		outputs: planOutputs{plan: {strategyID: "42", revision: 3,
			identity: &contract.MonitorOutputIdentity{DimensionFields: []string{"bk_host_id", "device"}}}},
		round: 1700000060,
	}, plan
}

func deliverSeries(t *testing.T, adapter *seriesAdapter, dimensions map[string]json.RawMessage) {
	t.Helper()
	dataset := execution.NewDataset([]contract.CanonicalRecordV2{{RecordID: "record", SourceTime: 1, BusinessID: "2", Dimensions: dimensions}})
	if err := adapter.ConsumeProviderSeries(context.Background(), execution.ProviderSeriesBatch{
		PhysicalQuery: adapter.query.Spec.Digest, CompletionRef: "result", Dataset: dataset,
		Delivery: execution.SeriesDelivery{PhysicalQuery: adapter.query.Spec.Digest,
			QueryRevision: adapter.query.Spec.PlanFacts.QueryRevision, Series: 1, Records: 1, Digest: "digest"},
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
}

// A definitive target-plan rejection reaches the close with the fingerprint
// the evaluator would have given the record: the monitor dedupe identity of
// the frozen strategy, the Plan's business and the record's own dimensions
// under the Plan's output identity.
func TestADefinitiveRejectionCarriesTheEvaluatorsFingerprint(t *testing.T) {
	var drops []ScopeDrop
	chain := admission.NewChain([]admission.Fuller{admission.IdentityFuller{}}, []admission.Filter{admission.TargetScopeFilter{}, admission.TargetPlanFilter{}})
	target := &admission.TargetPlanContext{Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true},
		Members: definitiveSet{memberSet: memberSet{"101": {}}, definitive: true}}
	adapter, plan := scopeDropAdapter(t, chain, admission.PlanContext{TargetPlan: target}, &drops)
	dimensions := map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"102"`), "device": json.RawMessage(`"sda"`)}
	deliverSeries(t, adapter, map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"101"`), "device": json.RawMessage(`"sda"`)})
	deliverSeries(t, adapter, dimensions)

	want, err := contract.MonitorDedupeMD5("42", plan.BusinessID, dimensions,
		contract.MonitorOutputIdentity{DimensionFields: []string{"bk_host_id", "device"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(drops) != 1 {
		t.Fatalf("drops = %+v, want only the out-of-target series", drops)
	}
	drop := drops[0]
	if !drop.Definitive || drop.Fingerprint != want || drop.Filter != "target_plan" || drop.Reason != admission.TargetPlanReasonOutOfTarget ||
		drop.Plan != plan || drop.StrategyRevision != 3 || drop.Round != 1700000060 {
		t.Fatalf("drop = %+v, want definitive out_of_target with fingerprint %s, revision 3, round 1700000060", drop, want)
	}
}

// A rejection the target could not stand behind reaches the close as not
// definitive and without a fingerprint, so nothing downstream can act on it.
func TestAnIndefiniteRejectionCarriesNoFingerprint(t *testing.T) {
	var drops []ScopeDrop
	chain := admission.NewChain([]admission.Fuller{admission.IdentityFuller{}}, []admission.Filter{admission.TargetPlanFilter{}})
	target := &admission.TargetPlanContext{Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true},
		Members: definitiveSet{memberSet: memberSet{"101": {}}}}
	adapter, _ := scopeDropAdapter(t, chain, admission.PlanContext{TargetPlan: target}, &drops)
	deliverSeries(t, adapter, map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"102"`)})
	if len(drops) != 1 || drops[0].Definitive || drops[0].Fingerprint != "" {
		t.Fatalf("drops = %+v, want one indefinite drop without a fingerprint", drops)
	}
}

// A host turned away by the host status filter is still inside its target;
// the close does not hear about it at all.
func TestAHostStatusRejectionIsNotReported(t *testing.T) {
	var drops []ScopeDrop
	filter, installed := admission.NewHostStatusFilter([]string{"spare"})
	if !installed {
		t.Fatal("host status filter not installed")
	}
	chain := admission.NewChain([]admission.Fuller{admission.IdentityFuller{}, stateFuller{state: "spare"}},
		[]admission.Filter{filter, admission.TargetScopeFilter{}})
	adapter, _ := scopeDropAdapter(t, chain, admission.PlanContext{TargetScope: hostScope("7")}, &drops)
	deliverSeries(t, adapter, map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"7"`)})
	if len(drops) != 0 {
		t.Fatalf("drops = %+v, want the host status rejection unreported", drops)
	}
}

// stateFuller resolves every host into one operational state, standing in
// for the host cache.
type stateFuller struct{ state string }

func (stateFuller) Name() string { return "state" }
func (fuller stateFuller) Fill(_ map[string]json.RawMessage, facts *admission.Facts) {
	facts.HostResolved = true
	facts.HostState = fuller.state
}

// The outputs follow the frozen Plan: its strategy reference and output
// identity, and no fingerprint where the evaluator would compute none.
func TestPlanOutputsFollowTheFrozenPlan(t *testing.T) {
	identity := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "2002"}
	compiled := compilePlanWithTargetPlan(t, "2002", nil)
	outputs := buildPlanOutputs([]execution.DuePlan{{Identity: identity, CompiledPlan: compiled}})
	output, found := outputs[identity]
	if !found || output.strategyID != compiled.StrategyRef().StrategyID || output.revision != int64(compiled.StrategyRef().SnapshotRevision) {
		t.Fatalf("output = %+v", output)
	}
	if compiled.StrategyRef().SnapshotRevision == 0 && output.fingerprint("2", map[string]json.RawMessage{"host": json.RawMessage(`"a"`)}) != "" {
		t.Fatal("a Plan without a frozen revision was given a fingerprint the evaluator never computes")
	}
}
