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

// recordingSink is the close as the access path sees it: what Screen
// answers, and every call it received.
type recordingSink struct {
	screen   string
	screens  int
	observed []ScopeDrop
	counts   map[string]int
}

func (sink *recordingSink) Screen(execution.PlanIdentity) string { sink.screens++; return sink.screen }
func (sink *recordingSink) Observe(drop ScopeDrop)               { sink.observed = append(sink.observed, drop) }
func (sink *recordingSink) Count(_ execution.PlanIdentity, word string, n int) {
	if sink.counts == nil {
		sink.counts = map[string]int{}
	}
	sink.counts[word] += n
}

var scopeOutput = planOutput{strategyID: "42", revision: 3,
	identity: &contract.MonitorOutputIdentity{DimensionFields: []string{"bk_host_id", "device"}}}

func hostPlanContext(definitive bool) admission.PlanContext {
	return admission.PlanContext{TargetPlan: &admission.TargetPlanContext{
		Identity: contract.TargetPlanIdentityV1{Dimensions: []string{"bk_host_id"}, HostIdentity: true},
		Members:  definitiveSet{memberSet: memberSet{"101": {}}, definitive: definitive}}}
}

func scopeDropAdapter(t *testing.T, chain *admission.Chain, context admission.PlanContext, sink *recordingSink, output planOutput) (*seriesAdapter, execution.PlanIdentity) {
	t.Helper()
	_, frozen := frozenExecution(t)
	requirement := frozen.Requirements[0]
	plan := requirement.Consumers[0].Consumer.Plan
	context.StrategyID = plan.StrategyID
	return &seriesAdapter{
		consumer: &admissionConsumer{}, query: plannedQueryForTest(requirement), attemptNo: 1,
		admission: chain, scopes: planScopes{plan: context}, scopeSink: sink,
		outputs: planOutputs{plan: output}, round: 1700000060,
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

func targetChain() *admission.Chain {
	return admission.NewChain([]admission.Fuller{admission.IdentityFuller{}}, []admission.Filter{admission.TargetScopeFilter{}, admission.TargetPlanFilter{}})
}

func hostDims(id string) map[string]json.RawMessage {
	return map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"` + id + `"`), "device": json.RawMessage(`"sda"`)}
}

// A definitive target-plan rejection of a strategy the close cleared
// reaches it with the fingerprint the evaluator would have given the
// record: the monitor dedupe identity of the frozen strategy, the Plan's
// business and the record's own dimensions under the Plan's output
// identity.
func TestADefinitiveRejectionCarriesTheEvaluatorsFingerprint(t *testing.T) {
	sink := &recordingSink{}
	adapter, plan := scopeDropAdapter(t, targetChain(), hostPlanContext(true), sink, scopeOutput)
	deliverSeries(t, adapter, hostDims("101"))
	deliverSeries(t, adapter, hostDims("102"))
	adapter.flushScopeDrops()

	want, err := contract.MonitorDedupeMD5("42", plan.BusinessID, hostDims("102"),
		contract.MonitorOutputIdentity{DimensionFields: []string{"bk_host_id", "device"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.observed) != 1 || len(sink.counts) != 0 {
		t.Fatalf("observed %+v counted %v, want only the out-of-target series observed", sink.observed, sink.counts)
	}
	drop := sink.observed[0]
	if drop.Fingerprint != want || drop.Filter != "target_plan" || drop.Reason != admission.TargetPlanReasonOutOfTarget ||
		drop.Plan != plan || drop.StrategyRevision != 3 || drop.Round != 1700000060 {
		t.Fatalf("drop = %+v, want out_of_target with fingerprint %s, revision 3, round 1700000060", drop, want)
	}
}

// Every other rejection is counted in bulk, once per word, when the query
// ends: the indefinite ones, a Plan fed by several inputs, a Plan with no
// fingerprinted output, and whatever Screen turned away.
func TestRejectionsTheCloseCannotUseAreCountedInBulk(t *testing.T) {
	cases := []struct {
		name    string
		context admission.PlanContext
		output  planOutput
		screen  string
		word    string
		screens int
	}{
		{"indefinite", hostPlanContext(false), scopeOutput, "", ScopeDropIndefinite, 0},
		{"multi-input", hostPlanContext(true), func() planOutput { o := scopeOutput; o.multiInput = true; return o }(), "", ScopeDropFingerprintUnsupported, 0},
		{"no frozen revision", hostPlanContext(true), planOutput{strategyID: "42", identity: scopeOutput.identity}, "", ScopeDropNoFingerprint, 1},
		{"screened out", hostPlanContext(true), scopeOutput, "not_member", "not_member", 1},
	}
	for _, c := range cases {
		sink := &recordingSink{screen: c.screen}
		adapter, _ := scopeDropAdapter(t, targetChain(), c.context, sink, c.output)
		for _, id := range []string{"102", "103", "104"} {
			deliverSeries(t, adapter, hostDims(id))
		}
		if len(sink.counts) != 0 {
			t.Fatalf("%s: counted before the query ended: %v", c.name, sink.counts)
		}
		adapter.flushScopeDrops()
		if len(sink.observed) != 0 || sink.counts[c.word] != 3 || len(sink.counts) != 1 || sink.screens != c.screens {
			t.Errorf("%s: observed %d counts %v screens %d, want 3 under %q and %d screens", c.name, len(sink.observed), sink.counts, sink.screens, c.word, c.screens)
		}
	}
}

// A host turned away by the host status filter is still inside its target;
// the close does not hear about it at all.
func TestAHostStatusRejectionIsNotReported(t *testing.T) {
	filter, installed := admission.NewHostStatusFilter([]string{"spare"})
	if !installed {
		t.Fatal("host status filter not installed")
	}
	chain := admission.NewChain([]admission.Fuller{admission.IdentityFuller{}, stateFuller{state: "spare"}},
		[]admission.Filter{filter, admission.TargetScopeFilter{}})
	sink := &recordingSink{}
	adapter, _ := scopeDropAdapter(t, chain, admission.PlanContext{TargetScope: hostScope("7")}, sink, scopeOutput)
	deliverSeries(t, adapter, map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"7"`)})
	adapter.flushScopeDrops()
	if len(sink.observed) != 0 || len(sink.counts) != 0 || sink.screens != 0 {
		t.Fatalf("sink = %+v, want the host status rejection unreported", sink)
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

// The outputs follow the frozen Plan and the queries that feed it: its
// strategy reference and output identity, no fingerprint where the
// evaluator would compute none, and multi-input when more than one
// requirement feeds it.
func TestPlanOutputsFollowTheFrozenPlan(t *testing.T) {
	identity := execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "2002"}
	compiled := compilePlanWithTargetPlan(t, "2002", nil)
	consumer := execution.DataRequirementConsumer{Consumer: execution.ConsumerRef{Plan: identity}}
	one := []PlannedQuery{{Requirements: []execution.DataRequirement{{RequirementID: "a", Consumers: []execution.DataRequirementConsumer{consumer}}}}}
	outputs := buildPlanOutputs([]execution.DuePlan{{Identity: identity, CompiledPlan: compiled}}, one)
	output, found := outputs[identity]
	if !found || output.multiInput || output.strategyID != compiled.StrategyRef().StrategyID || output.revision != int64(compiled.StrategyRef().SnapshotRevision) {
		t.Fatalf("output = %+v", output)
	}
	if compiled.StrategyRef().SnapshotRevision == 0 && output.fingerprint("2", map[string]json.RawMessage{"host": json.RawMessage(`"a"`)}) != "" {
		t.Fatal("a Plan without a frozen revision was given a fingerprint the evaluator never computes")
	}
	two := append(one, PlannedQuery{Requirements: []execution.DataRequirement{{RequirementID: "b", Consumers: []execution.DataRequirementConsumer{consumer}}}})
	if !buildPlanOutputs([]execution.DuePlan{{Identity: identity, CompiledPlan: compiled}}, two)[identity].multiInput {
		t.Fatal("a Plan fed by two requirements was not marked multi-input")
	}
}

// The zero-member path - the strategy has no open alert, the common case
// on a filtered wide table - must cost nothing per record: no hash, no
// allocation, no call into the sink (and so no lock). One Screen per Plan
// per query, one Count when the query ends.
func TestTheZeroMemberPathDoesNoPerRecordWork(t *testing.T) {
	sink := &recordingSink{screen: "not_member"}
	identity := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "42"}
	adapter := &seriesAdapter{scopeSink: sink, outputs: planOutputs{identity: scopeOutput}}
	facts := admission.Facts{Dimensions: hostDims("102")}
	plan := admission.PlanContext{TargetScope: &admission.TargetScope{}}
	reject := func() {
		adapter.reportScopeDrop(identity, plan, &facts, "target_scope", contract.TargetScopeReasonOutOfScope)
	}
	reject()
	if allocs := testing.AllocsPerRun(1000, reject); allocs != 0 {
		t.Fatalf("zero-member path allocates %.1f per record, want 0", allocs)
	}
	if sink.screens != 1 || len(sink.observed) != 0 {
		t.Fatalf("screens %d observed %d, want one screen and no observation", sink.screens, len(sink.observed))
	}
	adapter.flushScopeDrops()
	if sink.counts["not_member"] != 1002 {
		t.Fatalf("counts = %v, want all 1002 rejections in one bulk count", sink.counts)
	}
}

// BenchmarkReportScopeDrop measures one definitive rejection: zero_members
// is the path almost every rejection takes, members the path of a strategy
// with open alerts (hash plus the sink's lookup).
func BenchmarkReportScopeDrop(b *testing.B) {
	identity := execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "42"}
	facts := admission.Facts{Dimensions: map[string]json.RawMessage{"bk_host_id": json.RawMessage(`"102"`),
		"device": json.RawMessage(`"sda"`), "mount": json.RawMessage(`"/data"`)}}
	plan := admission.PlanContext{TargetScope: &admission.TargetScope{}}
	output := planOutput{strategyID: "42", revision: 3,
		identity: &contract.MonitorOutputIdentity{DimensionFields: []string{"bk_host_id", "device", "mount"}}}
	for _, c := range []struct{ name, screen string }{{"zero_members", "not_member"}, {"members", ""}} {
		b.Run(c.name, func(b *testing.B) {
			adapter := &seriesAdapter{scopeSink: &discardSink{screen: c.screen}, outputs: planOutputs{identity: output}}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				adapter.reportScopeDrop(identity, plan, &facts, "target_scope", contract.TargetScopeReasonOutOfScope)
			}
		})
	}
}

type discardSink struct{ screen string }

func (sink *discardSink) Screen(execution.PlanIdentity) string      { return sink.screen }
func (sink *discardSink) Observe(ScopeDrop)                         {}
func (sink *discardSink) Count(execution.PlanIdentity, string, int) {}
