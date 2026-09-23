// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package access

import (
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// SeriesAdmission is the access-path decision Python makes in its filter chain:
// a series is evaluated for a plan only if it falls inside that strategy's
// monitoring target.
//
// It is split in two because enrichment is per series and admission is per
// plan. One series commonly feeds several plans, and deriving its facts once
// per plan would repeat the CMDB lookup for every one of them.
type SeriesAdmission interface {
	Enrich(dimensions map[string]json.RawMessage) admission.Facts
	Admit(plan admission.PlanContext, facts *admission.Facts) (bool, string, string)
}

// AdmissionObserver counts decisions. It is called once per series per plan, so
// it must stay allocation-free.
type AdmissionObserver func(filter, result, reason string)

// ScopeDrop is one series a Plan's monitoring target turned away, as the
// target-scope close needs it: which Plan, whether the rejection was the
// target's own verdict on current facts (admission.DefinitelyOutside), and,
// only then, the fingerprint the evaluator would have given the record.
//
// Fingerprint is empty when the rejection was not definitive, and when the
// Plan does not produce fingerprinted alerts at all (no frozen revision or
// no output identity - the evaluator computes none there either), or the
// record's dimensions cannot be fingerprinted.
type ScopeDrop struct {
	Plan             execution.PlanIdentity
	Filter, Reason   string
	Definitive       bool
	Fingerprint      string
	StrategyRevision int64
	// Round is the Slot's evaluation time: two drops of one fingerprint are
	// two observations only when they come from two Slots.
	Round int64
}

// ScopeDropObserver receives the target filters' rejections. It is called
// on the query's own goroutine, once per rejected series per Plan, and must
// not block.
type ScopeDropObserver func(ScopeDrop)

// planOutput is what the evaluator fingerprints a Plan's records under: the
// frozen strategy reference and the output identity. Held beside the scopes
// so a rejected record can be given the fingerprint the evaluator would
// have given it had the record been admitted.
type planOutput struct {
	strategyID string
	revision   int64
	identity   *contract.MonitorOutputIdentity
}

type planOutputs map[execution.PlanIdentity]planOutput

func buildPlanOutputs(duePlans []execution.DuePlan) planOutputs {
	outputs := make(planOutputs, len(duePlans))
	for _, due := range duePlans {
		if due.CompiledPlan == nil {
			continue
		}
		ref := due.CompiledPlan.StrategyRef()
		outputs[due.Identity] = planOutput{strategyID: ref.StrategyID, revision: int64(ref.SnapshotRevision),
			identity: due.CompiledPlan.OutputIdentity()}
	}
	return outputs
}

// fingerprint is trigger.EvaluateV2's dedupe identity for a record of this
// Plan: the same function over the same strategy, business, dimensions and
// output identity, under the same condition that there is a frozen revision
// and an identity at all.
func (output planOutput) fingerprint(businessID string, dimensions map[string]json.RawMessage) string {
	if output.revision <= 0 || output.identity == nil {
		return ""
	}
	fingerprint, err := contract.MonitorDedupeMD5(output.strategyID, businessID, dimensions, *output.identity)
	if err != nil {
		return ""
	}
	return fingerprint
}

// reportScopeDrop hands a target filter's rejection to the target-scope
// close. The fingerprint is computed only for a definitive rejection: it is
// the one the close can act on, and the only one worth an MD5 per series.
// Rejections by any other filter - the host status filter above all - are
// not reported: the record is still inside its target.
func (adapter *seriesAdapter) reportScopeDrop(identity execution.PlanIdentity, plan admission.PlanContext, facts *admission.Facts, filter, reason string) {
	if adapter.scopeDrop == nil {
		return
	}
	if filter != (admission.TargetScopeFilter{}).Name() && filter != (admission.TargetPlanFilter{}).Name() {
		return
	}
	drop := ScopeDrop{Plan: identity, Filter: filter, Reason: reason, Round: adapter.round,
		Definitive: admission.DefinitelyOutside(plan, facts, filter, reason)}
	if drop.Definitive {
		output := adapter.outputs[identity]
		drop.StrategyRevision = output.revision
		drop.Fingerprint = output.fingerprint(identity.BusinessID, facts.Dimensions)
	}
	adapter.scopeDrop(drop)
}

// planScopes indexes the frozen monitoring targets of the plans in one
// execution. It is built once per execution rather than looked up per series.
type planScopes map[execution.PlanIdentity]admission.PlanContext

func buildPlanScopes(duePlans []execution.DuePlan, targets execution.TargetMemberships) planScopes {
	scopes := make(planScopes, len(duePlans))
	for _, due := range duePlans {
		context := admission.PlanContext{
			TenantID:    due.Identity.TenantID,
			BusinessID:  due.Identity.BusinessID,
			StrategyID:  due.Identity.StrategyID,
			TargetScope: admission.TargetScopeFromContract(due.CompiledPlan.TargetScope()),
		}
		if plan := due.CompiledPlan.TargetPlan(); plan != nil {
			// The resolution is the Slot's, looked up by Plan; a Plan the Slot
			// did not resolve keeps Members nil and admits nothing.
			context.TargetPlan = &admission.TargetPlanContext{Identity: plan.Identity}
			if members, resolved := targets[due.Identity]; resolved && members != nil {
				context.TargetPlan.Members = members
			}
		}
		scopes[due.Identity] = context
	}
	return scopes
}

// seriesDimensions reads the dimensions of a series batch. Every record in the
// batch belongs to the same series, so the first one names it.
func seriesDimensions(dataset *execution.Dataset) map[string]json.RawMessage {
	if dataset == nil || dataset.Len() == 0 {
		return nil
	}
	record, found := dataset.Record(0)
	if !found {
		return nil
	}
	return record.Dimensions()
}
