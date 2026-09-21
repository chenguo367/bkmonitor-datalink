// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package access

import (
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/admission"
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
