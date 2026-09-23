// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package admission

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// DefinitiveMembership is what a target plan resolution says about itself
// when it is asked whether its "not a member" can be acted on. Only a
// resolution that knows every one of its selectors answered from current
// facts says yes; one that is not asked (a membership without the method)
// never does.
type DefinitiveMembership interface {
	Definitive() bool
}

// DefinitelyOutside reports whether a rejection is the monitoring target
// itself saying the record is outside it, decided on facts that were all
// there and current. It is the one question the target-scope close asks of
// a rejection, and it answers yes for far less than every rejection:
//
//   - only the two target filters count. A host turned away for its
//     operational state (a spare, a machine under repair) is still in the
//     strategy's target, and its alert is not this close's to end;
//   - only the plain out-of-target reason counts. A record whose key or
//     object identity could not be built, or a target nobody resolved, is a
//     gap on some side, not a record placed outside;
//   - the facts it was decided on must have been read. A host the record
//     names and the host cache did not find may be a host the cache has not
//     learned yet, and a target plan whose selectors did not all answer
//     from fresh facts is a lower bound, not the target.
//
// Every "no" here costs a close that could have been sent; every wrong
// "yes" closes an alert that is still in scope. The rule leans to the first.
func DefinitelyOutside(plan PlanContext, facts *Facts, filter, reason string) bool {
	switch filter {
	case TargetScopeFilter{}.Name():
		if reason != contract.TargetScopeReasonOutOfScope || facts == nil || facts.HostFactsUnavailable {
			return false
		}
		if _, named := facts.HostNaming.LookupKey(); named && !facts.HostResolved {
			return false
		}
		return true
	case TargetPlanFilter{}.Name():
		if reason != TargetPlanReasonOutOfTarget || plan.TargetPlan == nil || plan.TargetPlan.Members == nil ||
			facts == nil || facts.HostFactsUnavailable {
			return false
		}
		membership, knows := plan.TargetPlan.Members.(DefinitiveMembership)
		return knows && membership.Definitive()
	}
	return false
}
