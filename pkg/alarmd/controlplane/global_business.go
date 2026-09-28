// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

import (
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// ReasonGlobalBusinessUnsupported withholds a global business strategy this
// build cannot run as one. The detail names which condition it failed
// (reason=<word>); the words are below.
const ReasonGlobalBusinessUnsupported = "GLOBAL_BUSINESS_UNSUPPORTED"

// Why a global business strategy is withheld.
const (
	// GlobalBusinessLegacyTarget: the strategy names its target in the old
	// form. That path's no-data roster keeps only the hosts of the Plan's
	// own business, so every other business's hosts would lose no-data
	// detection without a word.
	GlobalBusinessLegacyTarget = "legacy_target"
	// GlobalBusinessQueryKind: the query is not a structured time series
	// query over the metric router. PromQL is scoped only by the space the
	// provider is told, FTA conditions carry the Plan's own business, and
	// how logs, events and the computing platform's tables are routed with
	// the space skipped has not been established.
	GlobalBusinessQueryKind = "query_kind"
	// GlobalBusinessQueryTable: a query names no table or data label. With
	// the space skipped the provider has no space to find tables in, so
	// such a query would answer empty on every round.
	GlobalBusinessQueryTable = "query_table"
	// GlobalBusinessOutputProtocol: the Plan would publish the compatible
	// event, which has no place for the business an alert is about.
	GlobalBusinessOutputProtocol = "output_protocol"
)

// globalBusinessQuerySemantics are the query sources a global business Plan
// may read: the structured time series the metric router serves. An empty
// SourceSemantics is the first of them alone.
var globalBusinessQuerySemantics = map[string]struct{}{
	"bk_monitor/time_series": {},
	"custom/time_series":     {},
}

// globalBusinessRefusal is the admission a global business strategy passes
// before its Plan is compiled, and nil for one that passes or is not global.
// The output protocol is decided later and checked where it is.
func globalBusinessRefusal(
	sourceID string, identity SourceIdentity, targetScope *contract.TargetScopeV2, facts execution.QueryPlanFacts,
) *ObjectDisposition {
	if !identity.GlobalBusiness {
		return nil
	}
	if targetScope != nil {
		refusal := globalBusinessUnsupported(sourceID, GlobalBusinessLegacyTarget, "items[0].target")
		return &refusal
	}
	if facts.PromQL != nil {
		refusal := globalBusinessUnsupported(sourceID, GlobalBusinessQueryKind, "items[0].query_configs")
		return &refusal
	}
	for _, semantics := range facts.SourceSemantics {
		if _, supported := globalBusinessQuerySemantics[semantics]; !supported {
			refusal := globalBusinessUnsupported(sourceID, GlobalBusinessQueryKind, "items[0].query_configs")
			return &refusal
		}
	}
	for _, clause := range facts.QueryList {
		if clause.FieldSemantics != "" || clause.SourceConditions != nil {
			refusal := globalBusinessUnsupported(sourceID, GlobalBusinessQueryKind, "items[0].query_configs")
			return &refusal
		}
		if !namesTableOrDataLabel(clause.TableID) {
			refusal := globalBusinessUnsupported(sourceID, GlobalBusinessQueryTable, "items[0].query_configs")
			return &refusal
		}
	}
	return nil
}

// namesTableOrDataLabel reports whether a query's table id routes on its
// own: the provider splits it at the first dot and finds tables by the part
// before it, a data label or the database of a table.
func namesTableOrDataLabel(tableID string) bool {
	database, _, _ := strings.Cut(tableID, ".")
	return strings.TrimSpace(database) != ""
}

func globalBusinessUnsupported(sourceID, reason, fieldPath string) ObjectDisposition {
	return ObjectDisposition{
		SourceID: sourceID, Scope: "PLAN", Disposition: DispositionUnsupported,
		Reason: ReasonGlobalBusinessUnsupported, FieldPath: fieldPath, Detail: "reason=" + reason,
	}
}
