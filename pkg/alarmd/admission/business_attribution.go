// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package admission

import (
	"encoding/json"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// HostBusinessReader is the host cache asked which business a host is in,
// by the identity the record names it with: a host id, or "ip|cloud".
type HostBusinessReader interface {
	LookupHostBusiness(identity string) (string, bool)
}

// BusinessAttribution is what a global business Plan's event is filed
// under, and where that came from (one of contract.BusinessAttributionSources).
type BusinessAttribution struct {
	BusinessID string
	Source     string
}

// AttributeBusiness names the business a global business Plan's event on
// one record is about. It follows the strategy's configuration and nothing
// else, taking the first of these the strategy configures and the record
// answers:
//
//  1. The target. A host target (host_id, or model_inst_id over hosts) gives
//     the business of the record's host, found the way admission finds it -
//     HostNaming.LookupKey, then the host cache - so a record that names its
//     host only by address and cloud is attributed like one with a host id.
//     A Kubernetes target gives the business configured on the static
//     target the record's key matches.
//  2. The bk_biz_id aggregation dimension, when the strategy groups by it
//     and the record carries a business there.
//  3. The Plan's own business: a strategy that neither targets nor groups by
//     business aggregates across businesses, and its alert is the global
//     business's own.
//
// The host is looked up now, not when the record was admitted: a host cache
// refreshed in between answers with the host's current business.
func AttributeBusiness(
	target *contract.TargetPlanV1, dimensionFields []string, planBusiness string,
	dimensions map[string]json.RawMessage, hosts HostBusinessReader,
) BusinessAttribution {
	if business, found := targetBusiness(target, dimensions, hosts); found {
		return BusinessAttribution{BusinessID: business, Source: contract.BusinessAttributionTarget}
	}
	if groupsByBusiness(dimensionFields) {
		if business, found := canonicalBusiness(dimensionText(dimensions, contract.BusinessDimension)); found {
			return BusinessAttribution{BusinessID: business, Source: contract.BusinessAttributionDimension}
		}
	}
	return BusinessAttribution{BusinessID: planBusiness, Source: contract.BusinessAttributionGlobal}
}

func targetBusiness(target *contract.TargetPlanV1, dimensions map[string]json.RawMessage, hosts HostBusinessReader) (string, bool) {
	if target == nil {
		return "", false
	}
	if target.Identity.HostIdentity {
		if hosts == nil {
			return "", false
		}
		facts := &Facts{}
		IdentityFuller{}.Fill(dimensions, facts)
		identity, named := facts.HostNaming.LookupKey()
		if !named {
			return "", false
		}
		business, held := hosts.LookupHostBusiness(identity)
		if !held {
			return "", false
		}
		return canonicalBusiness(business)
	}
	key, placed := target.Identity.Key(func(name string) string { return dimensionText(dimensions, name) })
	if !placed {
		return "", false
	}
	return target.StaticBusiness(key)
}

func groupsByBusiness(dimensionFields []string) bool {
	for _, field := range dimensionFields {
		if field == contract.BusinessDimension {
			return true
		}
	}
	return false
}

// canonicalBusiness reads a business id as its canonical decimal text, and
// only a positive one: zero is the platform's "belongs to no business", and
// a negative id is a space that is not a CMDB business, which the alert
// consumer does not take as an alert's business label. Either falls through
// to the next source rather than becoming the label.
func canonicalBusiness(text string) (string, bool) {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return "", false
	}
	return strconv.FormatInt(value, 10), true
}
