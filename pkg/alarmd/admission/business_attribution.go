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

// ClusterBusinessReader is the platform's published BCS cluster -> business
// mapping, asked by cluster id.
type ClusterBusinessReader interface {
	LookupClusterBusiness(clusterID string) (string, bool)
}

// BusinessLookups are the caches attribution reads, each at the moment it
// is asked. Either may be nil: a nil host cache holds no host, and a nil
// cluster mapping maps no cluster.
type BusinessLookups struct {
	Hosts    HostBusinessReader
	Clusters ClusterBusinessReader
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
//  3. The record's bcs_cluster_id, when the strategy groups by it: the
//     business the platform published for that cluster. A cluster the
//     mapping does not hold is filed under the global business and counted
//     as unmapped, apart from a record that named no cluster at all.
//  4. The Plan's own business: a strategy that neither targets nor groups by
//     business or cluster aggregates across businesses, and its alert is the
//     global business's own.
//
// The caches are read now, not when the record was admitted: a host cache
// or a mapping refreshed in between answers with the current business.
func AttributeBusiness(
	target *contract.TargetPlanV1, dimensionFields []string, planBusiness string,
	dimensions map[string]json.RawMessage, lookups BusinessLookups,
) BusinessAttribution {
	if business, found := targetBusiness(target, dimensions, lookups.Hosts); found {
		return BusinessAttribution{BusinessID: business, Source: contract.BusinessAttributionTarget}
	}
	if groupsBy(dimensionFields, contract.BusinessDimension) {
		if business, found := canonicalBusiness(dimensionText(dimensions, contract.BusinessDimension)); found {
			return BusinessAttribution{BusinessID: business, Source: contract.BusinessAttributionDimension}
		}
	}
	if groupsBy(dimensionFields, contract.ClusterDimension) {
		if cluster := dimensionText(dimensions, contract.ClusterDimension); cluster != "" {
			if business, found := clusterBusiness(lookups.Clusters, cluster); found {
				return BusinessAttribution{BusinessID: business, Source: contract.BusinessAttributionCluster}
			}
			return BusinessAttribution{BusinessID: planBusiness, Source: contract.BusinessAttributionUnmapped}
		}
	}
	return BusinessAttribution{BusinessID: planBusiness, Source: contract.BusinessAttributionGlobal}
}

func clusterBusiness(clusters ClusterBusinessReader, cluster string) (string, bool) {
	if clusters == nil {
		return "", false
	}
	business, found := clusters.LookupClusterBusiness(cluster)
	if !found {
		return "", false
	}
	return canonicalBusiness(business)
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

func groupsBy(dimensionFields []string, dimension string) bool {
	for _, field := range dimensionFields {
		if field == dimension {
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
