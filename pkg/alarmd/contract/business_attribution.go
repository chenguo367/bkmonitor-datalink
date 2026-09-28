// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package contract

// Where a global business Plan's event took its business from, in the order
// they are consulted: the target the record is in (the host's business, or
// the business configured on the matched Kubernetes static target), then the
// record's bk_biz_id aggregation dimension, then the business the platform
// published for the record's bcs_cluster_id, then the Plan's own business -
// a strategy that neither targets nor groups by business or cluster
// aggregates across businesses, and its alert belongs to the global
// business itself.
//
// They are the source label of the attribution counter. Unmapped is the
// global business too, taken because the record named a cluster the
// published mapping does not hold: a writer that does not publish the
// mapping yet, a cluster it left out, or one registered since. A strategy
// that configures a target or a business dimension and still lands on
// global says the business it relied on was not in the cache or the data.
const (
	BusinessAttributionTarget    = "target"
	BusinessAttributionDimension = "dimension"
	BusinessAttributionCluster   = "cluster"
	BusinessAttributionUnmapped  = "unmapped"
	BusinessAttributionGlobal    = "global"
)

// BusinessAttributionSources lists the sources in consultation order.
var BusinessAttributionSources = []string{
	BusinessAttributionTarget, BusinessAttributionDimension, BusinessAttributionCluster,
	BusinessAttributionUnmapped, BusinessAttributionGlobal,
}

// BusinessDimension is the aggregation dimension a record names its
// business under. Only the literal name counts: a result table's alias for
// it is an ordinary dimension here.
const BusinessDimension = "bk_biz_id"

// ClusterDimension is the aggregation dimension Kubernetes data names its
// BCS cluster under, the key of the published cluster -> business mapping.
const ClusterDimension = "bcs_cluster_id"
