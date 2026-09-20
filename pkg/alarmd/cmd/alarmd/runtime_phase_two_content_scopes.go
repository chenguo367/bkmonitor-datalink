// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// contentScopeSource is what the reconcile round needs of the catalog to
// name each Query Group's content: the activation that says which
// publication the fleet executes, and that publication's manifest.
type contentScopeSource interface {
	LoadActivation(context.Context) (controlplane.ActivationState, error)
	LoadCatalogManifest(context.Context, execution.SnapshotRevision) (controlplane.CatalogManifest, error)
}

// currentContentScopes reads the content each Query Group is published with
// (decision-016): the ObjectDigest the current activation's manifest names
// for it -- the same digest the Query Group's open Segment carries, which is
// what a worker's Slot declares. Both reads are the repository's cached ones,
// so a round costs the header check they already cost.
func currentContentScopes(source contentScopeSource) func(context.Context) (map[execution.QueryGroupIdentity]string, error) {
	return func(ctx context.Context) (map[execution.QueryGroupIdentity]string, error) {
		if source == nil {
			return nil, errors.New("phase-two content scopes: catalog repository is required")
		}
		state, err := source.LoadActivation(ctx)
		if err != nil {
			return nil, fmt.Errorf("phase-two content scopes: read activation: %w", err)
		}
		manifest, err := source.LoadCatalogManifest(ctx, state.Current.SnapshotRevision)
		if err != nil {
			return nil, fmt.Errorf("phase-two content scopes: read manifest %s: %w", state.Current.SnapshotRevision, err)
		}
		digests := make(map[execution.QueryGroupIdentity]string, len(manifest.QueryGroups))
		for _, entry := range manifest.QueryGroups {
			if entry.QueryGroup == "" || entry.ObjectDigest == "" {
				continue
			}
			digests[entry.QueryGroup] = string(entry.ObjectDigest)
		}
		return digests, nil
	}
}
