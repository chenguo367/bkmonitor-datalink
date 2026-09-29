// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

import (
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// RememberedManifestsForTest is the revisions whose manifests the next refresh
// walks without reading them, in order.
func (d *ObservationDirectory) RememberedManifestsForTest() []execution.SnapshotRevision {
	d.refresh.Lock()
	defer d.refresh.Unlock()
	revisions := make([]execution.SnapshotRevision, 0, len(d.manifests))
	for revision := range d.manifests {
		revisions = append(revisions, revision)
	}
	slices.Sort(revisions)
	return revisions
}

// RememberManifestForTest has the directory hold an empty manifest for
// revision as one it walked.
func (d *ObservationDirectory) RememberManifestForTest(revision execution.SnapshotRevision) {
	d.refresh.Lock()
	defer d.refresh.Unlock()
	d.manifests[revision] = directoryManifest{revision: revision}
}

// StoreSlotManifestForTest puts manifest in the Slot path's manifest cache, as
// a Segment freshness check does.
func (repository *RedisCatalogRepository) StoreSlotManifestForTest(manifest CatalogManifest) {
	repository.manifestCache.store(manifest.SnapshotRevision, manifest)
}

// SlotManifestForTest looks revision up in the Slot path's manifest cache.
func (repository *RedisCatalogRepository) SlotManifestForTest(revision execution.SnapshotRevision) (CatalogManifest, bool) {
	return repository.manifestCache.lookup(revision)
}
