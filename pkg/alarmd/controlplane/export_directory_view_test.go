// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

// RememberPublicationForTest has the reconciler hold catalog as the
// publication it just made, as a round that published it does.
func (reconciler *SourceReconciler) RememberPublicationForTest(publication SnapshotPublicationRef, catalog Catalog) {
	reconciler.rememberLastGood(publication, catalog)
}

// ContentMemoForTest is the content memo alone, for a test of what it keeps.
type ContentMemoForTest struct{ memo publishedContentMemo }

// Store remembers content as a read of it does.
func (memo *ContentMemoForTest) Store(content PublishedContent) { memo.memo.store(content) }

// Holds says whether the memo answers publication.
func (memo *ContentMemoForTest) Holds(publication SnapshotPublicationRef) bool {
	_, held := memo.memo.lookup(publication)
	return held
}
