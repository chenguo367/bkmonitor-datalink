// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"

// SegmentSkipAdmitted is whether a Slot dispatched with the segment skip word
// leaves its stable members' writes out (design 014 B3 stage 3): only when
// the word names the Leader whose view the worker has installed, viewEpoch,
// read once as the Slot begins. A Slot with no word skips nothing and
// ignores nothing; one whose word is not followed says why
// (observability.AllSegmentSkipIgnoredReasons).
func SegmentSkipAdmitted(word, viewEpoch uint64, installed bool) (bool, string) {
	switch {
	case word == 0:
		return false, ""
	case !installed:
		return false, observability.SegmentSkipIgnoredNoView
	case viewEpoch != word:
		return false, observability.SegmentSkipIgnoredEpochMismatch
	}
	return true, ""
}
