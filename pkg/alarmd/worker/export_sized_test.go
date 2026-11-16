// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "testing"

// SetSizedSplitSeriesForTest has the splits kept for a slice's size judge it
// against series in place of sliceSeries until t ends: a fixture of a slice
// past 87,000 series, read whole, is past what a test's budget holds.
func SetSizedSplitSeriesForTest(t testing.TB, series uint64) {
	t.Helper()
	before := sizedSplitSeriesOverride.Swap(series)
	t.Cleanup(func() { sizedSplitSeriesOverride.Store(before) })
}
