// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Both cells read zero before anything is counted - "about zero" is the
// reading the counter exists for, and a missing series is not one - and a
// count lands under whether the Plan groups by bk_target_ip.
func TestADifferingAddressIsCountedUnderItsGrouping(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	// Counted before anything here asks for a cell, which would create it.
	if cells := testutil.CollectAndCount(r.phaseTwo.cmdbAddressDiffers); cells != 2 {
		t.Fatalf("%d cells from start, want both", cells)
	}
	grouped, ungrouped := r.phaseTwo.cmdbAddressDiffers.WithLabelValues("true"), r.phaseTwo.cmdbAddressDiffers.WithLabelValues("false")
	if testutil.ToFloat64(grouped) != 0 || testutil.ToFloat64(ungrouped) != 0 {
		t.Fatal("the two cells do not start at zero")
	}
	r.RecordAddressDiffers(true)
	r.RecordAddressDiffers(true)
	r.RecordAddressDiffers(false)
	if testutil.ToFloat64(grouped) != 2 || testutil.ToFloat64(ungrouped) != 1 {
		t.Fatalf("grouped %v, ungrouped %v; want 2 and 1", testutil.ToFloat64(grouped), testutil.ToFloat64(ungrouped))
	}
}
