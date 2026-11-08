// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"reflect"
	"testing"
	"time"
)

// What the renewals remember is bounded by the timelines still referenced: a
// key that leaves the catalog is forgotten, and one that comes back is due
// as one this process never renewed.
func TestCatalogRenewalsForgetATimelineNoLongerReferenced(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	renewals := &catalogRenewals{clock: func() time.Time { return now }}
	renewals.renewed([]string{"a", "b"}, now)
	if due, first, _ := renewals.timelinesDue([]string{"a"}, time.Hour); len(due) != 0 || first != 0 {
		t.Fatalf("a timeline renewed just now is due: %v, first %d", due, first)
	}
	if len(renewals.timelines) != 1 {
		t.Fatalf("remembered %v after b left the catalog, want only a", renewals.timelines)
	}
	if due, first, _ := renewals.timelinesDue([]string{"a", "b"}, time.Hour); !reflect.DeepEqual(due, []string{"b"}) || first != 1 {
		t.Fatalf("a timeline back in the catalog: due %v, first %d; want b, first", due, first)
	}
}
