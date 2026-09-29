// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// The list is only as good as its reach. One key from each way it finds
// them - a field of a nested struct, a field of a value decoded later, a key
// of a decoded key list, a key read beside the structs - must be on it; a
// walk that stopped short would hold the source view to less than alarmd
// reads, and the view's own test would pass on the shorter list.
func TestLegacySourceReadPathsReachEveryWayAKeyIsRead(t *testing.T) {
	listed := map[string]bool{}
	for _, path := range controlplane.LegacySourceReadPaths() {
		listed[path] = true
	}
	for _, want := range []string{
		"items.algorithms.type",                          // a nested struct's field
		"detects.trigger_config.check_window",            // a nested struct inside a slice
		"items.query_configs.filter_dict",                // a raw value decoded into a struct
		"items.query_configs.agg_condition.key",          // a struct nested in one decoded later
		"items.target.value.bk_target_ip",                // a value decoded by a custom unmarshal
		"detects.trigger_config.uptime.active_calendars", // a key list of a value kept raw
		"items.algorithms.config.fetch_type",             // the comparison parameters' key list
		"bk_tenant_id",                                   // read beside the structs
	} {
		if !listed[want] {
			t.Errorf("%s is not listed; the list stops short of a way a key is read", want)
		}
	}
}
