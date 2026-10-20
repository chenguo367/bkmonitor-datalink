// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The alert reads the record's dimensions with the host overlay written
// over them, and so its fingerprint follows CMDB's address as the
// platform's does; without an overlay it reads the record's own map,
// unchanged and not copied. The record itself is never changed.
func TestTheAlertReadsTheHostOverlayOverTheRecordsDimensions(t *testing.T) {
	record := map[string]json.RawMessage{"bk_target_ip": json.RawMessage(`"198.51.100.9"`),
		"bk_target_cloud_id": json.RawMessage(`"0"`), "bk_agent_id": json.RawMessage(`"agent-live"`)}
	if got := alertDimensions(record, nil); len(got) != len(record) || string(got["bk_target_ip"]) != `"198.51.100.9"` {
		t.Fatalf("without an overlay the alert read %v", got)
	}
	overlay := map[string]json.RawMessage{"bk_target_ip": json.RawMessage(`"192.0.2.172"`), "bk_target_cloud_id": json.RawMessage(`"0"`),
		"bk_host_id": json.RawMessage(`720002`)}
	merged := alertDimensions(record, overlay)
	if string(merged["bk_target_ip"]) != `"192.0.2.172"` || string(merged["bk_host_id"]) != `720002` || string(merged["bk_agent_id"]) != `"agent-live"` {
		t.Fatalf("merged %v", merged)
	}
	if string(record["bk_target_ip"]) != `"198.51.100.9"` || len(record) != 3 {
		t.Fatalf("the record's own dimensions changed: %v", record)
	}
	identity := contract.MonitorOutputIdentity{DimensionFields: []string{"bk_target_ip", "bk_target_cloud_id", "bk_agent_id"}}
	own, err := contract.MonitorDedupeMD5("101", "2", record, identity)
	if err != nil {
		t.Fatal(err)
	}
	written, err := contract.MonitorDedupeMD5("101", "2", merged, identity)
	if err != nil {
		t.Fatal(err)
	}
	if own == written {
		t.Fatal("the overlay did not move the fingerprint to the host's address")
	}
}
