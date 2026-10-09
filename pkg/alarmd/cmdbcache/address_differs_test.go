// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import "testing"

// Python's fuller overwrites bk_target_ip with the CMDB host's address
// whenever it finds the host by bk_host_id or bk_agent_id (bk-monitor
// c0e828fa8c, fullers.py:57-74), and the alert's target is read from it
// afterwards. This side writes nothing back, so a record placed that way
// whose own bk_target_ip is not the host's - another address, empty, or
// missing - is one whose alert identity can differ from Python's. The fact
// is marked for exactly those records; a record placed by its address, or
// not placed at all, is not one Python rewrites.
func TestARecordPlacedByIDWhoseAddressIsNotTheHostsIsMarked(t *testing.T) {
	store := agentStore(map[string]string{"agent-live": "720002"})
	chain := instanceChain(t, store)
	for _, c := range []struct {
		name   string
		record string
		marked bool
	}{
		{"by id, the host's address", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172"}`, false},
		{"by id, another address", `{"bk_host_id":"720002","bk_target_ip":"198.51.100.9"}`, true},
		{"by id, an empty address", `{"bk_host_id":"720002","bk_target_ip":""}`, true},
		{"by id, no address", `{"bk_host_id":"720002"}`, true},
		// Not a string at all: Python writes CMDB's string over it.
		{"by id, an address that is not a string", `{"bk_host_id":"720002","bk_target_ip":192}`, true},
		{"by agent, another address", `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`, true},
		{"by agent, the host's address", `{"bk_agent_id":"agent-live","bk_target_ip":"192.0.2.172"}`, false},
		{"by address", `{"bk_target_ip":"192.0.2.172","bk_target_cloud_id":0}`, false},
		{"an id CMDB does not know", `{"bk_host_id":"720999","bk_target_ip":"198.51.100.9"}`, false},
	} {
		facts := chain.Enrich(jsonDims(t, c.record))
		if facts.ReportedAddressDiffers != c.marked {
			t.Errorf("%s: marked %t, want %t", c.name, facts.ReportedAddressDiffers, c.marked)
		}
	}
}
