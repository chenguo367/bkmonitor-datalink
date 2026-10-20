// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"encoding/json"
	"testing"
)

// A record whose host CMDB placed by bk_host_id or bk_agent_id carries the
// dimensions Python's fuller writes (fullers.py:57-74) - the host's address,
// its cloud ("0" for none) and its id - exactly when its own address or
// cloud is not the host's; a record that already names its host's address
// carries nothing, so the series an ordinary deployment sends cost nothing.
func TestARecordPlacedByIDCarriesTheHostsAddressOnlyWhereItDiffers(t *testing.T) {
	store := agentStore(map[string]string{"agent-live": "720002"})
	chain := instanceChain(t, store)
	written := map[string]string{"bk_target_ip": `"192.0.2.172"`, "bk_target_cloud_id": `"0"`, "bk_host_id": `720002`}
	for _, c := range []struct {
		name    string
		record  string
		overlay bool
	}{
		{"by id, the host's address", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172","bk_target_cloud_id":"0"}`, false},
		{"by id, the host's address and cloud as a number", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172","bk_target_cloud_id":0}`, false},
		{"by id, the host's address and no cloud", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172"}`, false},
		{"by id, the host's address escaped", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172"}`, false},
		{"by id, another address", `{"bk_host_id":"720002","bk_target_ip":"198.51.100.9","bk_target_cloud_id":"0"}`, true},
		{"by id, no address", `{"bk_host_id":"720002"}`, true},
		{"by id, the host's address in another cloud", `{"bk_host_id":"720002","bk_target_ip":"192.0.2.172","bk_target_cloud_id":"3"}`, true},
		{"by agent, another address", `{"bk_agent_id":"agent-live","bk_target_ip":"198.51.100.9"}`, true},
		{"by agent, no address", `{"bk_agent_id":"agent-live"}`, true},
		{"by agent, the host's address", `{"bk_agent_id":"agent-live","bk_target_ip":"192.0.2.172"}`, false},
		// Placed by its address: Python's address branch does not write
		// bk_target_ip, so neither is it overlaid here.
		{"by address", `{"bk_target_ip":"192.0.2.172","bk_target_cloud_id":0}`, false},
		{"an id CMDB does not know", `{"bk_host_id":"720999","bk_target_ip":"198.51.100.9"}`, false},
	} {
		facts := chain.Enrich(jsonDims(t, c.record))
		if (facts.HostOverlay != nil) != c.overlay {
			t.Errorf("%s: overlay %v, want one: %t", c.name, facts.HostOverlay, c.overlay)
			continue
		}
		if !c.overlay {
			continue
		}
		if len(facts.HostOverlay) != len(written) {
			t.Errorf("%s: overlay %v, want %v", c.name, facts.HostOverlay, written)
		}
		for key, want := range written {
			if got := string(facts.HostOverlay[key]); got != want {
				t.Errorf("%s: %s = %s, want %s", c.name, key, got, want)
			}
		}
		var decoded map[string]any
		encoded, _ := json.Marshal(facts.HostOverlay)
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Errorf("%s: overlay does not encode: %v", c.name, err)
		}
	}
}
