// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

func TestSREProvidersAreRegisteredOnlyByTheChannelRole(t *testing.T) {
	address, _ := startPhaseTwoRedis(t)
	for _, selected := range []roles.Set{nil, {roles.Control, roles.Worker, roles.Channel}, {roles.Control, roles.Channel}, {roles.Channel}, {roles.Control}, {roles.Worker}} {
		t.Run(selected.String(), func(t *testing.T) {
			cfg := roleChannelAuthConfig(address)
			cfg.Roles = selected
			runtime, err := openProductionRoleChannel(cfg, ChannelBinding{})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			for _, operation := range []string{"pod.targets", "pod.get", "pod.logs", "pod.exec", "uq.egresses", "uq.query"} {
				present := runtime.Executor.OperationContractRevision(operation) != ""
				if present != cfg.HasRole(roles.Channel) {
					t.Fatalf("%s registered in roles %v: %v", operation, selected, present)
				}
			}
		})
	}
}
