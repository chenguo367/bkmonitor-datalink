// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
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
			for _, operation := range []string{"pod.targets", "pod.get", "pod.logs", "pod.exec", "uq.egresses", "uq.query", "uq.trace"} {
				present := runtime.Executor.OperationContractRevision(operation) != ""
				if present != cfg.HasRole(roles.Channel) {
					t.Fatalf("%s registered in roles %v: %v", operation, selected, present)
				}
			}
		})
	}
}

func TestSREQueryReusesCurrentUQEndpointAndSource(t *testing.T) {
	var calls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/prefix/query/ts/" || r.Header.Get("Bk-Query-Source") != config.DefaultQuerySource || r.Header.Get("X-Bk-Tenant-Id") != "tenant-fixture" || r.Header.Get("X-Bk-Scope-Space-Uid") != "scope-fixture" || r.Header.Get("X-Bkapi-Jwt") != "" {
			t.Errorf("SRE query diverged from the current native UQ connection: %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"series":[]}`)
	}))
	defer backend.Close()
	cfg := config.Default()
	cfg.PhaseTwo.Access.UQEndpoint = backend.URL + "/prefix"
	cfg.PhaseTwo.Access.QuerySource = ""
	cfg.SRE.UQ.Egresses = []uq.Egress{{Name: "current", Tenants: []string{"tenant-fixture"}, Spaces: []string{"scope-fixture"}, Endpoints: []uq.Endpoint{{Path: "/query/ts/", StartField: "start_time", EndField: "end_time", TimeUnit: "s"}}}}
	operations, closeProvider, err := channelSREOperations(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeProvider()
	for _, operation := range operations {
		if operation.ID != "uq.query" {
			continue
		}
		out := operation.Run(context.Background(), obchannel.Params{"egress": "current", "endpoint": "/query/ts/", "tenant": "tenant-fixture", "space": "scope-fixture", "body": map[string]any{"start_time": "1700000000", "end_time": "1700000060", "query_list": []any{map[string]any{"table_id": "fixture.metric", "field_name": "value"}}}})
		if out.Error != nil || !out.Complete || calls != 1 {
			t.Fatalf("current runtime UQ was not used: %+v; calls=%d", out, calls)
		}
		return
	}
	t.Fatal("UQ query operation is missing")
}
