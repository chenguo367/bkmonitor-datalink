// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package config

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/uq"
)

func TestSREInheritsRuntimeUQOnlyForExplicitScopes(t *testing.T) {
	first := uq.Egress{Name: "current", Tenants: []string{"tenant-fixture"}, Spaces: []string{"scope-fixture"}, Endpoints: []uq.Endpoint{{Path: "/query/ts/raw", StartField: "start_time", EndField: "end_time", TimeUnit: "s"}}}
	second := first
	second.Name, second.URL = "other", "http://other.example.test/prefix"
	original := SREConfig{UQ: SREUQConfig{Egresses: []uq.Egress{first, second}}}
	effective := original.WithUQEndpoint("http://current.example.test")
	if err := effective.Validate(); err != nil {
		t.Fatal(err)
	}
	if effective.UQ.Egresses[0].URL != "http://current.example.test" || effective.UQ.Egresses[1].URL != second.URL || original.UQ.Egresses[0].URL != "" {
		t.Fatal("current endpoint inheritance changed explicit routing or the original configuration")
	}
	if err := original.WithUQEndpoint("").Validate(); err == nil {
		t.Fatal("accepted a scope without either an explicit or current UQ endpoint")
	}
	if scopes := (SREConfig{}).WithUQEndpoint("http://current.example.test").UQ.Egresses; len(scopes) != 0 {
		t.Fatal("endpoint inheritance implicitly granted an SRE query scope")
	}
	cfg := Default()
	cfg.Roles = roles.Set{roles.Channel}
	cfg.Redis.Address = "runtime.example.test:6379"
	cfg.PhaseTwo.Worker.ID = "channel-fixture"
	cfg.PhaseTwo.Access.UQEndpoint = "http://current.example.test"
	cfg.SRE = original
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configuration validation did not resolve the current endpoint: %v", err)
	}
	if cfg.SRE.UQ.Egresses[0].URL != "" {
		t.Fatal("validation mutated the declared configuration")
	}
}
