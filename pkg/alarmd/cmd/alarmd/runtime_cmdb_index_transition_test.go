// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cmdbcache"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A CMDB index going stale is one WARN line naming the tenant, the
// transition, the published age, the bound in force and the gap it was
// derived from; coming back is another.
func TestACMDBIndexTransitionIsOneWarnLine(t *testing.T) {
	var out bytes.Buffer
	line := cmdbIndexTransitionLine(observability.New("worker", &out))
	line(cmdbcache.Transition{Tenant: "tenant-a", From: "", To: cmdbcache.IndexStale,
		PublishedAge: 15*time.Minute + 17*time.Second, Bound: 15 * time.Minute, MaxGap: 5 * time.Minute})
	line(cmdbcache.Transition{Tenant: "tenant-a", From: cmdbcache.IndexStale, To: "",
		PublishedAge: 30 * time.Second, Bound: 33 * time.Minute, MaxGap: 16*time.Minute + 32*time.Second})
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("wrote %d lines, want one per transition:\n%s", len(lines), out.String())
	}
	for _, want := range []string{`"cmdb_index_transition"`, `"bk_tenant_id":"tenant-a"`, `"from":"usable"`, `"to":"index_stale"`,
		`"published_age_seconds":917`, `"stale_bound_seconds":900`, `"publish_max_gap_seconds":300`} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("the line into stale lacks %s: %s", want, lines[0])
		}
	}
	if !strings.Contains(lines[0], `"WARN"`) || !strings.Contains(lines[1], `"from":"index_stale"`) || !strings.Contains(lines[1], `"to":"usable"`) {
		t.Errorf("lines %q, want WARN into stale and a line out of it", lines)
	}
}
