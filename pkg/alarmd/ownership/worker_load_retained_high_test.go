// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import (
	"encoding/json"
	"testing"
)

// previousWorkerLoad is WorkerLoad as the previous release writes and reads
// it, before the retained high-water (014 C1 section 8).
type previousWorkerLoad struct {
	OwnedQueryGroups  int     `json:"owned_query_groups"`
	PermitsHeld       int     `json:"permits_held"`
	PermitBudget      int     `json:"permit_budget"`
	PermitSeconds     float64 `json:"permit_seconds"`
	Waiting           int     `json:"waiting"`
	MemoryUsedBytes   uint64  `json:"memory_used_bytes,omitempty"`
	MemoryLimitBytes  uint64  `json:"memory_limit_bytes,omitempty"`
	RetainedPoolBytes uint64  `json:"retained_pool_bytes,omitempty"`
}

// Across a rollout both releases read each other's registrations: the
// previous one's load decodes with no high-water - not reported, and the
// fleet is judged by the sum - a high-water of zero is a reading of zero,
// and the new load decodes under the previous shape without an error.
func TestTheRetainedHighWaterCrossesReleasesInBothDirections(t *testing.T) {
	old, err := json.Marshal(previousWorkerLoad{OwnedQueryGroups: 3, RetainedPoolBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	var load WorkerLoad
	if err := json.Unmarshal(old, &load); err != nil {
		t.Fatal(err)
	}
	if load.RetainedHigh != nil || load.RetainedShareBytes != 0 || load.RetainedPoolBytes != 1<<30 {
		t.Fatalf("previous load decoded as %+v, want no high-water and no share", load)
	}

	quiet, err := json.Marshal(WorkerLoad{RetainedPoolBytes: 1 << 30, RetainedHigh: &RetainedHighReading{WindowSeconds: 3600}, RetainedShareBytes: 1 << 28})
	if err != nil {
		t.Fatal(err)
	}
	var reread WorkerLoad
	if err := json.Unmarshal(quiet, &reread); err != nil {
		t.Fatal(err)
	}
	if reread.RetainedHigh == nil || reread.RetainedHigh.Bytes != 0 || reread.RetainedHigh.WindowSeconds != 3600 || reread.RetainedShareBytes != 1<<28 {
		t.Fatalf("a quiet hour decoded as %+v, want a reading of zero over the hour", reread)
	}

	var previous previousWorkerLoad
	if err := json.Unmarshal(quiet, &previous); err != nil {
		t.Fatalf("the previous release fails on the new load: %v", err)
	}
	if previous.RetainedPoolBytes != 1<<30 {
		t.Fatalf("the previous release read %+v", previous)
	}
	var registration WorkerRegistration
	if err := json.Unmarshal([]byte(`{"load":`+string(quiet)+`}`), &registration); err != nil || registration.Load == nil || registration.Load.RetainedHigh == nil {
		t.Fatalf("registration decoded as %+v (%v)", registration, err)
	}
	if err := (&WorkerLoad{RetainedHigh: &RetainedHighReading{WindowSeconds: -1}}).validate(); err == nil {
		t.Fatal("a negative window validated")
	}
}
