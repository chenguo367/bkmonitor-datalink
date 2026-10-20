// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package readhold

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A group held at its limit and then lowered by trials, with no arrival read
// since, is no longer at its limit: the arrival that wanted more than the
// limit is still on the record, and the hold is not.
func TestAHoldLoweredBelowItsLimitIsNotHeldAtIt(t *testing.T) {
	c, _, now := controllerFixture(t)
	spec := groupSpec("qg", 0)
	spec.HoldLimit = 10 * time.Minute
	prepare(t, c, spec)
	observeEarly(t, c, "qg", 15*time.Minute+spec.SettlingWait, 0)
	ctx := context.Background()
	first := execution.EvaluationTime(120)
	if _, err := c.SlotReadHold(ctx, scheduleFor(t, "qg", 60, nil), first, holdFence("qg")); err != nil {
		t.Fatal(err)
	}
	if record, _ := c.Reading("qg"); !record.AtLimit || !record.HeldAtLimit() || c.ReadHold("qg") != 10*time.Minute {
		t.Fatalf("fixture: record %+v hold %v, want held at the 10 minute limit", record, c.ReadHold("qg"))
	}
	*now = now.Add(time.Hour)
	for index := range EarlierMatchesRequired {
		if err := c.EarlierRead(ctx, EarlierEvidence{Contract: holdContract("qg", first+execution.EvaluationTime(index), 600_000),
			CandidateHold: 5 * time.Minute, Observed: true, Equal: true}); err != nil {
			t.Fatal(err)
		}
	}
	record, _ := c.Reading("qg")
	if c.ReadHold("qg") != 5*time.Minute {
		t.Fatalf("fixture: lowered to %v, want 5 minutes", c.ReadHold("qg"))
	}
	if !record.AtLimit {
		t.Fatal("fixture: the lowering rewrote the arrival reading; this test no longer covers a stale flag")
	}
	if record.HeldAtLimit() {
		t.Fatalf("a hold lowered to 5 minutes under a 10 minute limit reads held at its limit: %+v", record)
	}
}

// A group seeded past its limit starts held at the limit, and one seeded
// under it does not.
func TestASeedPastTheLimitStartsHeldAtIt(t *testing.T) {
	for _, test := range []struct {
		name  string
		seed  time.Duration
		limit time.Duration
		want  bool
	}{
		{"past the limit", 90 * time.Second, 30 * time.Second, true},
		{"under the limit", 30 * time.Second, 60 * time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _, _ := controllerFixture(t)
			spec := groupSpec("qg", 0)
			spec.SettlingWait = 10 * time.Second
			spec.HoldLimit = test.limit
			spec.SeedArrivalAge, spec.SeedOrigin = test.seed, SeedColdStart
			prepare(t, c, spec)
			if _, err := c.SlotReadHold(context.Background(), scheduleFor(t, "qg", 60, nil), 120, holdFence("qg")); err != nil {
				t.Fatal(err)
			}
			record, _ := c.Reading("qg")
			if record.HeldAtLimit() != test.want {
				t.Fatalf("seeded %v under limit %v: held at limit %t, want %t (record %+v)", test.seed, test.limit,
					record.HeldAtLimit(), test.want, record)
			}
		})
	}
}
