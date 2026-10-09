// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// What the sink takes, round by round, for the no-data trigger's sequences,
// through the production bundle on the native protocol: the strategy compiled
// from its document, the open-alert gate wired as in production, real Redis.
//
// The expectations are written out per round from the definitions, not read
// from a run. The alert is raised by the continuous-th consecutive absent
// point, the points one period apart; a round whose query was not FULL
// contributes no point and bridges nothing (P9). One round with data closes
// an open alert (P11, one recovery window), and only an open one: a RECOVERY
// reaches the consumer only for an alert it holds (the open-alert gate). A
// new absence after a close starts from nothing. A round not FULL is neither
// an absent point nor a present one: it neither raises nor closes, so an
// open alert stays open across it until data actually returns.
func TestNoDataSequencesSendWhatTheTriggerDefines(t *testing.T) {
	const a, r = contract.TriggerEventAbnormal, contract.TriggerEventRecovery
	for _, test := range []struct {
		name       string
		continuous int
		rounds     string // A absent, P present, U query not FULL
		want       []string
	}{
		{"continuous 1 raises on the first absent round", 1, "A", []string{a}},
		{"continuous 2: one short, then raised", 2, "AA", []string{"", a}},
		{"continuous 3 from a clean start: two short, then raised", 3, "AAA", []string{"", "", a}},
		{"a round not FULL after two absent ones restarts the count", 3, "AAUAAA", []string{"", "", "", "", "", a}},
		{"a round with data between absent ones restarts the count and closes nothing", 3, "APAAA", []string{"", "", "", "", a}},
		{"data closes the alert once, and a new absence starts from nothing", 3, "AAAPAAA", []string{"", "", a, r, "", "", a}},
		{"a round not FULL does not close an open alert, and data after it does", 3, "AAAUP", []string{"", "", a, "", r}},
		// The window after the hole holds two absent points of three, short
		// of the trigger; the hole is not a miss, so it closes nothing.
		{"an absent round after a round not FULL does not close the open alert", 3, "AAAUA", []string{"", "", a, "", ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := startNoDataFixtureWith(t, config.OutputProtocolNative, test.continuous)
			ctx := context.Background()
			seen := 0
			for index, round := range test.rounds {
				fixture.hasData.Store(round == 'P')
				fixture.partial.Store(round == 'U')
				// runSlot(n) puts the clock half a period past base+n*P, so
				// the Slot evaluated at base+n*P is the one due: n from 0
				// runs one Slot per round.
				fixture.runSlot(ctx, int64(index))
				var kinds []string
				recorded := fixture.events.recorded()
				for _, event := range recorded[seen:] {
					if noDataTagged(event) {
						kinds = append(kinds, event.EventKind)
					}
				}
				seen = len(recorded)
				want := test.want[index]
				if (want == "" && len(kinds) != 0) || (want != "" && (len(kinds) != 1 || kinds[0] != want)) {
					t.Errorf("round %d (%c) sent %v, want %q", index+1, round, kinds, want)
				}
			}
		})
	}
}
