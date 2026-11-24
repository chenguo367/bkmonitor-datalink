// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A round a case asks for on a view that never settles - the Worker's control
// stream withheld, so the view has nothing to install from - fails saying the
// view did not settle, how long it was waited for and what it lacked, and
// does not run; by hand through the runner a fixture holds and by the
// scheduled pass alike. Before, each waited out its bound and ran the round
// on the unsettled view, and a case reading it failed, if at all, on an
// assertion about something else: whether the view had settled was left to
// be inferred from how long the case took.
func TestARoundOnAViewThatNeverSettlesFailsSayingSoAndDoesNotRun(t *testing.T) {
	fixture := startRoundsFixture(t, roundsOptions{hosts: true})
	withholdControlStreamForTest(fixture.bundle)
	unsettled := func(how string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s on a view that never settles: no error, want the view not settled said", how)
		}
		for _, said := range []string{"view not settled", "waited", string(fixture.queryGroup)} {
			if !strings.Contains(err.Error(), said) {
				t.Fatalf("%s: %q does not say %q", how, err, said)
			}
		}
	}
	ran := func(mark int) bool {
		for _, observation := range fixture.since(mark) {
			if observation.Stage == observability.StageSlotStarted || observation.Stage == observability.StageSlotCompleted {
				return true
			}
		}
		return false
	}

	mark := fixture.mark()
	fixture.clock.Store(roundReadAtMillis(fixture.bundle.dependencies.Config, fixture.queryGroup, fixture.evaluationAt(1), fixture.interval))
	_, attempted, err := fixture.runner.RunOne(context.Background())
	unsettled("the runner's round", err)
	if attempted || ran(mark) {
		t.Fatalf("the runner's round ran on the unsettled view (attempted %t)", attempted)
	}

	mark = fixture.mark()
	unsettled("the scheduled pass", runScheduledOnceSettled(context.Background(), fixture.bundle))
	if ran(mark) {
		t.Fatal("the scheduled pass ran a round on the unsettled view")
	}
}
