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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// With no placement injected - production's - the Leader places on what it
// always placed on: every worker of the expected compatibility, the same
// eligibility value it built before the seam existed.
func TestNoInjectedPlacementIsTheCompatibilityAlone(t *testing.T) {
	expected, err := phaseTwoExpectedWorkerCompatibility(config.Default())
	if err != nil {
		t.Fatal(err)
	}
	got, err := phaseTwoWorkerEligibility(expected, nil)
	if err != nil {
		t.Fatal(err)
	}
	want, err := scheduler.NewStaticWorkerEligibility(expected)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("eligibility with no placement = %#v, want the compatibility alone %#v", got, want)
	}
}
