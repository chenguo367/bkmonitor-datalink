// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package strategy

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// A recovery written to end closed carries its end into the compiled plan and
// its canonical bytes; one written to recover compiles to exactly the bytes it
// had before the field existed, so its trigger fingerprint does not move; an
// end outside the closed list is refused; a disabled recovery has no end.
func TestARecoveryPlanCarriesItsEndAndOneWrittenToRecoverKeepsItsBytes(t *testing.T) {
	typed := func(config string) contract.TypedPlanV1 {
		return contract.TypedPlanV1{Type: RecoveryPlanTypeContinuousTriggerMiss, Version: 1, Config: []byte(config)}
	}
	for _, test := range []struct {
		config, wantCanonical, wantEnd string
	}{
		{`{"enabled":true,"consecutive_windows":4}`, `{"consecutive_windows":4,"enabled":true}`, ""},
		{`{"enabled":true,"consecutive_windows":4,"end":"close"}`, `{"consecutive_windows":4,"enabled":true,"end":"close"}`, contract.RecoveryEndClose},
		{`{"enabled":false,"consecutive_windows":4,"end":"close"}`, `{"consecutive_windows":0,"enabled":false}`, ""},
	} {
		plan, canonical, err := compileRecoveryPlan(typed(test.config))
		if err != nil {
			t.Fatalf("%s: compileRecoveryPlan error = %v", test.config, err)
		}
		if string(canonical.Config) != test.wantCanonical || plan.End != test.wantEnd {
			t.Fatalf("%s: canonical %s end %q, want %s and %q", test.config, canonical.Config, plan.End, test.wantCanonical, test.wantEnd)
		}
	}
	for _, refused := range []string{`{"enabled":true,"consecutive_windows":4,"end":"resolve"}`, `{"enabled":true,"consecutive_windows":4,"end":"recovery"}`} {
		if _, _, err := compileRecoveryPlan(typed(refused)); err == nil {
			t.Fatalf("%s: compiled, want an end outside the closed list refused", refused)
		}
	}
}
