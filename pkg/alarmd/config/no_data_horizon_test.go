// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package config

import (
	"strings"
	"testing"
)

// The deployment's no-data horizon is read from the key an operator writes.
//
// The key name is the whole interface. A setting that parses into a field
// nothing reads, or that is read from a name the operator did not write, is
// indistinguishable from one left at its default - and this default means
// "track absence indefinitely", so the mistake looks exactly like a deployment
// that chose not to set it.
func TestTheNoDataHorizonIsReadFromTheKeyAnOperatorWrites(t *testing.T) {
	loaded, err := Load(writeConfig(t, platformSettingsConfigContents("",
		platformSettingsControlBase+"  no_data:\n    tracking_horizon_seconds: 600\n")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := loaded.NoDataTrackingHorizonSeconds(); got != 600 {
		t.Fatalf("NoDataTrackingHorizonSeconds() = %d, want 600 from phase_two.no_data.tracking_horizon_seconds", got)
	}

	// The other answer, and it is the one that ships: a deployment saying
	// nothing keeps tracking absence indefinitely. Without this the case above
	// passes on a parser that returns 600 for anything, and the default the
	// feature is off by would go unasserted.
	silent, err := Load(writeConfig(t, platformSettingsConfigContents("", platformSettingsControlBase)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := silent.NoDataTrackingHorizonSeconds(); got != 0 {
		t.Fatalf("a config stating no horizon reads as %d, want 0: absence stays tracked until someone "+
			"decides otherwise, because a horizon stops no-data alerts once it passes", got)
	}
}

// A negative horizon is refused where the operator can still read it.
//
// The contract refuses it too, but by then it is inside a compiled Plan and
// the same typo is a refused strategy rather than a refused deployment - one
// strategy quietly not detecting, instead of a process that will not start.
func TestANegativeNoDataHorizonIsRefusedAtLoad(t *testing.T) {
	_, err := Load(writeConfig(t, platformSettingsConfigContents("",
		platformSettingsControlBase+"  no_data:\n    tracking_horizon_seconds: -1\n")))
	if err == nil {
		t.Fatal("Load() accepted a negative horizon")
	}
	if !strings.Contains(err.Error(), "tracking_horizon_seconds") {
		t.Fatalf("Load() error = %v, want it to name the key the operator has to fix", err)
	}
}
