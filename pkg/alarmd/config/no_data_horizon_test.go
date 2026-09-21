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
	got, stated := loaded.NoDataTrackingHorizonSeconds()
	if !stated || got != 600 {
		t.Fatalf("NoDataTrackingHorizonSeconds() = (%d, %t), want (600, true) from "+
			"phase_two.no_data.tracking_horizon_seconds", got, stated)
	}

	// The other answer, and it is the one that ships: a deployment saying
	// nothing keeps tracking absence indefinitely. Without this the case above
	// passes on a parser that returns 600 for anything, and the default the
	// feature is off by would go unasserted.
	//
	// It reports absence rather than a zero because the two are different
	// facts and only one of them is writable: this deployment set no horizon,
	// which is not the same as having written one down as nothing.
	silent, err := Load(writeConfig(t, platformSettingsConfigContents("", platformSettingsControlBase)))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, stated := silent.NoDataTrackingHorizonSeconds(); stated || got != 0 {
		t.Fatalf("a config stating no horizon reads as (%d, %t), want (0, false): absence stays tracked "+
			"until someone decides otherwise, because a horizon stops no-data alerts once it passes",
			got, stated)
	}
}

// A horizon that is not a positive number of seconds is refused where the
// operator can still read it.
//
// Zero is refused for the same reason as a negative, and this is the half that
// is easy to get wrong: a deployment with no horizon says so by leaving the
// key out, so a written zero is not that statement. Accepting it would give
// the value a second meaning nobody wrote - the shape that let this feature
// look configured for three batches while never running.
//
// The contract refuses these too, but by then they are inside a compiled Plan
// and the same typo is a refused strategy rather than a refused deployment:
// one strategy quietly not detecting, instead of a process that will not
// start where someone is watching.
func TestANoDataHorizonThatIsNotPositiveIsRefusedAtLoad(t *testing.T) {
	for name, stated := range map[string]string{
		"a written zero is not how a deployment says it has no horizon": "0",
		"a negative horizon is not a length of time":                    "-1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, platformSettingsConfigContents("",
				platformSettingsControlBase+"  no_data:\n    tracking_horizon_seconds: "+stated+"\n")))
			if err == nil {
				t.Fatalf("Load() accepted %s as a horizon", stated)
			}
			if !strings.Contains(err.Error(), "tracking_horizon_seconds") {
				t.Fatalf("Load() error = %v, want it to name the key the operator has to fix", err)
			}
		})
	}
}
