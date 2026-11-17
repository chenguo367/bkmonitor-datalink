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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
)

// The Progress store production opens counts its reads and its supersedes on
// the process's recorder. Both families have every cell from the start, so a
// hook left unset reads on /metrics as zeros - as if the owner never read its
// record and no projection were ever replaced - and only this tells the two
// apart.
func TestTheProductionProgressStoreCountsItsReadsAndSupersedesOnTheRecorder(t *testing.T) {
	recorder := metric.NewRecorder(metric.BuildInfo{})
	options := productionProgressStoreOptions(config.Config{}, nil, nil, time.Now, nil, recorder)
	if options.Reads == nil || options.Superseded == nil {
		t.Fatalf("production Progress options: Reads set %v, Superseded set %v, want both", options.Reads != nil, options.Superseded != nil)
	}
	options.Reads(progress.ProgressReadNext, progress.ProgressReadCache)
	options.Superseded(progress.SupersededDeadline)
	families, err := recorder.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	counted := map[string]float64{}
	for _, family := range families {
		for _, sample := range family.GetMetric() {
			labels := ""
			for _, label := range sample.GetLabel() {
				labels += label.GetName() + "=" + label.GetValue() + ","
			}
			counted[family.GetName()+"{"+labels+"}"] += sample.GetCounter().GetValue()
		}
	}
	for _, cell := range []struct {
		name string
		want float64
	}{
		{"alarmd_progress_reads_total{caller=next,source=cache,}", 1},
		{"alarmd_progress_reads_total{caller=begin,source=redis,}", 0},
		{"alarmd_progress_projection_superseded_total{field=deadline,}", 1},
		{"alarmd_progress_projection_superseded_total{field=keep_until,}", 0},
		{"alarmd_progress_projection_superseded_total{field=due_plan_targets,}", 0},
	} {
		got, ok := counted[cell.name]
		if !ok || got != cell.want {
			t.Errorf("%s = %v (present %v), want %v", cell.name, got, ok, cell.want)
		}
	}
}
