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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
)

// A control process's trends read counts the rows it lets go, and how its
// request's end was asked, on the process's counters, as a worker's queries
// do: the trends' own client is wired to the same observers. A PromQL range
// asks as its window ends.
func TestAControlProcesssTrendsCountTheRowsTheyLetGo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"series":[{"columns":["_time","_value"],"types":["float","float"],` +
			`"values":[[1789006260000,931],[1789006320000]]}],"is_partial":false}`))
	}))
	defer server.Close()
	cfg := validGoAccessRuntimeConfig()
	cfg.PhaseTwo.Access.UQEndpoint = server.URL
	recorder := metric.NewRecorder(metric.BuildInfo{})
	now := func() time.Time { return time.Unix(1789006380, 0) }
	trends, err := controlTrends(cfg, phaseTwoProductionExternalDependencies{Now: now, HTTPClient: server.Client()}, recorder)
	if err != nil || trends == nil {
		t.Fatalf("controlTrends() = %v, %v", trends, err)
	}
	end := now()
	if _, err := trends.Range.Range(context.Background(), "bkcc__2", "max(alarmd_fleet_objects)", end.Add(-time.Hour), end, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, recorder, "alarmd_access_query_points_dropped_total", map[string]string{"reason": "row_missing_columns"}); got != 1 {
		t.Fatalf("row_missing_columns = %v, want the one row let go", got)
	}
	if got := counterValue(t, recorder, "alarmd_access_query_request_end_total", map[string]string{"outcome": "source_not_eligible"}); got != 1 {
		t.Fatalf("request_end{source_not_eligible} = %v, want the one range request", got)
	}
}
