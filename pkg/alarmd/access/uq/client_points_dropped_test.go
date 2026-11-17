// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A point the provider returned outside the range the query accepts is let
// go and counted by the side it fell on - before the range, or at or after
// its end - on the completion and to the client's observer; a series none of
// whose points was inside is handed on as nothing, and counted all the same.
func TestPointsOutsideTheAcceptedRangeAreCountedBySide(t *testing.T) {
	// The attempt accepts [1_700_123_000, 1_700_124_000).
	kept := `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],"group_values":["127.0.0.1"],` +
		`"values":[[1700122999000,1],[1700123456789,12.5],[1700124000000,2],[1700124060000,3]]}`
	gone := `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],"group_values":["127.0.0.2"],` +
		`"values":[[1700122000000,4],[1700122940000,5]]}`
	client := fixtureClient(t, http.StatusOK, `{"series":[`+kept+`,`+gone+`],"is_partial":false}`, DefaultLimits())
	client.now = func() time.Time { return time.Unix(1_700_123_500, 0) }
	var heard []observability.PointDrops
	client.WithPointsDroppedObserver(func(dropped observability.PointDrops) { heard = append(heard, dropped) })
	sink := &collectingSink{}
	completion, err := client.Execute(context.Background(), validAttempt(t), sink)
	if err != nil {
		t.Fatal(err)
	}
	want := observability.PointDrops{OutsideRangeBefore: 3, OutsideRangeAfter: 2}
	if completion.Stats.PointsDropped != want {
		t.Fatalf("dropped = %+v, want %+v", completion.Stats.PointsDropped, want)
	}
	if completion.Stats.Records != 1 || len(sink.batches) != 1 || completion.DataState != execution.DataStateData {
		t.Fatalf("completion = %+v with %d batches, want the one point inside handed on", completion, len(sink.batches))
	}
	if len(heard) != 1 || heard[0] != want {
		t.Fatalf("observer heard %+v, want %+v once", heard, want)
	}

	// An answer with every point inside lets nothing go and says nothing.
	inside := fixtureClient(t, http.StatusOK, `{"series":[`+identityTestSeriesJSON()+`],"is_partial":false}`, DefaultLimits())
	inside.now = client.now
	heard = nil
	inside.WithPointsDroppedObserver(func(dropped observability.PointDrops) { heard = append(heard, dropped) })
	completion, err = inside.Execute(context.Background(), validAttempt(t), &collectingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if completion.Stats.PointsDropped != (observability.PointDrops{}) || len(heard) != 0 {
		t.Fatalf("dropped %+v, heard %+v; want none", completion.Stats.PointsDropped, heard)
	}
}

func identityTestSeriesJSON() string {
	return `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],"group_values":["127.0.0.1"],"values":[[1700123456789,12.5]]}`
}

// A range read's row without the time or the value column is let go and
// counted, not read as a quiet window.
func TestARangeRowMissingAColumnIsCounted(t *testing.T) {
	client, _ := rangeClient(t, func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"series":[{"columns":["_time","_value"],"types":["float","float"],
			"values":[[1789006260000,931],[1789006320000]]}],"is_partial":false}`))
	})
	var heard []observability.PointDrops
	client.WithPointsDroppedObserver(func(dropped observability.PointDrops) { heard = append(heard, dropped) })
	result, err := client.Range(context.Background(), rangeRequest())
	if err != nil {
		t.Fatal(err)
	}
	want := observability.PointDrops{RowMissingColumns: 1}
	if len(result.Points) != 1 || result.Dropped != want || len(heard) != 1 || heard[0] != want {
		t.Fatalf("points %+v, dropped %+v, heard %+v; want one kept and one row counted", result.Points, result.Dropped, heard)
	}
}
