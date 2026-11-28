// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeinput_test

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeinput"
)

// vmLabelsFile is testdata/inner/vm_labels.json: the series VictoriaMetrics stored, through the production-shaped
// ingestion, for the same neutral raw messages the plugin fixtures were cleaned from.
type vmLabelsFile struct {
	Series map[string][]map[string]string `json:"series"`
}

func loadVMLabels(t *testing.T) vmLabelsFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "inner", "vm_labels.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file vmLabelsFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	return file
}

func labelSetText(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+labels[name])
	}
	return strings.Join(parts, ",")
}

func pointLabelText(point realtimeinput.Point) string {
	labels := map[string]string{}
	for _, label := range point.Labels {
		labels[label.Name] = label.Value
	}
	return labelSetText(labels)
}

// decodedRowsOf decodes every recorded message of a kind.
func decodedRowsOf(t *testing.T, kind string) []realtimeinput.Row {
	t.Helper()
	var rows []realtimeinput.Row
	for name, message := range loadExpect(t).Messages {
		if message.Kind != kind {
			continue
		}
		decoded, err := realtimeinput.DecodeInner(readMessage(t, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rows = append(rows, decoded...)
	}
	if len(rows) == 0 {
		t.Fatalf("no recorded %s rows", kind)
	}
	return rows
}

func TestPluginRowsProjectToTheSeriesVictoriaMetricsStored(t *testing.T) {
	vm := loadVMLabels(t)
	for _, kind := range []string{realtimeinput.SourcePluginFlat, realtimeinput.SourcePluginNested} {
		t.Run(kind, func(t *testing.T) {
			want := map[string]map[string]bool{}
			for _, series := range vm.Series[kind] {
				metric := strings.TrimSuffix(series["__name__"], "_value")
				labels := map[string]string{}
				for name, value := range series {
					if name != "__name__" {
						labels[name] = value
					}
				}
				if want[metric] == nil {
					want[metric] = map[string]bool{}
				}
				want[metric][labelSetText(labels)] = true
			}
			got := map[string]map[string]bool{}
			refused := map[string]string{}
			for _, row := range decodedRowsOf(t, kind) {
				points, err := realtimeinput.Project(kind, row)
				if err != nil {
					var bad *realtimeinput.BadPoint
					if !errors.As(err, &bad) {
						t.Fatalf("%s: %v", *row.Metric, err)
					}
					refused[*row.Metric] = bad.Reason
					continue
				}
				for _, point := range points {
					if got[point.Metric] == nil {
						got[point.Metric] = map[string]bool{}
					}
					got[point.Metric][pointLabelText(point)] = true
				}
			}
			for metric, reason := range refused {
				// A row the realtime path refuses is one the aggregated path stores nothing for, and the only such
				// row is a flat plugin row without a group.
				if len(want[metric]) != 0 || kind != realtimeinput.SourcePluginFlat || reason != realtimeinput.BadPointGroupInfoEmpty {
					t.Fatalf("%s refused (%s) while VictoriaMetrics stored %d series", metric, reason, len(want[metric]))
				}
			}
			for metric, sets := range want {
				if !sameSet(got[metric], sets) {
					t.Fatalf("%s series:\n got  %v\n want %v", metric, keys(got[metric]), keys(sets))
				}
			}
			for metric := range got {
				if want[metric] == nil {
					t.Fatalf("%s projected to series VictoriaMetrics did not store", metric)
				}
			}
		})
	}
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for key := range a {
		if !b[key] {
			return false
		}
	}
	return true
}

func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// K8s and custom report rows project to the series VictoriaMetrics stored for the same neutral raw messages, metric by
// metric among those the realtime rules carry (the rules pass only referenced metrics; the aggregated path stores every
// one): a numeric dimension by its JSON text, and a custom report entry's target as the target label.
func TestK8sAndCustomReportRowsProjectToTheSeriesVictoriaMetricsStored(t *testing.T) {
	vm := loadVMLabels(t)
	for _, kind := range []string{realtimeinput.SourceK8s, realtimeinput.SourceCustomReport} {
		t.Run(kind, func(t *testing.T) {
			want := map[string]map[string]bool{}
			for _, series := range vm.Series[kind] {
				metric := strings.TrimSuffix(series["__name__"], "_value")
				labels := map[string]string{}
				for name, value := range series {
					if name != "__name__" {
						labels[name] = value
					}
				}
				if want[metric] == nil {
					want[metric] = map[string]bool{}
				}
				want[metric][labelSetText(labels)] = true
			}
			got := map[string]map[string]bool{}
			for _, row := range decodedRowsOf(t, kind) {
				points, err := realtimeinput.Project(kind, row)
				if err != nil {
					t.Fatalf("%s %s: %v", kind, *row.Metric, err)
				}
				if len(points) != 1 {
					t.Fatalf("%s row gave %d series", kind, len(points))
				}
				point := points[0]
				if point.Metric != *row.Metric || point.Value != *row.Value || point.EventTimeMs != *row.EventTimeMs {
					t.Fatalf("point %+v from row %+v", point, row)
				}
				if got[point.Metric] == nil {
					got[point.Metric] = map[string]bool{}
				}
				got[point.Metric][pointLabelText(point)] = true
			}
			if len(got) == 0 {
				t.Fatal("no rows decoded")
			}
			for metric, sets := range got {
				if !sameSet(sets, want[metric]) {
					t.Fatalf("%s series:\n got  %v\n want %v", metric, keys(sets), keys(want[metric]))
				}
			}
		})
	}
}

func TestTheIdentityDoesNotDependOnTheOrderTheDimensionsCameIn(t *testing.T) {
	metric, value, at := "cpu", 1.0, int64(1700000000000)
	first := `{"namespace":"demo","pod_name":"web-0","container":"app"}`
	second := `{"container":"app","namespace":"demo","pod_name":"web-0"}`
	other := `{"container":"app","namespace":"demo","pod_name":"web-1"}`
	identity := func(dimensions string) string {
		points, err := realtimeinput.Project(realtimeinput.SourceK8s,
			realtimeinput.Row{Metric: &metric, Value: &value, EventTimeMs: &at, Dimensions: &dimensions})
		if err != nil {
			t.Fatal(err)
		}
		return points[0].Identity()
	}
	if identity(first) != identity(second) {
		t.Fatal("the same dimensions in another order gave another series")
	}
	if identity(first) == identity(other) {
		t.Fatal("other dimensions gave the same series")
	}
	// A value holding what a separator would be cannot stand for two labels, whatever the separator would have been.
	joined := `{"a":"1","b":"2"}`
	for _, split := range []string{`{"a":"1,b=2"}`, `{"a":"1,b,2"}`, `{"a":"1\u0000b\u00002"}`, `{"a":"1|b|2"}`, `{"a":"1;b=2"}`} {
		if identity(split) == identity(joined) {
			t.Fatalf("the value of %s gave the series of two labels", split)
		}
	}
}

func TestRowsThatYieldNoPointAreNamed(t *testing.T) {
	text := func(s string) *string { return &s }
	number := func(v float64) *float64 { return &v }
	at := int64(1700000000000)
	zero := int64(0)
	whole := func() realtimeinput.Row {
		return realtimeinput.Row{Metric: text("cpu"), Value: number(1), EventTimeMs: &at, Dimensions: text(`{"a":"b"}`),
			Target: text(`[{"bk_target_ip":"192.0.2.1"}]`), Named: map[string]string{"bk_cmdb_level": `[{"bk_biz_id":1}]`}}
	}
	cases := []struct {
		name   string
		kind   string
		change func(*realtimeinput.Row)
		want   string
	}{
		{"no metric", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Metric = nil }, realtimeinput.BadPointMetricMissing},
		{"empty metric", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Metric = text("") }, realtimeinput.BadPointMetricMissing},
		{"no value", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Value = nil }, realtimeinput.BadPointValueMissing},
		{"NaN", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Value = number(math.NaN()) }, realtimeinput.BadPointValueNotFinite},
		{"infinity", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Value = number(math.Inf(1)) }, realtimeinput.BadPointValueNotFinite},
		{"no event time", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.EventTimeMs = nil }, realtimeinput.BadPointTimeMissing},
		{"zero event time", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.EventTimeMs = &zero }, realtimeinput.BadPointTimeMissing},
		{"dimensions not an object", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Dimensions = text(`["a"]`) }, realtimeinput.BadPointDimensionsMalformed},
		{"dimensions not JSON", realtimeinput.SourceCustomReport, func(r *realtimeinput.Row) { r.Dimensions = text(`{a`) }, realtimeinput.BadPointDimensionsMalformed},
		{"target not a list", realtimeinput.SourcePluginNested, func(r *realtimeinput.Row) { r.Target = text(`{"a":1}`) }, realtimeinput.BadPointTargetMalformed},
		{"target entry not an object", realtimeinput.SourcePluginFlat, func(r *realtimeinput.Row) { r.Target = text(`["a"]`) }, realtimeinput.BadPointTargetMalformed},
		{"flat without a group", realtimeinput.SourcePluginFlat, func(r *realtimeinput.Row) { r.Target = text(`[]`) }, realtimeinput.BadPointGroupInfoEmpty},
		{"flat without a target", realtimeinput.SourcePluginFlat, func(r *realtimeinput.Row) { r.Target = nil }, realtimeinput.BadPointGroupInfoEmpty},
		{"dimensions with trailing data", realtimeinput.SourceK8s, func(r *realtimeinput.Row) { r.Dimensions = text(`{"a":"1"}{"b":"2"}`) }, realtimeinput.BadPointDimensionsMalformed},
		{"nested without a group, cmdb level with trailing data", realtimeinput.SourcePluginNested, func(r *realtimeinput.Row) {
			r.Target = text(`[]`)
			r.Named = map[string]string{"bk_cmdb_level": `[{"bk_biz_id":1}][]`}
		}, realtimeinput.BadPointCMDBLevelMalformed},
		{"nested without a group, cmdb level not a list", realtimeinput.SourcePluginNested, func(r *realtimeinput.Row) {
			r.Target = text(`[]`)
			r.Named = map[string]string{"bk_cmdb_level": `{"bk_biz_id":1}`}
		}, realtimeinput.BadPointCMDBLevelMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := whole()
			tc.change(&row)
			points, err := realtimeinput.Project(tc.kind, row)
			var bad *realtimeinput.BadPoint
			if !errors.As(err, &bad) {
				t.Fatalf("points %v, error %v", points, err)
			}
			if bad.Reason != tc.want {
				t.Fatalf("reason %q, want %q", bad.Reason, tc.want)
			}
		})
	}
	row := whole()
	if _, err := realtimeinput.Project("basereport", row); !errors.Is(err, realtimeinput.ErrSourceKindUnsupported) {
		t.Fatalf("an unknown source kind gave %v", err)
	}
}

// A nested row takes bk_biz_id from its first bk_cmdb_level only when it has no group_info (design 016 section 5.3 ②):
// with a group, the group's labels are the row's, and a group without bk_biz_id leaves the series without one, as the
// aggregated path stores it. Nor does a malformed bk_cmdb_level refuse a row that has a group, which the aggregated path
// stores all the same.
func TestANestedRowWithAGroupTakesNoBizFromItsCMDBLevel(t *testing.T) {
	text := func(s string) *string { return &s }
	value := 1.0
	at := int64(1700000000000)
	row := realtimeinput.Row{Metric: text("cpu"), Value: &value, EventTimeMs: &at, Dimensions: text(`{}`),
		Target: text(`[{"bk_target_ip":"192.0.2.50"}]`), Named: map[string]string{"bk_cmdb_level": `[{"bk_biz_id":9}]`}}
	points, err := realtimeinput.Project(realtimeinput.SourcePluginNested, row)
	if err != nil || len(points) != 1 {
		t.Fatalf("points %v, error %v", points, err)
	}
	for _, label := range points[0].Labels {
		if label.Name == "bk_biz_id" {
			t.Fatalf("a row with a group took bk_biz_id=%s from its cmdb level", label.Value)
		}
	}
	row.Named = map[string]string{"bk_cmdb_level": `{"bk_biz_id":1}`}
	if points, err := realtimeinput.Project(realtimeinput.SourcePluginNested, row); err != nil || len(points) != 1 {
		t.Fatalf("a row with a group and a malformed cmdb level: points %v, error %v", points, err)
	}
}

// The source kinds are one set, the contract's, for the channel records and the projection alike; a channel whose source
// is not resolved yet projects no row, named apart from an unsupported kind.
func TestTheSourceKindsAreTheContractsOneSet(t *testing.T) {
	if got, want := realtimeinput.SourceKinds(), contract.RealtimeSourceKinds(); !slices.Equal(got, want) {
		t.Fatalf("projection kinds %v, contract %v", got, want)
	}
	text := func(s string) *string { return &s }
	value := 1.0
	at := int64(1700000000000)
	row := realtimeinput.Row{Metric: text("cpu"), Value: &value, EventTimeMs: &at}
	if _, err := realtimeinput.Project(contract.RealtimeSourceUnresolved, row); !errors.Is(err, realtimeinput.ErrSourceKindUnresolved) {
		t.Fatalf("an unresolved channel's row gave %v", err)
	}
}

func TestBadPointReasonsAreClosed(t *testing.T) {
	want := []string{"cmdb_level_malformed", "dimensions_malformed", "group_info_empty", "metric_missing", "target_malformed",
		"time_missing", "value_missing", "value_not_finite"}
	got := append([]string(nil), realtimeinput.BadPointReasons()...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("reasons %v, want %v", got, want)
	}
}
