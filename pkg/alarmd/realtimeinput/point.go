// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeinput

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The channel's source kinds (the channel record's source_kind): the clean rules a result table's rows were written by, and
// so how a row becomes the series the aggregated path stores in VictoriaMetrics.
const (
	SourceK8s           = contract.RealtimeSourceK8s
	SourceCustomReport  = contract.RealtimeSourceCustomReport
	SourcePluginFlat    = contract.RealtimeSourcePluginFlat
	SourcePluginNested  = contract.RealtimeSourcePluginNested
	userIPLabel         = "_user_tmp_ip_"
	ipLabel             = "ip"
	bizLabel            = "bk_biz_id"
	envelopeIP          = "ip"
	envelopeAgentID     = "bk_agent_id"
	envelopeHostID      = "bk_host_id"
	envelopeSupplier    = "bizid"
	envelopeCloud       = "cloudid"
	envelopeCMDBLevel   = "bk_cmdb_level"
	labelSupplierID     = "bk_supplier_id"
	labelCloudID        = "bk_cloud_id"
	labelAgentID        = "bk_agent_id"
	labelHostID         = "bk_host_id"
	cmdbLevelBizIDField = "bk_biz_id"
)

// SourceKinds is the closed set of source kinds a channel's rows are projected by: the contract's one set.
func SourceKinds() []string {
	return contract.RealtimeSourceKinds()
}

// ErrSourceKindUnsupported is a source kind outside SourceKinds: the channel record decides it, so a row is never refused
// for it one at a time.
var ErrSourceKindUnsupported = errors.New("realtime input: source kind not supported")

// ErrSourceKindUnresolved is a channel whose record says no source kind yet (contract.RealtimeSourceUnresolved): its rows
// are not projected, and the reader counts them by channel rather than as bad points.
var ErrSourceKindUnresolved = errors.New("realtime input: source kind not resolved yet")

// The reasons a row yields no point. The set is closed.
const (
	BadPointMetricMissing       = "metric_missing"
	BadPointValueMissing        = "value_missing"
	BadPointValueNotFinite      = "value_not_finite"
	BadPointTimeMissing         = "time_missing"
	BadPointDimensionsMalformed = "dimensions_malformed"
	BadPointTargetMalformed     = "target_malformed"
	BadPointCMDBLevelMalformed  = "cmdb_level_malformed"
	// BadPointGroupInfoEmpty: a flat plugin row without a target. The aggregated path writes no series for it, so the
	// realtime path judges none either.
	BadPointGroupInfoEmpty = "group_info_empty"
)

// BadPointReasons is the closed set of the reasons a row yields no point.
func BadPointReasons() []string {
	return []string{BadPointMetricMissing, BadPointValueMissing, BadPointValueNotFinite, BadPointTimeMissing,
		BadPointDimensionsMalformed, BadPointTargetMalformed, BadPointCMDBLevelMalformed, BadPointGroupInfoEmpty}
}

// BadPoint is a row that yields no point, with its named reason.
type BadPoint struct {
	Reason string
	Err    error
}

func (b *BadPoint) Error() string {
	if b.Err == nil {
		return "realtime input row: " + b.Reason
	}
	return fmt.Sprintf("realtime input row: %s: %v", b.Reason, b.Err)
}

func (b *BadPoint) Unwrap() error { return b.Err }

// Label is one label of a series.
type Label struct {
	Name  string
	Value string
}

// Point is one point of one series: the series' labels as the aggregated path stores them in VictoriaMetrics, less the
// result_table_id and db it adds, sorted by name and without empty values.
type Point struct {
	Metric      string
	Value       float64
	EventTimeMs int64
	Labels      []Label
}

// Identity is the series identity: the metric and its labels, each part length-prefixed so no value can stand for a
// separator.
func (p Point) Identity() string {
	var out strings.Builder
	writePart(&out, p.Metric)
	for _, label := range p.Labels {
		writePart(&out, label.Name)
		writePart(&out, label.Value)
	}
	return out.String()
}

func writePart(out *strings.Builder, part string) {
	out.WriteString(strconv.Itoa(len(part)))
	out.WriteByte(':')
	out.WriteString(part)
}

// Project turns a row into the points of the series the aggregated path stores for it (design 016 section 5.3), each
// rule observed against VictoriaMetrics for the same input. K8s and custom report rows are one series of their
// dimensions. A plugin row starts from its dimensions, the envelope's derived labels over them (ip, bk_supplier_id from
// bizid, bk_cloud_id from cloudid, and for the flat format bk_agent_id and bk_host_id), and its target's group_info entries
// over both: the flat format makes one series per entry and none for a row without one; the nested format lays every
// entry over the one series in order, and without an entry takes bk_biz_id from the first bk_cmdb_level, keeping a
// reported ip dimension as _user_tmp_ip_.
func Project(kind string, row Row) ([]Point, error) {
	if kind == contract.RealtimeSourceUnresolved {
		return nil, ErrSourceKindUnresolved
	}
	if !knownKind(kind) {
		return nil, fmt.Errorf("%w: %q", ErrSourceKindUnsupported, kind)
	}
	base, err := pointOf(row)
	if err != nil {
		return nil, err
	}
	dimensions, err := objectOf(row.Dimensions)
	if err != nil {
		return nil, &BadPoint{Reason: BadPointDimensionsMalformed, Err: err}
	}
	switch kind {
	case SourceK8s, SourceCustomReport:
		return []Point{withLabels(base, dimensions)}, nil
	}
	groups, err := groupsOf(row.Target)
	if err != nil {
		return nil, &BadPoint{Reason: BadPointTargetMalformed, Err: err}
	}
	envelope := row.Named
	derived := map[string]string{
		ipLabel:         envelope[envelopeIP],
		labelSupplierID: envelope[envelopeSupplier],
		labelCloudID:    envelope[envelopeCloud],
	}
	if kind == SourcePluginFlat {
		if len(groups) == 0 {
			return nil, &BadPoint{Reason: BadPointGroupInfoEmpty}
		}
		derived[labelAgentID] = envelope[envelopeAgentID]
		derived[labelHostID] = envelope[envelopeHostID]
		points := make([]Point, 0, len(groups))
		for _, group := range groups {
			labels := copyLabels(dimensions)
			overlay(labels, derived)
			overlay(labels, group)
			points = append(points, withLabels(base, labels))
		}
		return points, nil
	}
	labels := copyLabels(dimensions)
	if user, ok := labels[ipLabel]; ok {
		// The nested format keeps a reported ip dimension under its own name before the envelope's ip takes the label.
		delete(labels, ipLabel)
		labels[userIPLabel] = user
	}
	if len(groups) == 0 {
		// Only a row without a group takes bk_biz_id from its first cmdb level; with a group the group's labels are the
		// row's, and its cmdb level is not read at all.
		biz, err := cmdbBizID(envelope[envelopeCMDBLevel])
		if err != nil {
			return nil, &BadPoint{Reason: BadPointCMDBLevelMalformed, Err: err}
		}
		if biz != "" {
			labels[bizLabel] = biz
		}
	}
	overlay(labels, derived)
	for _, group := range groups {
		overlay(labels, group)
	}
	return []Point{withLabels(base, labels)}, nil
}

func knownKind(kind string) bool {
	for _, known := range SourceKinds() {
		if kind == known {
			return true
		}
	}
	return false
}

func pointOf(row Row) (Point, error) {
	switch {
	case row.Metric == nil || *row.Metric == "":
		return Point{}, &BadPoint{Reason: BadPointMetricMissing}
	case row.Value == nil:
		return Point{}, &BadPoint{Reason: BadPointValueMissing}
	case math.IsNaN(*row.Value) || math.IsInf(*row.Value, 0):
		return Point{}, &BadPoint{Reason: BadPointValueNotFinite}
	case row.EventTimeMs == nil || *row.EventTimeMs <= 0:
		return Point{}, &BadPoint{Reason: BadPointTimeMissing}
	}
	return Point{Metric: *row.Metric, Value: *row.Value, EventTimeMs: *row.EventTimeMs}, nil
}

// objectOf reads a JSON object's members as label values: a string as its text, any other value as its JSON text as
// written (a number keeps the digits it was written with), null as no value. An absent or empty text is no members.
func objectOf(text *string) (map[string]string, error) {
	out := map[string]string{}
	if text == nil || strings.TrimSpace(*text) == "" {
		return out, nil
	}
	var members map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(*text))
	decoder.UseNumber()
	if err := decoder.Decode(&members); err != nil {
		return nil, err
	}
	if err := atEnd(decoder); err != nil {
		return nil, err
	}
	if members == nil {
		return nil, errors.New("not an object")
	}
	for name, raw := range members {
		value, err := labelValue(raw)
		if err != nil {
			return nil, fmt.Errorf("member %q: %w", name, err)
		}
		out[name] = value
	}
	return out, nil
}

func labelValue(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	switch {
	case len(trimmed) == 0 || string(trimmed) == "null":
		return "", nil
	case trimmed[0] == '"':
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return "", err
		}
		return text, nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err != nil {
		return "", err
	}
	return compact.String(), nil
}

// groupsOf reads the target text: the group_info list, each entry an object of label values.
func groupsOf(text *string) ([]map[string]string, error) {
	if text == nil || strings.TrimSpace(*text) == "" {
		return nil, nil
	}
	var entries []json.RawMessage
	if err := json.Unmarshal([]byte(*text), &entries); err != nil {
		return nil, err
	}
	groups := make([]map[string]string, 0, len(entries))
	for i, entry := range entries {
		entryText := string(entry)
		group, err := objectOf(&entryText)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		groups = append(groups, group)
	}
	return groups, nil
}

// cmdbBizID is the bk_biz_id of the first entry of the envelope's bk_cmdb_level list, as text; none when the list is
// absent or empty.
func cmdbBizID(text string) (string, error) {
	if strings.TrimSpace(text) == "" {
		return "", nil
	}
	var levels []map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&levels); err != nil {
		return "", err
	}
	if err := atEnd(decoder); err != nil {
		return "", err
	}
	if len(levels) == 0 {
		return "", nil
	}
	raw, ok := levels[0][cmdbLevelBizIDField]
	if !ok {
		return "", nil
	}
	return labelValue(raw)
}

// atEnd refuses text after the one JSON value a field holds: `{"a":"1"}{"b":"2"}` is malformed, not its first object.
func atEnd(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing data after the value")
	}
	return nil
}

func copyLabels(from map[string]string) map[string]string {
	out := make(map[string]string, len(from)+8)
	for name, value := range from {
		out[name] = value
	}
	return out
}

func overlay(labels, over map[string]string) {
	for name, value := range over {
		labels[name] = value
	}
}

func withLabels(base Point, labels map[string]string) Point {
	out := base
	out.Labels = make([]Label, 0, len(labels))
	for name, value := range labels {
		if value == "" {
			continue
		}
		out.Labels = append(out.Labels, Label{Name: name, Value: value})
	}
	sort.Slice(out.Labels, func(i, j int) bool { return out.Labels[i].Name < out.Labels[j].Name })
	return out
}
