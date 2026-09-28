// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// How one (series, bucket) of the recheck differs from the first read,
// closed. A bucket with no record is not a bucket with 0: "new" and
// "vanished" are records that were or were not there, and a 0 is a value.
const (
	DiffUnchanged      = "unchanged"
	DiffZeroToNonzero  = "zero_to_nonzero"
	DiffIncreased      = "increased"
	DiffDecreased      = "decreased"
	DiffNewPoint       = "new_point"
	DiffVanishedPoint  = "vanished_point"
	DiffNewSeries      = "new_series"
	DiffVanishedSeries = "vanished_series"
	DiffNotNumeric     = "not_numeric"
)

// Differences is every class, in the order a reader lists them.
var Differences = []string{DiffUnchanged, DiffZeroToNonzero, DiffIncreased, DiffDecreased, DiffNewPoint,
	DiffVanishedPoint, DiffNewSeries, DiffVanishedSeries, DiffNotNumeric}

// How one Level's verdict on one (series, bucket) moved between the reads,
// for a Plan that admitted the series at the first read. absent_to_* is a
// bucket the first read did not have. A series the first read did not have
// at all has no admission to judge it under and is not judged.
const (
	JudgeUnchanged        = "unchanged"
	JudgeNormalToAbnormal = "normal_to_abnormal"
	JudgeAbnormalToNormal = "abnormal_to_normal"
	JudgeAbsentToAbnormal = "absent_to_abnormal"
	JudgeAbsentToNormal   = "absent_to_normal"
)

// Judgments is every judgment class.
var Judgments = []string{JudgeUnchanged, JudgeNormalToAbnormal, JudgeAbnormalToNormal, JudgeAbsentToAbnormal, JudgeAbsentToNormal}

// seriesRead is one series of one read: its buckets ascending, each value's
// text in one buffer (text[ends[i-1]:ends[i]] is buckets[i]'s), and, on the
// first read, which of the sample's Plans admitted it (bit i is plans[i]).
// dims is a rendering kept only on a recheck, for examples. One buffer per
// series, not one string per point: a first read is kept on the formal
// query's own goroutine.
type seriesRead struct {
	admitted uint64
	buckets  []int64
	ends     []uint32
	text     []byte
	dims     string
}

func (series *seriesRead) value(index int) []byte {
	start := uint32(0)
	if index > 0 {
		start = series.ends[index-1]
	}
	return series.text[start:series.ends[index]]
}

// keep adds every record of a delivered series.
func (series *seriesRead) keep(dataset *execution.Dataset, valueField string) {
	if cap(series.text) == 0 {
		// One allocation for a series whose values are short, as counts are.
		series.text = make([]byte, 0, 8*dataset.Len())
	}
	for index := 0; index < dataset.Len(); index++ {
		record, _ := dataset.Record(index)
		series.text, _ = record.AppendValue(series.text, valueField)
		series.buckets = append(series.buckets, record.SourceTime())
		series.ends = append(series.ends, uint32(len(series.text)))
	}
}

// seal puts the buckets in order, a later record of one bucket replacing an
// earlier one; the provider delivers them in order, so this is a check.
func (series *seriesRead) seal() {
	ordered := true
	for index := 1; index < len(series.buckets); index++ {
		if series.buckets[index] <= series.buckets[index-1] {
			ordered = false
			break
		}
	}
	if ordered {
		return
	}
	last := make(map[int64]int, len(series.buckets))
	for index, bucket := range series.buckets {
		last[bucket] = index
	}
	order := make([]int, 0, len(last))
	for _, index := range last {
		order = append(order, index)
	}
	sort.Slice(order, func(i, j int) bool { return series.buckets[order[i]] < series.buckets[order[j]] })
	sealed := &seriesRead{admitted: series.admitted, dims: series.dims}
	for _, index := range order {
		sealed.buckets = append(sealed.buckets, series.buckets[index])
		sealed.text = append(sealed.text, series.value(index)...)
		sealed.ends = append(sealed.ends, uint32(len(sealed.text)))
	}
	*series = *sealed
}

// bytes is what the series holds.
func (series *seriesRead) bytes() int {
	return cap(series.buckets)*8 + cap(series.ends)*4 + cap(series.text)
}

type readSet map[string]*seriesRead

// DiffExample is one differing (series, bucket), for a reader to look at.
type DiffExample struct {
	Series     string `json:"series"`
	Dimensions string `json:"dimensions,omitempty"`
	Bucket     int64  `json:"bucket"`
	First      string `json:"first,omitempty"`
	Recheck    string `json:"recheck,omitempty"`
	Class      string `json:"class"`
}

type comparison struct {
	buckets         int
	differences     map[string]int
	judgments       map[string]int
	newSeries       int
	vanishedSeries  int
	examples        []DiffExample
	differsInWindow bool
}

// maxExamples bounds the examples one comparison keeps.
const maxExamples = 8

func compare(first, recheck readSet, plans []planCheck) comparison {
	result := comparison{differences: map[string]int{}, judgments: map[string]int{}}
	note := func(digest string, bucket int64, v0, v1 []byte, class, dims string) {
		result.differences[class]++
		if class == DiffUnchanged {
			return
		}
		result.differsInWindow = true
		if len(result.examples) < maxExamples {
			result.examples = append(result.examples, DiffExample{Series: shortDigest(digest), Dimensions: dims, Bucket: bucket,
				First: string(v0), Recheck: string(v1), Class: class})
		}
	}
	digests := make([]string, 0, len(first)+len(recheck))
	for digest := range first {
		digests = append(digests, digest)
	}
	for digest := range recheck {
		if _, known := first[digest]; !known {
			digests = append(digests, digest)
		}
	}
	sort.Strings(digests)
	for _, digest := range digests {
		before, after := first[digest], recheck[digest]
		dims := ""
		if after != nil {
			dims = after.dims
		}
		switch {
		case before == nil:
			result.newSeries++
			for index, bucket := range after.buckets {
				result.buckets++
				note(digest, bucket, nil, after.value(index), DiffNewSeries, dims)
			}
			continue
		case after == nil:
			result.vanishedSeries++
			for index, bucket := range before.buckets {
				result.buckets++
				note(digest, bucket, before.value(index), nil, DiffVanishedSeries, "")
			}
			continue
		}
		i, j := 0, 0
		for i < len(before.buckets) || j < len(after.buckets) {
			result.buckets++
			switch {
			case j == len(after.buckets) || i < len(before.buckets) && before.buckets[i] < after.buckets[j]:
				note(digest, before.buckets[i], before.value(i), nil, DiffVanishedPoint, dims)
				i++
			case i == len(before.buckets) || after.buckets[j] < before.buckets[i]:
				note(digest, after.buckets[j], nil, after.value(j), DiffNewPoint, dims)
				result.judge(plans, before.admitted, nil, after.value(j))
				j++
			default:
				v0, v1 := before.value(i), after.value(j)
				note(digest, before.buckets[i], v0, v1, valueChange(v0, v1), dims)
				result.judge(plans, before.admitted, v0, v1)
				i++
				j++
			}
		}
	}
	return result
}

// judge counts, for every Plan that admitted the series and can be decided
// from a value, how each Level's verdict moved. v0 nil is a bucket the first
// read did not have. The admission mask has 64 bits: a query feeding more
// than 64 Plans judges its first 64 and compares the rest as data only,
// which a log query's Plans do not come near.
func (result *comparison) judge(plans []planCheck, admitted uint64, v0, v1 []byte) {
	for index, plan := range plans {
		if index >= 64 || admitted&(1<<uint(index)) == 0 || !plan.comparable {
			continue
		}
		for _, level := range plan.levels {
			after, ok := level.abnormal(v1)
			if !ok {
				continue
			}
			if v0 == nil {
				if after {
					result.judgments[JudgeAbsentToAbnormal]++
				} else {
					result.judgments[JudgeAbsentToNormal]++
				}
				continue
			}
			before, ok := level.abnormal(v0)
			if !ok {
				continue
			}
			switch {
			case before == after:
				result.judgments[JudgeUnchanged]++
			case after:
				result.judgments[JudgeNormalToAbnormal]++
			default:
				result.judgments[JudgeAbnormalToNormal]++
			}
		}
	}
}

func valueChange(v0, v1 []byte) string {
	a, b := bytes.TrimSpace(v0), bytes.TrimSpace(v1)
	if bytes.Equal(a, b) {
		return DiffUnchanged
	}
	x, errA := strconv.ParseFloat(string(a), 64)
	y, errB := strconv.ParseFloat(string(b), 64)
	switch {
	case errA != nil || errB != nil:
		return DiffNotNumeric
	case x == y:
		return DiffUnchanged
	case x == 0:
		return DiffZeroToNonzero
	case y > x:
		return DiffIncreased
	default:
		return DiffDecreased
	}
}

func shortDigest(digest string) string {
	if len(digest) > 16 {
		return digest[:16]
	}
	return digest
}
