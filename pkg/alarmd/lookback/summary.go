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
	"math"
	"strconv"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A read is kept as a summary per bucket (a record's source time): how many
// points the bucket had, and two sums over its points - of each point's
// series, and of each point's series, time and value - every term hashed to
// 64 bits and the sums taken modulo 2^64. Sums do not depend on the order the
// points arrive in, so the same data read in another series or page order
// sums to the same bits, and a bucket is three words whatever the number of
// series. That is what lets every owned Query Group keep a sample.
type bucketSummary struct {
	points uint64
	series uint64
	values uint64
}

// readSummary is one read's buckets by source time.
type readSummary map[int64]bucketSummary

// summaryEntryBytes is what one bucket is charged: its key, its three words
// and the map's share of an entry.
const summaryEntryBytes = 48

func (summary readSummary) bytes() int { return len(summary) * summaryEntryBytes }

// maxBucketsPerSample guards against a window or step read wrongly. A day
// at one-second steps is 86400 buckets; no Query Group's window reaches
// 2^17 buckets, so a read that does is a defect to fix, counted as a fault
// and not kept, never a bound that normal running meets.
const maxBucketsPerSample = 1 << 17

// summarizer builds a readSummary from delivered series.
type summarizer struct {
	valueField string
	buckets    readSummary
	buffer     []byte
	faulted    bool
}

func newSummarizer(valueField string) *summarizer {
	return &summarizer{valueField: valueField, buckets: readSummary{}}
}

// add sums one delivered series: every record's bucket.
func (summarizer *summarizer) add(dataset *execution.Dataset) {
	if summarizer.faulted || dataset == nil || dataset.Len() == 0 {
		return
	}
	first, _ := dataset.Record(0)
	series := hashString(first.DimensionIdentityDigest())
	seriesTerm := mix(series)
	for index := 0; index < dataset.Len(); index++ {
		record, _ := dataset.Record(index)
		at := record.SourceTime()
		bucket, known := summarizer.buckets[at]
		if !known && len(summarizer.buckets) >= maxBucketsPerSample {
			summarizer.faulted = true
			summarizer.buckets = nil
			return
		}
		summarizer.buffer, _ = record.AppendValue(summarizer.buffer[:0], summarizer.valueField)
		bucket.points++
		bucket.series += seriesTerm
		bucket.values += pointHash(series, at, valueBits(summarizer.buffer))
		summarizer.buckets[at] = bucket
	}
}

// valueBits is a value as the bits it is compared by: a number's IEEE bits,
// so the same number rendered two ways is one value, with -0 read as 0 and
// every NaN as one; anything else by a hash of its text.
func valueBits(text []byte) uint64 {
	if number, err := strconv.ParseFloat(string(text), 64); err == nil {
		switch {
		case number == 0:
			return 0
		case math.IsNaN(number):
			return math.Float64bits(math.NaN())
		default:
			return math.Float64bits(number)
		}
	}
	return hashBytes(text) ^ 0xa0761d6478bd642f
}

// Changes between two reads of one bucket, closed.
const (
	ChangePointsAdded   = "points_added"
	ChangePointsRemoved = "points_removed"
	ChangeSeriesChanged = "series_changed"
	ChangeValuesChanged = "values_changed"
)

// Changes is every change class.
var Changes = []string{ChangePointsAdded, ChangePointsRemoved, ChangeSeriesChanged, ChangeValuesChanged}

// compareSummaries counts the buckets of later that differ from earlier, by
// class: more points than before, fewer, as many from another set of series,
// or the same series with other values.
func compareSummaries(earlier, later readSummary) map[string]int {
	changes := map[string]int{}
	classify := func(before, after bucketSummary) {
		switch {
		case before == after:
		case after.points > before.points:
			changes[ChangePointsAdded]++
		case after.points < before.points:
			changes[ChangePointsRemoved]++
		case after.series != before.series:
			changes[ChangeSeriesChanged]++
		default:
			changes[ChangeValuesChanged]++
		}
	}
	for at, after := range later {
		classify(earlier[at], after)
	}
	for at, before := range earlier {
		if _, present := later[at]; !present {
			classify(before, bucketSummary{})
		}
	}
	return changes
}

// hashBytes and hashString are 64-bit FNV-1a, spelled out so hashing a
// point allocates nothing.
func hashBytes(data []byte) uint64 {
	hash := uint64(14695981039346656037)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

func hashString(text string) uint64 {
	hash := uint64(14695981039346656037)
	for index := 0; index < len(text); index++ {
		hash ^= uint64(text[index])
		hash *= 1099511628211
	}
	return hash
}

// mix is the splitmix64 finalizer: every input bit reaches every output bit,
// so sums of mixed terms do not cancel the way sums of raw hashes can.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func pointHash(series uint64, at int64, value uint64) uint64 {
	return mix(series ^ mix(uint64(at)+0x9e3779b97f4a7c15) ^ mix(value+0x632be59bd9b4e019))
}
