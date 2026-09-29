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
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Values a store summed in another order are one value: these pairs are two
// reads of the same twenty-minute-old points in one deployment. A change a
// threshold could turn on is another value, however small next to that.
func TestValuesThatDifferOnlyInHowTheyWereSummedAreOneValue(t *testing.T) {
	for _, pair := range [][2]string{
		{"810051.6916666667", "810051.6916666663"},
		{"869455.4750000001", "869455.4749999999"},
		{"807011.3083333333", "807011.3083333328"},
		{"837278.1250000001", "837278.1249999998"},
		{"1", "1.0000000000000002"},
		{"-42.5", "-42.50000000000001"},
	} {
		if valueBits([]byte(pair[0])) != valueBits([]byte(pair[1])) {
			t.Errorf("%s and %s are two values, want one", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{
		{"810051.6916666667", "810051.70"},
		{"1", "1.00000001"},
		{"-42.5", "42.5"},
		{"0.1", "0.2"},
		{"+Inf", "-Inf"},
		{"+Inf", strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64)},
	} {
		if valueBits([]byte(pair[0])) == valueBits([]byte(pair[1])) {
			t.Errorf("%s and %s are one value, want two", pair[0], pair[1])
		}
	}
	if valueBits([]byte("-0")) != valueBits([]byte("0")) || valueBits([]byte("NaN")) != valueBits([]byte("nan")) {
		t.Error("-0 and 0, or two NaNs, are two values")
	}
}

// A Query Group whose every read of a window comes back different only in
// how its store summed it is complete: no rung changed, and it is not read
// early however many samples say so.
func TestAGroupWhoseReadsDifferOnlyInHowTheyWereSummedIsComplete(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		f.classSample(0, []*execution.Dataset{point(f.clock.now().Unix(), "810051.6916666667")},
			func(slot int64) []*execution.Dataset { return []*execution.Dataset{point(slot, "810051.6916666663")} })
	}
	source := f.engine.Stats().Sources[sourceLog]
	if source.Classes[ClassComplete] != 3 || source.Classes[ClassWindowReadEarly] != 0 || source.ChangedWindows[RungNames[0]] != 0 ||
		len(f.engine.ReadEarly()) != 0 {
		t.Fatalf("classes %v changed %v read early %+v, want three complete samples", source.Classes, source.ChangedWindows,
			f.engine.ReadEarly())
	}
}

// A sample is classed by what its data settled to. A value that changed and
// came back was judged as it stands, and data that came to an empty first
// read and went again was not there to judge: both are complete, where a
// class kept from the first rung that differed called them read early.
func TestAChangeThatCameBackIsNotAWindowReadEarly(t *testing.T) {
	f := newFixture(t)
	f.classSampleByRung(0, []*execution.Dataset{point(f.clock.now().Unix(), "1")}, func(slot int64, rung int) []*execution.Dataset {
		if rung == 0 {
			return []*execution.Dataset{point(slot, "3")}
		}
		return []*execution.Dataset{point(slot, "1")}
	})
	f.classSampleByRung(0, nil, func(slot int64, rung int) []*execution.Dataset {
		if rung == 0 {
			return []*execution.Dataset{point(slot, "1")}
		}
		return nil
	})
	source := f.engine.Stats().Sources[sourceLog]
	if source.Classes[ClassComplete] != 2 || source.Classes[ClassWindowReadEarly] != 0 || source.Classes[ClassSeriesLate] != 0 ||
		source.ChangedWindows[RungNames[1]] != 2 {
		t.Fatalf("classes %v changed %v, want both complete after their second rung changed back", source.Classes, source.ChangedWindows)
	}
	if state := f.group("qg"); state.readEarly != nil || state.seriesLate != nil {
		t.Fatalf("a change that came back marked its group: %+v %+v", state.readEarly, state.seriesLate)
	}
	// Changed and held, the same sample is read early: two later reads
	// agree, and both differ from the first.
	f.classSampleByRung(0, []*execution.Dataset{point(f.clock.now().Unix(), "1")}, func(slot int64, rung int) []*execution.Dataset {
		return []*execution.Dataset{point(slot, "3")}
	})
	if source := f.engine.Stats().Sources[sourceLog]; source.Classes[ClassWindowReadEarly] != 1 {
		t.Fatalf("classes %v, want a change that held to be read early", source.Classes)
	}
}

// A sample still changing at the deepest rung has no later read to say its
// data settled: unclassified as unsettled, which neither starts a run of
// window_read_early nor ends one.
func TestASampleStillChangingAtTheDeepestRungIsUnsettled(t *testing.T) {
	f := newFixture(t)
	f.classSampleByRung(0, []*execution.Dataset{point(f.clock.now().Unix(), "1")}, func(slot int64, rung int) []*execution.Dataset {
		return []*execution.Dataset{point(slot, strconv.Itoa(rung+2))}
	})
	stats := f.engine.Stats()
	source := stats.Sources[sourceLog]
	if source.Classes[ClassUnclassified] != 1 || source.Unclassified[UnclassifiedUnsettled] != 1 ||
		source.Unclassified[UnclassifiedMemoryRefused] != 0 || source.Classes[ClassWindowReadEarly] != 0 ||
		source.ChangedWindows[RungNames[len(RungNames)-1]] != 1 {
		t.Fatalf("classes %v unclassified %v changed %v, want one unsettled sample changed at every rung", source.Classes,
			source.Unclassified, source.ChangedWindows)
	}
	if state := f.group("qg"); state.readEarly != nil {
		t.Fatalf("an unsettled sample started a run: %+v", state.readEarly)
	}
}
