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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The record ids a normalized series carries, derived on one deriver of the
// series (014 next9 ⑤ R), are byte for byte the ones the one-shot
// derivation gives - the derivation every build before gave, its outputs
// held to that build's in contract's golden file.
func TestTheRecordIDsOfANormalizedSeriesAreTheOnesTheOneShotDerivationGives(t *testing.T) {
	attempt := validAttempt(t)
	series := identityTestSeries([]string{"bk_target_ip"}, []string{"127.0.0.1"})
	series.Values = nil
	for point := 0; point < 9; point++ {
		series.Values = append(series.Values, []json.RawMessage{
			json.RawMessage(strconv.FormatInt(1_700_123_456_789+int64(point)*60_000, 10)), json.RawMessage(`12.5`)})
	}
	oneShots, built := contract.RecordIDDerivationCounts()
	batch, _, ids, err := normalizeSeriesKeeping(attempt.Spec, "provider-result", series, 1_700_123_500, true)
	if err != nil {
		t.Fatal(err)
	}
	// One deriver for the series, no id derived one at a time.
	if afterOneShots, afterBuilt := contract.RecordIDDerivationCounts(); afterOneShots != oneShots || afterBuilt-built != 1 {
		t.Fatalf("the series derived %d ids one at a time and built %d derivers, want none and one", afterOneShots-oneShots, afterBuilt-built)
	}
	if len(ids) < 2 || batch.Dataset.Len() != len(ids) {
		t.Fatalf("fixture: %d ids over %d records, want a series of points", len(ids), batch.Dataset.Len())
	}
	for index, id := range ids {
		record, _ := batch.Dataset.Record(index)
		want, err := contract.DeriveRecordIDV2(record.DimensionIdentityDigest(), record.SourceTime())
		if err != nil {
			t.Fatal(err)
		}
		if id != want {
			t.Fatalf("point %d at %d carries id %s, want %s", index, record.SourceTime(), id, want)
		}
	}
}

// oneShotRowSourceTime is the row's time as rowFacts read it before (014
// next9 ⑤ T): decoded as a json.Number and read as an int64.
func oneShotRowSourceTime(raw json.RawMessage) (int64, error) {
	var timestamp json.Number
	if err := json.Unmarshal(raw, &timestamp); err != nil {
		return 0, errors.New("alarmd access uq: invalid source time")
	}
	value, err := timestamp.Int64()
	if err != nil {
		return 0, errors.New("alarmd access uq: non-integer source time")
	}
	return value, nil
}

// A row's time read byte by byte where it is a bare integer token, and as
// before otherwise, is the time and the refusal it was: every writing a
// provider could send.
func TestARowsSourceTimeIsReadAsTheDecodedNumberReadsIt(t *testing.T) {
	for _, raw := range []string{"1700000000", " 17 ", "\t17\n", "-0", "0", "-5", "01", "-01", `"123"`, `"12"`, "1.0", "1e3", "1E3",
		"null", "true", "99999999999999999999", "-9223372036854775808", "9223372036854775807", "9223372036854775808",
		"-9223372036854775809", "", " ", "-", "+1", "1 2", "0x10", "1_000", "١٢"} {
		got, gotErr := rowSourceTime(json.RawMessage(raw))
		want, wantErr := oneShotRowSourceTime(json.RawMessage(raw))
		if got != want || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
			t.Fatalf("%q: read %d (%v), want %d (%v)", raw, got, gotErr, want, wantErr)
		}
	}
}

// The time and value columns found once a series are the ones each row
// found: the last time column, the first value column, else the first a
// query names; missing, each row's refusal.
func TestARowsColumnsAreFoundOnceAsEachRowFoundThem(t *testing.T) {
	row := []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`), json.RawMessage(`3`), json.RawMessage(`4`)}
	for _, test := range []struct {
		columns     []string
		time, value int64
	}{
		{[]string{"_time", "_value", "_time_", "_result"}, 3, 2},
		{[]string{"_result_", "_time", "x", "_value"}, 2, 1},
		{[]string{"_time", "a", "b", "c"}, 1, 0},
	} {
		timeValue, value, err := rowColumnsOf(test.columns, nil).facts(row)
		if test.value == 0 {
			if err == nil {
				t.Fatalf("%v: read a row with no value column", test.columns)
			}
			continue
		}
		if err != nil || timeValue != test.time || string(value) != strconv.FormatInt(test.value, 10) {
			t.Fatalf("%v: time %d value %s (%v), want %d and %d", test.columns, timeValue, value, err, test.time, test.value)
		}
	}
}
