// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

func targetsFacts(space string, global bool, clauses ...[2]string) QueryPlanFacts {
	facts := QueryPlanFacts{SpaceScope: space, GlobalBusiness: global}
	for _, clause := range clauses {
		facts.QueryList = append(facts.QueryList, QueryClause{TableID: clause[0], FieldName: clause[1]})
	}
	return facts
}

// What a query asked for is read from its own query list: each table and
// field once, sorted, under the space the request was scoped to - none for a
// global business. A query naming no table has none.
func TestRequestTargetsAreTheQuerysOwnEachOnceSorted(t *testing.T) {
	got := RequestTargetsOf(targetsFacts("bkcc__2", false, [2]string{"system.mem", "used"}, [2]string{"system.cpu", "usage"}, [2]string{"system.mem", "used"}))
	want := &observability.RequestTargetsFacts{Count: 2, Targets: []observability.RequestTarget{
		{TableID: "system.cpu", FieldName: "usage", SpaceUID: "bkcc__2"}, {TableID: "system.mem", FieldName: "used", SpaceUID: "bkcc__2"}}}
	if got == nil || got.Digest == "" {
		t.Fatalf("RequestTargetsOf() = %+v, want two targets with a digest", got)
	}
	want.Digest = got.Digest
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RequestTargetsOf() = %+v, want %+v", got, want)
	}
	if global := RequestTargetsOf(targetsFacts("bkcc__2", true, [2]string{"system.cpu", "usage"})); global == nil || global.Targets[0].SpaceUID != "" {
		t.Fatalf("a global business's request was scoped as %+v, want no space", global)
	}
	if none := RequestTargetsOf(targetsFacts("bkcc__2", false)); none != nil {
		t.Fatalf("a query naming no table asked for %+v", none)
	}
}

// The digest keys the whole set as asked, and only the set: the same set in
// another order is one key; two sets alike in their first five and their
// count, apart past them, are two; two tables alike in their first 128 bytes
// are two, though the line shows both cut alike and marked; and names that
// would join alike are two.
func TestTheRequestTargetsDigestKeysTheWholeSetUncut(t *testing.T) {
	seven := func(last string) [][2]string {
		var clauses [][2]string
		for i := 0; i < 6; i++ {
			clauses = append(clauses, [2]string{fmt.Sprintf("table_%d", i), "value"})
		}
		return append(clauses, [2]string{last, "value"})
	}
	a, b := RequestTargetsOf(targetsFacts("s", false, seven("table_x")...)), RequestTargetsOf(targetsFacts("s", false, seven("table_y")...))
	if a.Count != 7 || b.Count != 7 || !reflect.DeepEqual(a.Targets, b.Targets) || len(a.Targets) != observability.RequestTargetSample {
		t.Fatalf("the fixture is meant to name the same five of seven: %+v against %+v", a, b)
	}
	if a.Digest == b.Digest {
		t.Fatalf("two sets apart past their first five share the key %s", a.Digest)
	}

	forward := RequestTargetsOf(targetsFacts("s", false, [2]string{"t1", "f"}, [2]string{"t2", "f"}, [2]string{"t3", "f"}))
	backward := RequestTargetsOf(targetsFacts("s", false, [2]string{"t3", "f"}, [2]string{"t2", "f"}, [2]string{"t1", "f"}, [2]string{"t2", "f"}))
	if !reflect.DeepEqual(forward, backward) {
		t.Fatalf("one set in another order reads %+v, want %+v", backward, forward)
	}

	prefix := strings.Repeat("t", observability.RequestTargetBytes)
	long1, long2 := RequestTargetsOf(targetsFacts("s", false, [2]string{prefix + "_one", "f"})), RequestTargetsOf(targetsFacts("s", false, [2]string{prefix + "_two", "f"}))
	if long1.Targets[0].TableID != prefix || long2.Targets[0].TableID != prefix ||
		!reflect.DeepEqual(long1.Targets[0].Truncated, []string{"table_id"}) || !reflect.DeepEqual(long2.Targets[0].Truncated, []string{"table_id"}) {
		t.Fatalf("a long table is shown as %+v and %+v, want both cut to %d bytes and marked", long1.Targets, long2.Targets, observability.RequestTargetBytes)
	}
	if long1.Digest == long2.Digest {
		t.Fatalf("two tables alike in their first %d bytes share the key %s", observability.RequestTargetBytes, long1.Digest)
	}

	if joined1, joined2 := RequestTargetsOf(targetsFacts("s", false, [2]string{"ab", "c"})), RequestTargetsOf(targetsFacts("s", false, [2]string{"a", "bc"})); joined1.Digest == joined2.Digest {
		t.Fatalf("names that join alike share the key %s", joined1.Digest)
	}
}

// A name is cut on a character boundary, never inside one.
func TestARequestTargetNameIsCutOnACharacterBoundary(t *testing.T) {
	name := "a" + strings.Repeat("表", observability.RequestTargetBytes)
	cut, truncated := observability.TruncateRequestTargetName(name)
	if !truncated || len(cut) > observability.RequestTargetBytes || !utf8.ValidString(cut) || !strings.HasPrefix(name, cut) ||
		len(cut) < observability.RequestTargetBytes-utf8.UTFMax {
		t.Fatalf("cut to %d bytes, valid %t, truncated %t", len(cut), utf8.ValidString(cut), truncated)
	}
	if short, truncated := observability.TruncateRequestTargetName("system.cpu"); truncated || short != "system.cpu" {
		t.Fatalf("a short name was cut to %q", short)
	}
}

// A target names the keys whose value was cut, and only those: the table
// alone when only the table was long, every key cut in the key order, and
// none when nothing was.
func TestARequestTargetNamesTheKeysItsCutValuesAreUnder(t *testing.T) {
	long := strings.Repeat("x", observability.RequestTargetBytes+1)
	for _, test := range []struct {
		table, field, space string
		want                []string
	}{
		{long, "usage", "bkcc__2", []string{"table_id"}},
		{"system.cpu", long, long, []string{"field_name", "space_uid"}},
		{long, long, long, []string{"table_id", "field_name", "space_uid"}},
		{"system.cpu", "usage", "bkcc__2", nil},
	} {
		got := RequestTargetsOf(QueryPlanFacts{SpaceScope: test.space, QueryList: []QueryClause{{TableID: test.table, FieldName: test.field}}})
		if got == nil || !reflect.DeepEqual(got.Targets[0].Truncated, test.want) {
			t.Errorf("%d, %d, %d bytes: truncated %v, want %v", len(test.table), len(test.field), len(test.space), got.Targets[0].Truncated, test.want)
		}
	}
}
