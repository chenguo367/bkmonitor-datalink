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
	"fmt"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A Slot that ended without a finished result has no usage, and its line
// carries none: a row of zeros beside a share refusal read as a Slot that
// held nothing when it was refused. A reported usage, even one that took
// nothing, still carries its limits and is on the line.
func TestASlotWithoutUsageCarriesNoUsageFacts(t *testing.T) {
	if facts := slotBudgetUsageFacts(execution.SlotBudgetUsage{}); facts != nil {
		t.Fatalf("an unreported usage became facts %+v", facts)
	}
	facts := slotBudgetUsageFacts(execution.SlotBudgetUsage{RetainedBytesLimit: 1 << 30, RetainedShareBytes: 512 << 20})
	if facts == nil || facts.RetainedBytes != 0 || facts.RetainedShareBytes != 512<<20 {
		t.Fatalf("a reported usage that took nothing became %+v", facts)
	}
}

// Every field of a Slot's usage reaches its line under the same name with
// its value: each is given a value of its own, and the facts read back hold
// them all. A field added to the usage and left out of the conversion reaches
// the line as nothing at all, which reads as the Slot having none of it.
func TestEveryFieldOfASlotsUsageReachesItsFacts(t *testing.T) {
	var usage execution.SlotBudgetUsage
	value := reflect.ValueOf(&usage).Elem()
	next := uint64(0)
	var fill func(field reflect.Value)
	fill = func(field reflect.Value) {
		switch field.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			next++
			field.SetUint(next)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			next++
			field.SetInt(int64(next))
		case reflect.Bool:
			field.SetBool(true)
		case reflect.String:
			next++
			field.SetString(fmt.Sprintf("v%d", next))
		case reflect.Array:
			for index := 0; index < field.Len(); index++ {
				fill(field.Index(index))
			}
		case reflect.Struct:
			for index := 0; index < field.NumField(); index++ {
				if field.Type().Field(index).IsExported() {
					fill(field.Field(index))
				}
			}
		}
	}
	fill(value)
	facts := slotBudgetUsageFacts(usage)
	if facts == nil {
		t.Fatal("a usage with every field set became no facts")
	}
	got := reflect.ValueOf(facts).Elem()
	for index := 0; index < value.NumField(); index++ {
		name := value.Type().Field(index).Name
		read := got.FieldByName(name)
		if !read.IsValid() {
			t.Errorf("the usage's %s has no field of its name on the Slot's facts", name)
			continue
		}
		if fmt.Sprint(read.Interface()) != fmt.Sprint(value.Field(index).Interface()) {
			t.Errorf("the usage's %s is %v, its facts' %v", name, value.Field(index).Interface(), read.Interface())
		}
	}
}
