// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package redisbatch

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
)

// HashFields reads one field of each hash on a real Redis: the field's value
// where the hash has it, Missing where the hash has not or there is no hash,
// and the key's own error - not the batch's - for a key that is no hash.
func TestHashFieldsReadsOneFieldOfEachHash(t *testing.T) {
	server := redistest.Start(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	client.HSet(ctx, "with", "ledger", "value", "other", "x")
	client.HSet(ctx, "without", "other", "x")
	client.Set(ctx, "string", "x", 0)
	values, err := HashFields(ctx, client, []string{"with", "without", "gone", "string"}, "ledger")
	if err != nil || len(values) != 4 {
		t.Fatalf("HashFields() = %d values, %v", len(values), err)
	}
	if string(values[0].Raw) != "value" || values[0].Missing || values[0].Err != nil {
		t.Fatalf("the hash with the field: %+v, want its value", values[0])
	}
	for index, name := range []string{"the hash without the field", "no hash"} {
		if value := values[1+index]; !value.Missing || value.Err != nil {
			t.Fatalf("%s: %+v, want it missing", name, value)
		}
	}
	if code, answered := AnsweredCode(values[3].Err); !answered || code != "WRONGTYPE" {
		t.Fatalf("a string key: %+v, want its own WRONGTYPE", values[3])
	}
}
