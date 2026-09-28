// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package obevidence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// One CMDB record at a time, from the hash the index loads whole: a host by
// its id or by its "ip|cloud" field - the writer keys each host both ways -
// and a service instance by its id. The record comes back as written, with
// what alarmd decodes from it or why it cannot, which is what a load that
// skips it never says. A key that is not there and a record the hash does
// not hold are both missing, and say which; a record past the document limit
// is not read at all.
func TestACMDBRecordIsReadAsWrittenAndAsAlarmdDecodesIt(t *testing.T) {
	ctx := context.Background()
	client := redisForTest(t)
	const prefix = "bk_test.ee"
	hosts, instances := prefix+".cache.cmdb.host", prefix+".cache.cmdb.service_instance"
	record := `{"bk_host_id":101,"bk_host_innerip":"192.0.2.10","bk_cloud_id":0,"bk_biz_id":2,"bk_state":"运营中","topo_link":{"module|7":[]}}`
	if err := client.HSet(ctx, hosts, "101", record, "192.0.2.10|0", record, "102", "{not json", "104", "").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, hosts, "103", strings.Repeat("x", MaxDocumentBytes+1)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, instances, "501", `{"service_instance_id":501,"bk_host_id":101,"ip":"192.0.2.10","bk_cloud_id":0}`).Err(); err != nil {
		t.Fatal(err)
	}
	service := New(Options{CMDBCache: RedisBinding{Client: client, Location: Location{Role: "cmdb_cache", Prefix: prefix}}})
	host := func(field string) Result {
		return service.Store(ctx, StoreRequest{Family: FamilyCMDBHost, Host: field})
	}

	for _, field := range []string{"101", "192.0.2.10|0"} {
		r := host(field)
		got, _ := r.Value.(CMDBRecord)
		decoded, _ := got.Decoded.(map[string]any)
		if r.Status != "ok" || r.Location.Key != hosts || got.Field != field || decoded["host_id"] != "101" || decoded["ip"] != "192.0.2.10" ||
			r.Limits.Commands != 7 || r.Limits.Bytes != len(record) {
			t.Fatalf("host %s = %+v value %+v, want the record, decoded, in 7 commands", field, r, got)
		}
		if raw, ok := got.Record.(json.RawMessage); !ok || string(raw) != record {
			t.Fatalf("host %s record = %v, want the payload as written", field, got.Record)
		}
	}
	if r := host("999"); r.Status != "missing" || r.Reason != "field_absent" || !r.Complete || r.Limits.Commands != 6 {
		t.Fatalf("a host the hash does not hold = %+v, want missing/field_absent without reading a value", r)
	}
	r := host("102")
	if got, _ := r.Value.(CMDBRecord); r.Status != "ok" || got.DecodeError == "" || got.Decoded != nil || got.Record != "{not json" {
		t.Fatalf("a record alarmd cannot decode = %+v value %+v, want it as text with the decode error", r, r.Value)
	}
	if r := host("103"); r.Status != "budget_exceeded" || r.Reason != "document_bytes" || r.Value != nil || r.Limits.Commands != 6 {
		t.Fatalf("a record past the document limit = %+v, want refused unread", r)
	}
	if r := host("104"); r.Status != "empty" || !r.Complete {
		t.Fatalf("an empty record = %+v, want empty", r)
	}

	instance := service.Store(ctx, StoreRequest{Family: FamilyCMDBServiceInstance, ServiceInstance: "501"})
	if got, _ := instance.Value.(CMDBRecord); instance.Status != "ok" || instance.Location.Key != instances || got.Decoded.(map[string]any)["host_id"] != "101" {
		t.Fatalf("service instance 501 = %+v value %+v", instance, instance.Value)
	}

	// The whole cache gone reads apart from one record gone.
	elsewhere := New(Options{CMDBCache: RedisBinding{Client: client, Location: Location{Role: "cmdb_cache", Prefix: "bk_other.ee"}}})
	if r := elsewhere.Store(ctx, StoreRequest{Family: FamilyCMDBHost, Host: "101"}); r.Status != "missing" || r.Reason != "key_absent" {
		t.Fatalf("no host hash at all = %+v, want missing/key_absent", r)
	}
	if err := client.Set(ctx, "bk_wrong.ee.cache.cmdb.host", "a string", 0).Err(); err != nil {
		t.Fatal(err)
	}
	wrong := New(Options{CMDBCache: RedisBinding{Client: client, Location: Location{Role: "cmdb_cache", Prefix: "bk_wrong.ee"}}})
	if r := wrong.Store(ctx, StoreRequest{Family: FamilyCMDBHost, Host: "101"}); r.Status != "wrong_type" || r.Type != "string" {
		t.Fatalf("a host key that is not a hash = %+v, want wrong_type", r)
	}

	for name, request := range map[string]StoreRequest{
		"host not in either form":       {Family: FamilyCMDBHost, Host: "not-a-host"},
		"address without a cloud":       {Family: FamilyCMDBHost, Host: "192.0.2.10"},
		"host with a strategy":          {Family: FamilyCMDBHost, Host: "101", StrategyID: "1"},
		"host asked as a service":       {Family: FamilyCMDBHost, ServiceInstance: "501"},
		"service instance of zero":      {Family: FamilyCMDBServiceInstance, ServiceInstance: "0"},
		"service instance with a host":  {Family: FamilyCMDBServiceInstance, ServiceInstance: "501", Host: "101"},
		"service instance not a number": {Family: FamilyCMDBServiceInstance, ServiceInstance: "abc"},
	} {
		if r := service.Store(ctx, request); r.Status != "invalid_input" {
			t.Errorf("%s = %+v, want invalid_input", name, r)
		}
	}
	unwired := New(Options{})
	if r := unwired.Store(ctx, StoreRequest{Family: FamilyCMDBHost, Host: "101"}); r.Status != "not_configured" {
		t.Fatalf("no CMDB binding = %+v, want not_configured", r)
	}
}
