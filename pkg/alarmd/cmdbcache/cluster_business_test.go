// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// The BCS cluster -> business mapping hangs off the platform prefix beside
// the host hash and is read into the same snapshot. Only a positive business
// is held; every other field is counted as refused, and the store's health
// and the lookup a global business Plan's attribution asks both read the
// held snapshot.
func TestLoadReadsTheClusterBusinessMappingIntoTheSameSnapshot(t *testing.T) {
	client := &hashClient{hashes: map[string][]string{
		"bk_monitorv3.ce.cache.cmdb.host": {"10.0.0.7|0", disabledByAddressHost},
		"bk_monitorv3.ce.cache.cmdb.bcs_cluster_business": {
			"BCS-K8S-00001", "11", "BCS-K8S-00002", " 12 ",
			"BCS-K8S-00003", "0", "BCS-K8S-00004", "-3", "BCS-K8S-00005", "biz", " ", "13",
		},
	}}
	reader, err := NewReader(client, "bk_monitorv3.ce")
	if err != nil {
		t.Fatal(err)
	}
	index, err := reader.Load(context.Background(), time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	held, refused, truncated := index.ClusterBusinesses()
	if held != 2 || refused != 4 || truncated != 0 {
		t.Fatalf("held %d, refused %d, truncated %d; want 2 held and 4 refused", held, refused, truncated)
	}
	store := &Store{index: index, now: time.Now, maxAge: time.Hour, interval: time.Minute}
	lookup := NewHostBusinessLookup(store)
	for cluster, want := range map[string]string{"BCS-K8S-00001": "11", "BCS-K8S-00002": "12"} {
		if business, found := lookup.LookupClusterBusiness(cluster); !found || business != want {
			t.Fatalf("cluster %s = %q, %v; want %s", cluster, business, found, want)
		}
	}
	for _, cluster := range []string{"BCS-K8S-00003", "BCS-K8S-00004", "BCS-K8S-00005", "BCS-K8S-09999", ""} {
		if business, found := lookup.LookupClusterBusiness(cluster); found {
			t.Fatalf("cluster %q resolved to %q", cluster, business)
		}
	}
	if health := store.Health(); health.ClusterBusinesses != 2 || health.ClusterBusinessesRefused != 4 {
		t.Fatalf("health = %+v", health)
	}
}

// A writer that does not publish the mapping yet leaves the hash absent.
// That is no error and no degradation: the hosts load as before and no
// cluster is mapped.
func TestAnAbsentClusterMappingMapsNoCluster(t *testing.T) {
	client := &hashClient{hashes: map[string][]string{
		"bk_monitorv3.ce.cache.cmdb.host": {"10.0.0.7|0", disabledByAddressHost},
	}}
	reader, _ := NewReader(client, "bk_monitorv3.ce")
	at := time.Unix(1700000000, 0).UTC()
	index, err := reader.Load(context.Background(), at)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if held, refused, truncated := index.ClusterBusinesses(); held != 0 || refused != 0 || truncated != 0 {
		t.Fatalf("held %d, refused %d, truncated %d", held, refused, truncated)
	}
	store := &Store{index: index, now: func() time.Time { return at }, maxAge: time.Hour, interval: time.Minute}
	if health := store.Health(); health.Degraded || health.Hosts != 1 {
		t.Fatalf("health = %+v, want the hosts loaded and nothing degraded", health)
	}
	if _, found := NewHostBusinessLookup(store).LookupClusterBusiness("BCS-K8S-00001"); found {
		t.Fatal("a cluster resolved with no mapping published")
	}
}

// The mapping is bounded: past MaxClusterBusinesses a cluster is counted as
// truncated and not held, at the bound and one past it.
func TestTheClusterMappingIsBounded(t *testing.T) {
	builder := newIndexBuilder(time.Unix(1700000000, 0).UTC())
	fields := make([]string, 0, 2*(MaxClusterBusinesses+1))
	for cluster := 0; cluster < MaxClusterBusinesses; cluster++ {
		fields = append(fields, "BCS-K8S-"+strconv.Itoa(cluster), "7")
	}
	builder.addClusterBusinessFields(fields)
	if held, _, truncated := builder.index.ClusterBusinesses(); held != MaxClusterBusinesses || truncated != 0 {
		t.Fatalf("at the bound: held %d, truncated %d", held, truncated)
	}
	builder.addClusterBusinessFields([]string{"BCS-K8S-past", "7"})
	if held, _, truncated := builder.index.ClusterBusinesses(); held != MaxClusterBusinesses || truncated != 1 {
		t.Fatalf("one past the bound: held %d, truncated %d", held, truncated)
	}
	if _, found := builder.index.LookupClusterBusiness("BCS-K8S-past"); found {
		t.Fatal("a cluster past the bound was held")
	}
}
