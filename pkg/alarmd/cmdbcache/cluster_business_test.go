// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
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
	index := &Index{}
	fields := make([]string, 0, 2*(MaxClusterBusinesses+1))
	for cluster := 0; cluster < MaxClusterBusinesses; cluster++ {
		fields = append(fields, "BCS-K8S-"+strconv.Itoa(cluster), "7")
	}
	index.clusterBusiness.add(fields, MaxClusterBusinesses)
	if held, _, truncated := index.ClusterBusinesses(); held != MaxClusterBusinesses || truncated != 0 {
		t.Fatalf("at the bound: held %d, truncated %d", held, truncated)
	}
	index.clusterBusiness.add([]string{"BCS-K8S-past", "7"}, MaxClusterBusinesses)
	if held, _, truncated := index.ClusterBusinesses(); held != MaxClusterBusinesses || truncated != 1 {
		t.Fatalf("one past the bound: held %d, truncated %d", held, truncated)
	}
	if _, found := index.LookupClusterBusiness("BCS-K8S-past"); found {
		t.Fatal("a cluster past the bound was held")
	}
}

// failingHashClient is hashClient with a scan that fails for chosen keys,
// as a hash the writer wrote as a string answers WRONGTYPE.
type failingHashClient struct {
	*hashClient
	fail map[string]error
}

func (client *failingHashClient) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	if err, failing := client.fail[key]; failing {
		client.scans = append(client.scans, key)
		return redis.NewScanCmdResult(nil, 0, err)
	}
	return client.hashClient.HScan(ctx, key, cursor, match, count)
}

// The cluster mapping is optional beside the hosts: a mapping that cannot be
// read does not fail the load, the hosts refresh as always, and the failure
// is visible. The store keeps the mapping it held until a read succeeds, so
// one failed read does not turn every mapped event into an unmapped one.
func TestAMappingThatCannotBeReadDoesNotHoldBackTheHosts(t *testing.T) {
	const clusterKey = "bk_monitorv3.ce.cache.cmdb.bcs_cluster_business"
	at := time.Unix(1700000000, 0).UTC()
	good := &hashClient{hashes: map[string][]string{
		"bk_monitorv3.ce.cache.cmdb.host": {"10.0.0.7|0", disabledByAddressHost},
		clusterKey:                        {"BCS-K8S-00001", "11"},
	}}
	reader, _ := NewReader(good, "bk_monitorv3.ce")
	store, err := NewStore(reader, StoreOptions{RefreshInterval: time.Minute, MaxAge: time.Hour, Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	wrongType := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	failing := &failingHashClient{
		hashClient: &hashClient{hashes: map[string][]string{
			"bk_monitorv3.ce.cache.cmdb.host": {"10.0.0.7|0", disabledByAddressHost, "10.0.0.8|0", disabledByAddressHost},
			clusterKey:                        {"BCS-K8S-00002", "12"},
		}},
		fail: map[string]error{clusterKey: wrongType},
	}
	reader.client = failing
	if err := store.Refresh(context.Background()); err != nil {
		t.Fatalf("a mapping that cannot be read failed the refresh: %v", err)
	}
	health := store.Health()
	if health.Refreshes != 2 || health.ConsecutiveErrors != 0 || health.Degraded {
		t.Fatalf("health = %+v, want the second refresh taken, no store failure, nothing degraded", health)
	}
	if _, found := store.Current().Lookup("10.0.0.8|0"); !found {
		t.Fatal("the host index of the refresh whose mapping could not be read was not taken")
	}
	if !health.ClusterBusinessesReadFailed || health.ClusterBusinesses != 1 {
		t.Fatalf("health = %+v, want the read failure visible and the held mapping carried", health)
	}
	lookup := NewHostBusinessLookup(store)
	if business, found := lookup.LookupClusterBusiness("BCS-K8S-00001"); !found || business != "11" {
		t.Fatalf("carried cluster = %q, %v; want the previous mapping's 11", business, found)
	}
	if _, found := lookup.LookupClusterBusiness("BCS-K8S-00002"); found {
		t.Fatal("a cluster from the hash that could not be read was held")
	}

	reader.client = good
	if err := store.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health := store.Health(); health.ClusterBusinessesReadFailed {
		t.Fatalf("health = %+v, want the flag down once a read succeeds", health)
	}
}

// A first load whose mapping cannot be read has nothing to carry: no cluster
// is mapped, the hosts load, and the failure is visible.
func TestAFirstLoadWithAnUnreadableMappingMapsNoCluster(t *testing.T) {
	const clusterKey = "bk_monitorv3.ce.cache.cmdb.bcs_cluster_business"
	client := &failingHashClient{
		hashClient: &hashClient{hashes: map[string][]string{"bk_monitorv3.ce.cache.cmdb.host": {"10.0.0.7|0", disabledByAddressHost}}},
		fail:       map[string]error{clusterKey: errors.New("i/o timeout")},
	}
	reader, _ := NewReader(client, "bk_monitorv3.ce")
	index, err := reader.Load(context.Background(), time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if held, _, _ := index.ClusterBusinesses(); held != 0 || !index.ClusterBusinessReadFailed() || index.Hosts() != 1 {
		t.Fatalf("held %d, read failed %v, hosts %d", held, index.ClusterBusinessReadFailed(), index.Hosts())
	}
}
