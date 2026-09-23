package obevidence

import (
	"context"
	"strings"
	"testing"

	"github.com/go-redis/redis/v8"
)

// One entry per server however many roles share it, with the memory ceiling,
// its policy, the usage and the evictions as the server reports them; only
// the allowed fields leave; a server that does not answer is named, never
// read as empty.
func TestInfoReadsEachServerOnceAndOnlyTheAllowedFields(t *testing.T) {
	client := redisForTest(t)
	ctx := context.Background()
	if err := client.ConfigSet(ctx, "maxmemory", "104857600").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Fatal(err)
	}
	client.Set(ctx, "k", "v", 0)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	runtime := binding(client, "runtime", "p")
	source := binding(client, "strategy_cache", "q")
	source.Location.DB = 8
	gone := RedisBinding{Client: dead, Location: Location{Role: "cmdb_cache", Address: "elsewhere", Mode: "standalone", DB: 0}}
	got := New(Options{Published: runtime, SourceStrategy: source, CMDBCache: gone}).Info(ctx)
	if len(got.Servers) != 2 || got.Complete {
		t.Fatalf("servers %+v complete %v", got.Servers, got.Complete)
	}
	shared := got.Servers[0]
	if strings.Join(shared.Roles, ",") != "runtime,strategy_cache" || len(shared.DBs) != 2 || shared.DBs[0] != 5 || shared.DBs[1] != 8 || shared.Status != "ok" {
		t.Fatalf("the shared server %+v", shared)
	}
	if shared.Fields["maxmemory"] != "104857600" || shared.Fields["maxmemory_policy"] != "allkeys-lru" ||
		shared.Fields["evicted_keys"] != "0" || shared.Fields["used_memory"] == "" || shared.Fields["role"] != "master" {
		t.Fatalf("fields %v", shared.Fields)
	}
	for name := range shared.Fields {
		allowed := false
		for _, field := range InfoFields {
			allowed = allowed || field == name
		}
		if !allowed {
			t.Errorf("a field outside the list left: %s", name)
		}
	}
	if !strings.HasPrefix(shared.Keyspace["db5"], "keys=1") || len(shared.Keyspace) != 1 {
		t.Errorf("keyspace of the roles' dbs only: %v", shared.Keyspace)
	}
	if down := got.Servers[1]; down.Status != "dependency_unavailable" || down.Fields != nil || down.ReadAt == nil {
		t.Errorf("an unanswering server %+v", down)
	}
}

func TestParseInfoNamesMissingFields(t *testing.T) {
	fields, keyspace, missing := parseInfo("# Memory\r\nused_memory:10\r\nclient_list:secret\r\ndb3:keys=2\r\n", []int{3})
	if fields["used_memory"] != "10" || fields["client_list"] != "" || keyspace["db3"] != "keys=2" || len(missing) != len(InfoFields)-1 {
		t.Fatalf("%v %v %v", fields, keyspace, missing)
	}
}
