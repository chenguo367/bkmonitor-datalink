package obevidence

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"
)

// InfoFields is every INFO field this read returns, closed: the memory
// ceiling and what the server does at it, what it holds, and what it has
// dropped. Nothing else of INFO is carried, so no client list, address or
// command line leaves the process.
var InfoFields = []string{
	"redis_version", "role", "maxmemory", "maxmemory_policy", "used_memory", "used_memory_peak",
	"used_memory_rss", "evicted_keys", "expired_keys", "keyspace_hits", "keyspace_misses",
}

// ServerInfo is one Redis server as alarmd reaches it: which of alarmd's
// roles read it, where, and the INFO fields above. Status is ok, or
// dependency_unavailable with no fields: a server that did not answer is
// never read as one with nothing in it.
type ServerInfo struct {
	Roles    []string          `json:"roles"`
	Address  string            `json:"address"`
	Mode     string            `json:"mode,omitempty"`
	DBs      []int             `json:"dbs"`
	Status   string            `json:"status"`
	ReadAt   *time.Time        `json:"read_at,omitempty"`
	Fields   map[string]string `json:"fields,omitempty"`
	Missing  []string          `json:"missing,omitempty"`
	Keyspace map[string]string `json:"keyspace,omitempty"`
}

// InfoResult is every Redis server alarmd is configured with, one entry per
// server however many roles share it.
type InfoResult struct {
	Servers  []ServerInfo `json:"servers"`
	Complete bool         `json:"complete"`
}

func (service *Service) bindings() []RedisBinding {
	if service == nil {
		return nil
	}
	o := service.options
	return []RedisBinding{o.Published, o.SourceStrategy, o.CMDBCache, o.TargetGroup, o.DynamicConfig}
}

// Info reads INFO from each server alarmd's roles are bound to.
func (service *Service) Info(ctx context.Context) InfoResult {
	type server struct {
		binding RedisBinding
		info    ServerInfo
	}
	byKey := map[string]*server{}
	var order []string
	for _, binding := range service.bindings() {
		if binding.Client == nil {
			continue
		}
		key := binding.Location.Mode + "|" + binding.Location.Address
		s, ok := byKey[key]
		if !ok {
			s = &server{binding: binding, info: ServerInfo{Address: binding.Location.Address, Mode: binding.Location.Mode}}
			byKey[key] = s
			order = append(order, key)
		}
		s.info.Roles = append(s.info.Roles, binding.Location.Role)
		s.info.DBs = appendUnique(s.info.DBs, binding.Location.DB)
	}
	result := InfoResult{Servers: []ServerInfo{}, Complete: true}
	for _, key := range order {
		s := byKey[key]
		read, cancel := context.WithTimeout(ctx, ReadTimeout)
		// One section per INFO call in this client, and "all" on every server
		// version; the fields are filtered here, so the rest never leaves.
		raw, err := s.binding.Client.Info(read, "all").Result()
		cancel()
		at := time.Now().UTC()
		s.info.ReadAt = &at
		if err != nil {
			s.info.Status = "dependency_unavailable"
			result.Complete = false
		} else {
			s.info.Status = "ok"
			s.info.Fields, s.info.Keyspace, s.info.Missing = parseInfo(raw, s.info.DBs)
		}
		sort.Strings(s.info.Roles)
		result.Servers = append(result.Servers, s.info)
	}
	return result
}

func appendUnique(values []int, value int) []int {
	for _, v := range values {
		if v == value {
			return values
		}
	}
	values = append(values, value)
	sort.Ints(values)
	return values
}

// parseInfo keeps the allowed fields, and the keyspace line of each db a
// role reads. A field the server did not report is named as missing, not
// written as zero.
func parseInfo(raw string, dbs []int) (map[string]string, map[string]string, []string) {
	allowed := make(map[string]bool, len(InfoFields))
	for _, name := range InfoFields {
		allowed[name] = true
	}
	wantDB := map[string]bool{}
	for _, db := range dbs {
		wantDB["db"+strconv.Itoa(db)] = true
	}
	fields, keyspace := map[string]string{}, map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || strings.HasPrefix(name, "#") {
			continue
		}
		switch {
		case allowed[name]:
			fields[name] = value
		case wantDB[name]:
			keyspace[name] = value
		}
	}
	var missing []string
	for _, name := range InfoFields {
		if _, ok := fields[name]; !ok {
			missing = append(missing, name)
		}
	}
	return fields, keyspace, missing
}
