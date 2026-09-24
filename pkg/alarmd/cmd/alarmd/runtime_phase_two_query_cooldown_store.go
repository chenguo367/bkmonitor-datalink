// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// queryCooldownRecordTTL is how long a pool record outlives its last write.
// An extension rewrites it, so a Query Group in the pool keeps its record for
// as long as it stays; one that left keeps its last exit for this long, well
// past the re-entry window.
const queryCooldownRecordTTL = 7 * 24 * time.Hour

// saveQueryCooldownScript writes the record unless one written by a later
// owner is already there: an owner that has been replaced cannot write over
// its successor's pool state. The epoch is read from the stored record, so
// the check needs no second key.
const saveQueryCooldownScript = `
local current = redis.call('GET', KEYS[1])
if current then
  local ok, decoded = pcall(cjson.decode, current)
  if ok and type(decoded) == 'table' and tonumber(decoded['owner_epoch']) and
     tonumber(decoded['owner_epoch']) > tonumber(ARGV[2]) then
    return 0
  end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1
`

// errQueryCooldownSuperseded is a save a later owner's record refused.
var errQueryCooldownSuperseded = errors.New("alarmd: query cooldown record belongs to a later owner")

// redisQueryCooldownStore keeps each Query Group's pool record under
// {prefix}:{query group} in the runtime store.
type redisQueryCooldownStore struct {
	client redis.UniversalClient
	prefix string
}

// newRedisQueryCooldownStore is the store, or none -- a nil interface, not a
// nil pointer inside one -- when there is no runtime store to keep it in.
func newRedisQueryCooldownStore(client redis.UniversalClient, prefix string) scheduler.QueryCooldownStore {
	if client == nil || prefix == "" {
		return nil
	}
	return &redisQueryCooldownStore{client: client, prefix: prefix}
}

func (store *redisQueryCooldownStore) key(queryGroup execution.QueryGroupIdentity) string {
	return store.prefix + ":" + string(queryGroup)
}

// LoadQueryCooldown reads the record. Absent is no record; so is one that
// does not decode, which the Runner treats the same way: outside the pool.
func (store *redisQueryCooldownStore) LoadQueryCooldown(ctx context.Context, queryGroup execution.QueryGroupIdentity) (scheduler.QueryCooldownRecord, bool, error) {
	raw, err := store.client.Get(ctx, store.key(queryGroup)).Bytes()
	if errors.Is(err, redis.Nil) {
		return scheduler.QueryCooldownRecord{}, false, nil
	}
	if err != nil {
		return scheduler.QueryCooldownRecord{}, false, err
	}
	var record scheduler.QueryCooldownRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.QueryGroup != queryGroup {
		return scheduler.QueryCooldownRecord{}, false, errors.New("alarmd: query cooldown record does not decode")
	}
	return record, true, nil
}

// SaveQueryCooldown writes the record under the owner's epoch.
func (store *redisQueryCooldownStore) SaveQueryCooldown(ctx context.Context, fence execution.OwnerFence, record scheduler.QueryCooldownRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	written, err := store.client.Eval(ctx, saveQueryCooldownScript, []string{store.key(record.QueryGroup)},
		payload, strconv.FormatUint(fence.OwnerEpoch, 10), queryCooldownRecordTTL.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if written != 1 {
		return errQueryCooldownSuperseded
	}
	return nil
}
