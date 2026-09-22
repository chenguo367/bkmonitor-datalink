// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package cliauth

// Each script judges and writes deadlines against Redis TIME. The grant and
// session keys share the deployment hash tag, including on Redis Cluster.
const issueScript = `
redis.replicate_commands()
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local record = cjson.decode(ARGV[1])
record.expires_at_ms = now + tonumber(ARGV[2])
local encoded = cjson.encode(record)
if not redis.call('SET', KEYS[1], encoded, 'PX', ARGV[2], 'NX') then
  return {2}
end
return {1, encoded, 0}
`

const exchangeScript = `
redis.replicate_commands()
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local raw = redis.call('GET', KEYS[1])
if not raw then return {0} end
local record = cjson.decode(raw)
if record.environment_id ~= ARGV[1] or record.scope ~= ARGV[3] or record.expires_at_ms <= now then
  return {0}
end
record.session_id = ARGV[2]
record.expires_at_ms = now + tonumber(ARGV[4])
local encoded = cjson.encode(record)
-- A failed session creation must leave the grant available. Consumption only
-- follows a successful SET; both operations run in the same atomic script.
if not redis.call('SET', KEYS[2], encoded, 'PX', ARGV[4], 'NX') then
  return {2}
end
redis.call('DEL', KEYS[1])
return {1, encoded, 0}
`

const sessionScript = `
redis.replicate_commands()
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local raw = redis.call('GET', KEYS[1])
if not raw then return {0} end
local record = cjson.decode(raw)
if record.environment_id ~= ARGV[1] or record.scope ~= ARGV[4] or record.expires_at_ms <= now then
  return {0}
end
if ARGV[2] ~= '' and record.session_id ~= ARGV[2] then return {0} end
local renewed = 0
if ARGV[3] == 'delete' then
  redis.call('DEL', KEYS[1])
elseif ARGV[3] == 'renew' and record.expires_at_ms - now <= tonumber(ARGV[6]) then
  record.expires_at_ms = now + tonumber(ARGV[5])
  raw = cjson.encode(record)
  redis.call('SET', KEYS[1], raw, 'PX', ARGV[5], 'XX')
  renewed = 1
end
return {1, raw, renewed}
`
