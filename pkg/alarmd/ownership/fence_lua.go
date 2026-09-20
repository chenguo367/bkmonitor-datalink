// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import "time"

// ContentSwitchMargin is how long after the current lease deadline a pending
// content scope takes effect. It covers the last batch a worker may have
// admitted before it read the pending change, and the clock spread between
// the leader that wrote it, the worker that reads it and the Redis that
// compares it. It is a system constant, not a setting: nothing an operator
// knows would make a better number than the batch bound the worker enforces.
const ContentSwitchMargin = 5 * time.Second

// FenceLua is the one Lua definition of the owner fence, prepended to every
// script that decides whether a writer may act. Six scripts used to carry
// their own copy of the same five comparisons; a rule copied six times is a
// rule that will be changed in five places, and the one left behind is the
// one that then admits a stale owner. decision-016 adds a sixth comparison
// -- the content scope -- and it is added here once.
//
// It defines two functions and nothing else:
//
//	current_content_scope(assignment_key, now_ms)
//	    The content scope the assignment record names now. A pending scope
//	    whose effective time has passed is promoted on the way out -- copied
//	    into content_scope and cleared -- so every script that reads the scope
//	    also settles a change that fell due. The scope is an empty string
//	    until a leader has ever written one.
//
//	fence_refusal(assignment_key, ownership_key, require_assignment,
//	              owner_id, epoch, token, content_scope, now_ms)
//	    nil when the fence holds, otherwise why it does not: NOT_DESIRED when
//	    the assignment names another worker, CONTENT_MOVED when the caller
//	    declared a content scope and the record names a different one, STALE
//	    for a lease that is paused, belongs to someone else, carries another
//	    epoch or token, or has passed its deadline.
//
// The content scope comparison is optional on both sides on purpose. A
// caller that passes an empty scope -- every binary built before this field
// existed -- gets the five comparisons it always had; a record that names no
// scope -- every record written by a leader from before it -- authorizes
// whatever the caller declares, as it always did. The comparison binds only
// once a leader has written a scope and a writer declares one, so the two
// argument shapes and the two record shapes coexist through a rolling
// restart in either order.
//
// The clock is still the caller's now_ms. Judging expiry by the server's own
// TIME is the next step of the same contract and changes every script and
// every fake-clock test at once; it is not smuggled in beside a field.
const FenceLua = `
local function current_content_scope(assignment_key, now_ms)
  local fields = redis.call('HMGET', assignment_key, 'content_scope', 'pending_content_scope', 'effective_at_ms')
  local scope = fields[1] or ''
  local pending = fields[2]
  local effective = tonumber(fields[3] or '0')
  if pending and pending ~= '' and effective > 0 and now_ms >= effective then
    redis.call('HSET', assignment_key, 'content_scope', pending)
    redis.call('HDEL', assignment_key, 'pending_content_scope', 'effective_at_ms')
    scope = pending
  end
  return scope
end
local function fence_refusal(assignment_key, ownership_key, require_assignment, owner_id, epoch, token, content_scope, now_ms)
  if require_assignment == '1' then
    local desired = redis.call('HGET', assignment_key, 'desired_worker_id')
    if not desired or desired ~= owner_id then return 'NOT_DESIRED' end
    if content_scope and content_scope ~= '' then
      local named = current_content_scope(assignment_key, now_ms)
      if named ~= '' and named ~= content_scope then return 'CONTENT_MOVED' end
    end
  end
  if redis.call('HGET', ownership_key, 'execution_disposition') ~= 'ACTIVE' then return 'STALE' end
  if redis.call('HGET', ownership_key, 'owner_id') ~= owner_id or
     redis.call('HGET', ownership_key, 'owner_epoch') ~= epoch or
     redis.call('HGET', ownership_key, 'lease_token') ~= token or
     tonumber(redis.call('HGET', ownership_key, 'deadline_ms') or '0') <= now_ms then return 'STALE' end
  return nil
end
`
