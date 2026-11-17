// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// segmentWriteScript writes one chunk of a Plan's segments under the owner
// fence, refused whole by a stale one: the pages it carries, and, on a
// Plan's last chunk, the mark, a renewal of every page it does not carry -
// answering how many of those were already gone - and the removal of the
// pages past its count. Unconditional writes: under one fence the owner is
// the segments' only writer, and a read from Redis compares what it finds
// with what the owner cached (drift).
//
// ARGV[1] fenced ('1'/'0'); ARGV[2] the pages written; ARGV[3] whether the
// mark is; ARGV[4] the pages kept; ARGV[5] the pages removed; ARGV[6] the TTL
// in milliseconds; ARGV[7] the departing members' State keys renewed to it
// (014 B3 3.3 step 3), in chunks of their own before the mark's, at most
// segment.RenewKeys a chunk. Fenced, KEYS[1..2] are the assignment and ownership
// hashes and ARGV[8..12] the fence's. Then the pages' keys and values, the
// mark's key and value, the kept pages' keys, the removed pages' keys, the
// departing members' keys. The answer counts the kept pages already gone
// and, apart, the departing members' keys already gone.
const segmentWriteScript = ownership.FenceLua + `
local fenced = ARGV[1] == '1'
local pages = tonumber(ARGV[2])
local marked = ARGV[3] == '1'
local kept = tonumber(ARGV[4])
local removed = tonumber(ARGV[5])
local ttl = ARGV[6]
local departing = tonumber(ARGV[7])
local key, arg = 1, 8
if fenced then
  local refusal = fence_refusal(KEYS[1], KEYS[2], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], redis_now_ms())
  if refusal == 'CONTENT_MOVED' then return {'CONTENT_MOVED'} end
  if refusal then return {'STALE_OWNER'} end
  key, arg = 3, 13
end
for index = 0, pages - 1 do
  redis.call('PSETEX', KEYS[key + index], ttl, ARGV[arg + index])
end
key, arg = key + pages, arg + pages
local missing = 0
if marked then
  redis.call('PSETEX', KEYS[key], ttl, ARGV[arg])
  key = key + 1
  for index = 0, kept - 1 do
    if redis.call('PEXPIRE', KEYS[key + index], ttl) == 0 then missing = missing + 1 end
  end
  key = key + kept
  for index = 0, removed - 1 do
    redis.call('DEL', KEYS[key + index])
  end
  key = key + removed
end
local gone = 0
for index = 0, departing - 1 do
  if redis.call('PEXPIRE', KEYS[key + index], ttl) == 0 then gone = gone + 1 end
end
return {'APPLIED', missing, gone}
`

// segmentWriteSHA addresses segmentWriteScript by its SHA-1.
var segmentWriteSHA = func() string {
	sum := sha1.Sum([]byte(segmentWriteScript))
	return hex.EncodeToString(sum[:])
}()

// segmentPipelineBytes bounds the bytes one round trip of segment writes
// carries: what eight fenced write scripts would. A Plan's chunks go in one.
const segmentPipelineBytes = 8 * fencedScriptBytes

func segmentWriteArguments(guard *FenceGuard, ttl time.Duration, chunk segmentWriteChunk) ([]string, []interface{}) {
	keys := make([]string, 0, 3+len(chunk.pages)+len(chunk.keep)+len(chunk.remove)+len(chunk.departing))
	args := make([]interface{}, 0, 14+len(chunk.pages))
	args = append(args, boolArg(guard != nil), len(chunk.pages), boolArg(chunk.mark != nil), len(chunk.keep),
		len(chunk.remove), ttl.Milliseconds(), len(chunk.departing))
	if guard != nil {
		keys = append(keys, guard.Keys.AssignmentKey, guard.Keys.OwnershipKey)
		args = append(args, boolArg(guard.Keys.RequireAssignment), guard.OwnerID,
			strconv.FormatUint(guard.OwnerEpoch, 10), guard.LeaseToken, guard.ContentScope)
	}
	for _, page := range chunk.pages {
		keys = append(keys, page.Key)
		args = append(args, page.Value)
	}
	if chunk.mark != nil {
		keys = append(keys, chunk.mark.Key)
		args = append(args, chunk.mark.Value)
		keys = append(keys, chunk.keep...)
		keys = append(keys, chunk.remove...)
	}
	keys = append(keys, chunk.departing...)
	return keys, args
}

// WriteSegments sends the Plans' chunks in round trips of at most
// segmentPipelineBytes, a Plan never split across two, each chunk by its
// SHA-1. A Plan any of whose chunks the server had no script for is sent
// again whole with the script's text, so its mark lands after its pages
// whatever another client loaded in between.
func (backend *RedisBackend) WriteSegments(ctx context.Context, guard *FenceGuard, plans []segmentPlanWrite) []segmentPlanOutcome {
	outcomes := make([]segmentPlanOutcome, len(plans))
	if backend == nil || backend.client == nil {
		for index := range outcomes {
			outcomes[index].result = segment.WriteFailed
		}
		return outcomes
	}
	for start := 0; start < len(plans); {
		end, bytes := start, 0
		for end < len(plans) {
			size := 0
			for _, chunk := range plans[end].chunks {
				size += chunk.bytes
			}
			if end > start && bytes+size > segmentPipelineBytes {
				break
			}
			bytes += size
			end++
		}
		backend.writeSegmentWave(ctx, guard, plans[start:end], outcomes[start:end])
		start = end
	}
	return outcomes
}

func (backend *RedisBackend) writeSegmentWave(ctx context.Context, guard *FenceGuard, plans []segmentPlanWrite, outcomes []segmentPlanOutcome) {
	send := func(plans []segmentPlanWrite, byDigest bool) ([]redis.Cmder, error) {
		cmds, err := backend.client.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
			for _, plan := range plans {
				for _, chunk := range plan.chunks {
					keys, args := segmentWriteArguments(guard, plan.ttl, chunk)
					if byDigest {
						pipeline.EvalSha(ctx, segmentWriteSHA, keys, args...)
						continue
					}
					pipeline.Eval(ctx, segmentWriteScript, keys, args...)
				}
			}
			return nil
		})
		if err != nil && !isRedisReplyError(err) {
			return nil, err
		}
		return cmds, nil
	}
	chunkCount := 0
	for _, plan := range plans {
		chunkCount += len(plan.chunks)
	}
	cmds, err := send(plans, true)
	if err != nil || len(cmds) != chunkCount {
		for index := range outcomes {
			outcomes[index].result = segment.WriteUnknown
		}
		return
	}
	var again []int
	next := 0
	for index, plan := range plans {
		replies := cmds[next : next+len(plan.chunks)]
		next += len(plan.chunks)
		uncached := false
		for _, reply := range replies {
			uncached = uncached || noScriptReply(reply.Err())
		}
		if uncached {
			again = append(again, index)
			continue
		}
		outcomes[index] = segmentPlanOutcomeOf(replies)
	}
	if len(again) == 0 {
		return
	}
	resend := make([]segmentPlanWrite, len(again))
	resendChunks := 0
	for position, index := range again {
		resend[position] = plans[index]
		resendChunks += len(plans[index].chunks)
	}
	replies, err := send(resend, false)
	if err != nil || len(replies) != resendChunks {
		for _, index := range again {
			outcomes[index].result = segment.WriteUnknown
		}
		return
	}
	next = 0
	for _, index := range again {
		count := len(plans[index].chunks)
		outcomes[index] = segmentPlanOutcomeOf(replies[next : next+count])
		next += count
	}
}

// segmentPlanOutcomeOf is a Plan's result from its chunks' replies, in
// order: the first that did not apply, and the pages its mark found gone.
func segmentPlanOutcomeOf(replies []redis.Cmder) segmentPlanOutcome {
	outcome := segmentPlanOutcome{result: segment.WriteApplied}
	for _, reply := range replies {
		result, missing := segmentChunkResult(reply)
		if result != segment.WriteApplied {
			return segmentPlanOutcome{result: result}
		}
		outcome.missing += missing
		outcome.departedGone += segmentDepartedGone(reply)
	}
	return outcome
}

// segmentDepartedGone is the departing members' keys an applied chunk found
// already gone.
func segmentDepartedGone(cmd redis.Cmder) int {
	typed, ok := cmd.(*redis.Cmd)
	if !ok {
		return 0
	}
	items, _ := typed.Val().([]interface{})
	if len(items) > 2 {
		if gone, ok := items[2].(int64); ok {
			return int(gone)
		}
	}
	return 0
}

func segmentChunkResult(cmd redis.Cmder) (string, int) {
	typed, ok := cmd.(*redis.Cmd)
	if !ok {
		return segment.WriteFailed, 0
	}
	value, err := typed.Result()
	if err != nil {
		if isRedisReplyError(err) {
			return segment.WriteFailed, 0
		}
		return segment.WriteUnknown, 0
	}
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 {
		return segment.WriteFailed, 0
	}
	switch code, _ := items[0].(string); code {
	case "APPLIED":
		missing := 0
		if len(items) > 1 {
			if count, ok := items[1].(int64); ok {
				missing = int(count)
			}
		}
		return segment.WriteApplied, missing
	case string(FencedWriteStaleOwner):
		return segment.WriteStaleOwner, 0
	case string(FencedWriteContentMoved):
		return segment.WriteContentMoved, 0
	}
	return segment.WriteFailed, 0
}

// segmentRenewScript renews one segment of a Slot's due pages (014 B3 3.5):
// under the owner fence, refused whole by a stale one, PEXPIRE of each
// member key it carries to the Plan's lifetime, answering how many were
// already gone - a key gone is not made again, it reads MISSING_WARMING -
// and how long it ran by the server's clock, in microseconds, which is what
// the 2.5 ms budget is held to (a pipelined round trip says nothing of one
// script). PEXPIRE is idempotent: a segment sent again renews again.
//
// ARGV[1] fenced ('1'/'0'); ARGV[2] the TTL in milliseconds. Fenced, KEYS[1..2]
// are the assignment and ownership hashes and ARGV[3..7] the fence's. Then the
// member keys.
const segmentRenewScript = ownership.FenceLua + `
local started = redis.call('TIME')
local key = 1
if ARGV[1] == '1' then
  local refusal = fence_refusal(KEYS[1], KEYS[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], redis_now_ms())
  if refusal == 'CONTENT_MOVED' then return {'CONTENT_MOVED'} end
  if refusal then return {'STALE_OWNER'} end
  key = 3
end
local missing = 0
for index = key, #KEYS do
  if redis.call('PEXPIRE', KEYS[index], ARGV[2]) == 0 then missing = missing + 1 end
end
local ended = redis.call('TIME')
local elapsed = (tonumber(ended[1]) - tonumber(started[1])) * 1000000 + (tonumber(ended[2]) - tonumber(started[2]))
return {'APPLIED', missing, elapsed}
`

// segmentRenewSHA addresses segmentRenewScript by its SHA-1.
var segmentRenewSHA = func() string {
	sum := sha1.Sum([]byte(segmentRenewScript))
	return hex.EncodeToString(sum[:])
}()

// segmentRenewChunk is one renewal script's member keys and the lifetime it
// gives them.
type segmentRenewChunk struct {
	ttl  time.Duration
	keys []string
}

// segmentRenewOutcome is how one renewal script came back: its result, the
// keys it found gone and how long it ran.
type segmentRenewOutcome struct {
	result  string
	missing int
	elapsed time.Duration
}

// SegmentRenewBackend renews member keys under the owner fence, a script a
// chunk, the chunks in as few round trips as their bytes allow.
type SegmentRenewBackend interface {
	RenewSegments(ctx context.Context, guard *FenceGuard, chunks []segmentRenewChunk) []segmentRenewOutcome
}

func segmentRenewArguments(guard *FenceGuard, chunk segmentRenewChunk) ([]string, []interface{}) {
	keys := make([]string, 0, 2+len(chunk.keys))
	args := make([]interface{}, 0, 7)
	args = append(args, boolArg(guard != nil), chunk.ttl.Milliseconds())
	if guard != nil {
		keys = append(keys, guard.Keys.AssignmentKey, guard.Keys.OwnershipKey)
		args = append(args, boolArg(guard.Keys.RequireAssignment), guard.OwnerID,
			strconv.FormatUint(guard.OwnerEpoch, 10), guard.LeaseToken, guard.ContentScope)
	}
	return append(keys, chunk.keys...), args
}

// RenewSegments sends the chunks in round trips of at most
// segmentPipelineBytes of keys, each by its SHA-1 and again with its text
// where the server had none; order does not matter, PEXPIRE being
// idempotent.
func (backend *RedisBackend) RenewSegments(ctx context.Context, guard *FenceGuard, chunks []segmentRenewChunk) []segmentRenewOutcome {
	outcomes := make([]segmentRenewOutcome, len(chunks))
	if backend == nil || backend.client == nil {
		for index := range outcomes {
			outcomes[index].result = segment.WriteFailed
		}
		return outcomes
	}
	send := func(indexes []int, byDigest bool) ([]redis.Cmder, error) {
		cmds, err := backend.client.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
			for _, index := range indexes {
				keys, args := segmentRenewArguments(guard, chunks[index])
				if byDigest {
					pipeline.EvalSha(ctx, segmentRenewSHA, keys, args...)
					continue
				}
				pipeline.Eval(ctx, segmentRenewScript, keys, args...)
			}
			return nil
		})
		if err != nil && !isRedisReplyError(err) {
			return nil, err
		}
		return cmds, nil
	}
	answer := func(indexes []int, byDigest bool) []int {
		cmds, err := send(indexes, byDigest)
		if err != nil || len(cmds) != len(indexes) {
			for _, index := range indexes {
				outcomes[index].result = segment.WriteUnknown
			}
			return nil
		}
		var uncached []int
		for position, cmd := range cmds {
			if byDigest && noScriptReply(cmd.Err()) {
				uncached = append(uncached, indexes[position])
				continue
			}
			outcomes[indexes[position]] = segmentRenewOutcomeOf(cmd)
		}
		return uncached
	}
	for start := 0; start < len(chunks); {
		end, bytes := start, 0
		for end < len(chunks) {
			size := 0
			for _, key := range chunks[end].keys {
				size += len(key)
			}
			if end > start && bytes+size > segmentPipelineBytes {
				break
			}
			bytes += size
			end++
		}
		wave := make([]int, 0, end-start)
		for index := start; index < end; index++ {
			wave = append(wave, index)
		}
		if uncached := answer(wave, true); len(uncached) > 0 {
			answer(uncached, false)
		}
		start = end
	}
	return outcomes
}

func segmentRenewOutcomeOf(cmd redis.Cmder) segmentRenewOutcome {
	result, _ := segmentChunkResult(cmd)
	outcome := segmentRenewOutcome{result: result}
	if result != segment.WriteApplied {
		return outcome
	}
	items, _ := cmd.(*redis.Cmd).Val().([]interface{})
	if len(items) > 1 {
		if missing, ok := items[1].(int64); ok {
			outcome.missing = int(missing)
		}
	}
	if len(items) > 2 {
		if elapsed, ok := items[2].(int64); ok {
			outcome.elapsed = time.Duration(elapsed) * time.Microsecond
		}
	}
	return outcome
}
