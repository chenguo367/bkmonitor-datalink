// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisscript"
)

// AssignmentSweep is what one sweep of the Assignment records found and did.
type AssignmentSweep struct {
	// Scanned is how many Assignment records the key space held.
	Scanned int
	// Retired is how many of them named a Query Group the leader no longer
	// runs; Reclaimed how many of those were deleted, HeldByLease how many
	// were left because a worker still holds a live lease on the Query
	// Group, and Changed how many were left because the record moved under
	// the sweep.
	Retired, Reclaimed, HeldByLease, Changed int
	Duration                                 time.Duration
	// Held names the retired Query Groups whose record a live lease kept,
	// HeldByLease of them: the next round reads these again
	// (RecheckAssignments) rather than walk the key space for them.
	Held []execution.QueryGroupIdentity
	// ScanPages is how many SCAN calls the sweep made: one a page of the
	// key space for a full sweep, none for a recheck.
	ScanPages int
}

// reclaimAssignmentScript deletes one retired Query Group's Assignment
// record and empties its ownership hash under the leader fence, and only
// while nobody holds a live lease on it: a worker that still runs the Query
// Group off an assigned set it read before the retirement keeps its record
// until its lease lapses, and the next sweep takes it. The record is re-read
// inside the script and left alone if it no longer names the Query Group the
// sweep read it under. Expiry is judged on Redis's clock, like every fence.
//
// The owner epoch is not reclaimed with the rest (02 section 6.3 rule 7). A
// Query Group's identity is a digest of its query facts, so a retired one
// comes back when its strategy is enabled again or an edit is reverted, and
// the records its earlier owners wrote are ordered by their epochs: deleted
// here, the epoch would start over at 1 below them. The ownership hash keeps
// that one field, under the expiry ARGV[5], and nothing else -- no owner,
// token, deadline or disposition -- so every fence refuses it and Acquire
// reads it as free and counts on from it. A hash that never had an epoch
// is simply deleted.
//
// KEYS[1] leader ownership hash, KEYS[2] the Assignment record, KEYS[3] the
// Query Group's ownership hash, KEYS[4] the session record of the holder the
// sweep read before it ran (ARGV[6]). ARGV[1..3] the leader fence, ARGV[4]
// the Query Group the record was read under, ARGV[5] how long the epoch is
// kept, in milliseconds.
//
// A holder whose lease is held under a worker session (A2) has no deadline
// of its own: it holds the Query Group while its session is not proven gone,
// read from KEYS[4] as the publication reads it. A holder other than the one
// read before is taken as alive and met again on the next sweep.
var reclaimAssignmentScript = redisscript.New("ownership_reclaim_assignment", FenceLua+`
if fence_refusal('', KEYS[1], '0', ARGV[1], ARGV[2], ARGV[3], '', redis_now_ms()) then return 'STALE' end
local named = redis.call('HGET', KEYS[2], 'query_group')
if not named then return 'GONE' end
if named ~= ARGV[4] then return 'CHANGED' end
local held = redis.call('HMGET', KEYS[3], 'owner_id', 'owner_session_epoch', 'deadline_ms')
local holder, session_epoch = held[1], held[2]
local now_ms = redis_now_ms()
if holder and holder ~= '' then
  if session_epoch and session_epoch ~= '' then
    if holder ~= ARGV[6] then return 'HELD' end
    local session = redis.call('HMGET', KEYS[4], 'session_epoch', 'deadline_ms')
    if not session[1] then return 'HELD' end
    local through = tonumber(session[1])
    if tonumber(session[2] or '0') > now_ms then through = through - 1 end
    if tonumber(session_epoch) > through then return 'HELD' end
  elseif tonumber(held[3] or '0') > now_ms then
    return 'HELD'
  end
end
local epoch = redis.call('HGET', KEYS[3], 'owner_epoch')
redis.call('DEL', KEYS[2], KEYS[3])
if epoch then
  redis.call('HSET', KEYS[3], 'owner_epoch', epoch)
  redis.call('PEXPIRE', KEYS[3], ARGV[5])
end
return 'RECLAIMED'
`)

// ConfigureEpochRetention sets how long a reclaimed Query Group's owner
// epoch outlives the sweep that reclaimed it. It must cover the Query
// Group's retirement recovery period and every record ordered by its owner
// epoch, which only the process wiring the store knows; a store not told
// refuses to sweep rather than delete or keep the epoch on a guess.
func (store *RedisStore) ConfigureEpochRetention(retention time.Duration) error {
	if store == nil || retention < time.Millisecond {
		return errors.New("alarmd ownership: invalid owner epoch retention")
	}
	store.epochRetention = retention
	return nil
}

// assignmentSweepScan bounds one SCAN page and one pipelined read.
const assignmentSweepScan = 500

// SweepAssignments walks every Assignment record in the store and reclaims
// the ones that name a Query Group outside keep -- the set the leader runs
// this round, active and draining -- whose lease has lapsed. Records and
// ownership hashes carry no expiry, so a Query Group that leaves the active
// set leaves both behind for good: six such records, all naming workers of
// long-retired replicas, were found on a deployment, and a coverage reading
// over the records never reaches its whole because of them. The record is
// deleted; of the ownership hash only the owner epoch is left, for the
// retention ConfigureEpochRetention set, after which it expires too.
//
// The sweep is the leader's, under its fence, and it decides nothing about
// live work: a record whose Query Group is still in keep is not touched
// however stale its desired worker, that is the reconcile round's to
// re-place; a retired record with a live lease is left for the lease to
// lapse. A leader that loses its fence mid-sweep stops with ErrStaleFence.
// The walk is a SCAN over the store's own prefix, which is fine on the
// standalone store this runs against and is not written for a cluster.
func (store *RedisStore) SweepAssignments(
	ctx context.Context,
	authority PublicationAuthority,
	keep map[execution.QueryGroupIdentity]struct{},
) (sweep AssignmentSweep, _ error) {
	// sweep is the named result: the deferred Duration below is set on what
	// the caller gets, not on a copy taken by the return.
	if store == nil || store.client == nil {
		return sweep, errors.New("alarmd ownership: initialized store is required")
	}
	if authority.Fence.QueryGroup != ControlLeaderIdentity || validateFence(authority.Fence) != nil {
		return sweep, errors.New("alarmd ownership: assignment sweep needs the control leader authority")
	}
	if store.epochRetention <= 0 {
		return sweep, errors.New("alarmd ownership: assignment sweep needs the owner epoch retention")
	}
	started := time.Now()
	defer func() { sweep.Duration = time.Since(started) }()
	pattern := store.prefix + ":{*}:assignment"
	leaderKey := store.ownershipKey(ControlLeaderIdentity)
	var cursor uint64
	for {
		keys, next, err := store.client.Scan(ctx, cursor, pattern, assignmentSweepScan).Result()
		sweep.ScanPages++
		if err != nil {
			return sweep, fmt.Errorf("alarmd ownership: assignment sweep scan: %w", err)
		}
		if len(keys) > 0 {
			replies := make([]*redis.StringCmd, len(keys))
			if _, err := store.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for index, key := range keys {
					replies[index] = pipe.HGet(ctx, key, "query_group")
				}
				return nil
			}); err != nil && !errors.Is(err, redis.Nil) {
				return sweep, fmt.Errorf("alarmd ownership: assignment sweep read: %w", err)
			}
			for index, key := range keys {
				named, err := replies[index].Result()
				if errors.Is(err, redis.Nil) || named == "" {
					continue
				}
				if err != nil {
					return sweep, fmt.Errorf("alarmd ownership: assignment sweep read: %w", err)
				}
				sweep.Scanned++
				queryGroup := execution.QueryGroupIdentity(named)
				if _, kept := keep[queryGroup]; kept {
					continue
				}
				if store.assignmentKey(queryGroup) != key {
					// The record's own name does not hash to its key: not a
					// record this store wrote. Left alone, counted with the
					// ones that changed under the sweep.
					sweep.Retired++
					sweep.Changed++
					continue
				}
				sweep.Retired++
				if err := store.reclaimRetired(ctx, authority, leaderKey, key, queryGroup, &sweep); err != nil {
					return sweep, err
				}
			}
		}
		if next == 0 {
			return sweep, nil
		}
		cursor = next
	}
}

// reclaimRetired runs the reclaim script on one retired Query Group's
// record, at key, and counts what it answered on sweep: reclaimed, held by a
// live lease (and named in Held for the next round's recheck), or changed
// under the sweep. A fence the leader no longer holds stops the sweep.
func (store *RedisStore) reclaimRetired(
	ctx context.Context, authority PublicationAuthority, leaderKey, key string, queryGroup execution.QueryGroupIdentity, sweep *AssignmentSweep,
) error {
	holder, err := store.client.HGet(ctx, store.ownershipKey(queryGroup), "owner_id").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("alarmd ownership: assignment sweep holder %s: %w", queryGroup, err)
	}
	result, err := reclaimAssignmentScript.Run(ctx, store.client,
		[]string{leaderKey, key, store.ownershipKey(queryGroup), store.workerSessionKey(holder)},
		authority.Fence.OwnerID, authority.Fence.OwnerEpoch, authority.Fence.LeaseToken, string(queryGroup),
		store.epochRetention.Milliseconds(), holder).Text()
	if err != nil {
		return fmt.Errorf("alarmd ownership: assignment sweep reclaim %s: %w", queryGroup, err)
	}
	switch result {
	case "RECLAIMED":
		sweep.Reclaimed++
	case "HELD":
		sweep.HeldByLease++
		sweep.Held = append(sweep.Held, queryGroup)
	case "STALE":
		return ErrStaleFence
	default:
		sweep.Changed++
	}
	return nil
}

// RecheckAssignments reads again only the records a sweep found a live
// lease holding (AssignmentSweep.Held) and reclaims those whose lease has
// lapsed, the same way the full sweep does, with no SCAN. A sweep that
// found one held record used to walk the whole key space again every round
// until that lease lapsed: a retirement on a deployment of half a million
// keys cost eight or nine walks of about a thousand SCAN pages each. A
// Query Group back in keep is left alone, as the full sweep leaves it; a
// record gone or renamed since is not counted. Scanned is the records read.
func (store *RedisStore) RecheckAssignments(
	ctx context.Context,
	authority PublicationAuthority,
	keep map[execution.QueryGroupIdentity]struct{},
	held []execution.QueryGroupIdentity,
) (sweep AssignmentSweep, _ error) {
	// Named, as SweepAssignments' is, for the deferred Duration.
	if store == nil || store.client == nil {
		return sweep, errors.New("alarmd ownership: initialized store is required")
	}
	if authority.Fence.QueryGroup != ControlLeaderIdentity || validateFence(authority.Fence) != nil {
		return sweep, errors.New("alarmd ownership: assignment sweep needs the control leader authority")
	}
	if store.epochRetention <= 0 {
		return sweep, errors.New("alarmd ownership: assignment sweep needs the owner epoch retention")
	}
	started := time.Now()
	defer func() { sweep.Duration = time.Since(started) }()
	leaderKey := store.ownershipKey(ControlLeaderIdentity)
	for _, queryGroup := range held {
		if _, kept := keep[queryGroup]; kept {
			continue
		}
		key := store.assignmentKey(queryGroup)
		named, err := store.client.HGet(ctx, key, "query_group").Result()
		if errors.Is(err, redis.Nil) || (err == nil && named != string(queryGroup)) {
			continue
		}
		if err != nil {
			return sweep, fmt.Errorf("alarmd ownership: assignment recheck read: %w", err)
		}
		sweep.Scanned++
		sweep.Retired++
		if err := store.reclaimRetired(ctx, authority, leaderKey, key, queryGroup, &sweep); err != nil {
			return sweep, err
		}
	}
	return sweep, nil
}
