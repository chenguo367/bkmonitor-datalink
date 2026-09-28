// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package cmdbcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

// The dynamic group cache is the fork's own: one String per group under
// "<redis_key_prefix>dynamic_group:<id>", JSON, written by the dynamic group
// module on its own cadence with a seven-day expiry. alarmd reads it from
// configured target group connection (legacy prefix-only configurations use
// the host cache connection), and only for the groups active Plans reference:
// tens to hundreds of GETs a minute, never a scan of the inverse hash that lists every
// instance of a model.

// GroupClient is the one command the reader issues: MGET over the
// referenced ids, groupReadBatch keys a round trip. Narrow so a test can
// stand in for Redis and fail the transport on purpose.
type GroupClient interface {
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
}

// GroupReader reads group documents by id.
type GroupReader struct {
	client GroupClient
	prefix string
}

// NewGroupReader builds a reader over the fork's group cache. The prefix is
// the fork's own key prefix, spelled exactly as the writer spells it (it
// carries its own separator, ":" by default); it is a deployment coordinate
// rendered from the same source as the writer's, never derived here.
func NewGroupReader(client GroupClient, prefix string) (*GroupReader, error) {
	if client == nil {
		return nil, errors.New("alarmd cmdbcache: a redis client is required")
	}
	if strings.TrimSpace(prefix) == "" {
		return nil, errors.New("alarmd cmdbcache: the dynamic group key prefix is required")
	}
	return &GroupReader{client: client, prefix: prefix}, nil
}

func (reader *GroupReader) key(id string) string {
	return reader.prefix + "dynamic_group:" + id
}

// GroupRead is one id's raw read: the payload, or that the key was absent.
type GroupRead struct {
	Payload []byte
	Missing bool
}

// groupReadBatch bounds the keys of one MGET. A group document is a whole
// member list, from a few hundred bytes to hundreds of kilobytes, and one
// MGET over every referenced group returns all of them in one reply that
// Redis builds, and every other client waits behind, in one go; the batch
// keeps each reply to a size the shared instance serves between other
// commands.
const groupReadBatch = 64

// Read fetches every id, groupReadBatch keys an MGET. The error is the
// transport's - a round trip failed - and a caller keeps what it held: a read
// that failed part way returns nothing rather than some groups from this
// round and none of the rest. A missing key is not an error, it is an
// answer.
func (reader *GroupReader) Read(ctx context.Context, ids []string) (map[string]GroupRead, error) {
	reads := make(map[string]GroupRead, len(ids))
	for start := 0; start < len(ids); start += groupReadBatch {
		batch := ids[start:min(start+groupReadBatch, len(ids))]
		keys := make([]string, len(batch))
		for index, id := range batch {
			keys[index] = reader.key(id)
		}
		values, err := reader.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("alarmd cmdbcache: read dynamic groups: %w", err)
		}
		if len(values) != len(batch) {
			return nil, fmt.Errorf("alarmd cmdbcache: read dynamic groups: %d values for %d keys", len(values), len(batch))
		}
		for index, id := range batch {
			switch value := values[index].(type) {
			case nil:
				reads[id] = GroupRead{Missing: true}
			case string:
				reads[id] = GroupRead{Payload: []byte(value)}
			case []byte:
				reads[id] = GroupRead{Payload: value}
			default:
				return nil, fmt.Errorf("alarmd cmdbcache: read dynamic group %s: unexpected value %T", id, value)
			}
		}
	}
	return reads, nil
}

// GroupMember is one member as the cache carries it, after the checks that
// do not depend on a plan: it names the group's model and an instance, and
// the group's summary lists it.
type GroupMember struct {
	ModelID     string
	ModelInstID string
	// HostID is the host member's bk_host_id as text; empty when the writer
	// did not put one on the member, which drops it under the host rule.
	HostID string
}

// GroupSnapshot is one group as last read: unavailable by name, or a model
// with its members. It is immutable once published; per-plan key sets are
// memoised on it, so a large group is walked once per plan identity per
// snapshot rather than once per Slot.
type GroupSnapshot struct {
	ID string
	// ReadAt is when the read that produced this snapshot succeeded.
	ReadAt time.Time
	// Unavailable is the closed reason the group cannot be used at all:
	// key_missing, json_invalid, structure_invalid. Empty when it can.
	Unavailable string
	ModelID     string
	Members     []GroupMember
	// Dropped counts the members refused by the plan-independent checks.
	Dropped int

	mu   sync.Mutex
	keys map[string]*groupKeys
}

type groupKeys struct {
	members map[string]struct{}
	dropped int
}

// Keys returns the group's member keys under a plan's identity and rule,
// and how many members the rule dropped on top of the plan-independent
// drops. The set is shared; callers read it.
func (snapshot *GroupSnapshot) Keys(plan *contract.TargetPlanV1) (map[string]struct{}, int) {
	if snapshot == nil || plan == nil {
		return nil, 0
	}
	signature := string(plan.Rule) + "\x00" + plan.ModelID + "\x00" + strings.Join(plan.Identity.Dimensions, ",") + "\x00" + plan.Identity.ModelDimension + "\x00" + plan.Identity.ModelValue
	if plan.Identity.HostIdentity {
		signature += "\x00host"
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if cached, found := snapshot.keys[signature]; found {
		return cached.members, cached.dropped
	}
	built := &groupKeys{members: make(map[string]struct{}, len(snapshot.Members))}
	for _, member := range snapshot.Members {
		if member.ModelID != plan.ModelID {
			built.dropped++
			continue
		}
		switch {
		case plan.Rule == contract.TargetPlanRuleHostID || plan.Identity.HostIdentity:
			// Held under the host id: the host_id rule's key, and the key of
			// a model_inst_id plan read by host identity. A member the
			// writer put no host id on cannot be placed and is dropped.
			if member.HostID == "" {
				built.dropped++
				continue
			}
			built.members[plan.Identity.HostKey(member.HostID)] = struct{}{}
		default:
			built.members[plan.Identity.MemberKey(member.ModelID, member.ModelInstID)] = struct{}{}
		}
	}
	if snapshot.keys == nil {
		snapshot.keys = make(map[string]*groupKeys, 2)
	}
	snapshot.keys[signature] = built
	return built.members, built.dropped
}

// decodeGroup reads one group document per the protocol: an object with
// model_id, member_list and the model_inst_ids summary. A member is kept
// when it names the group's model, names an instance, and the summary lists
// it; the rest are dropped and counted. member_list absent is a structure
// the reader does not accept - it cannot tell "empty" from "not written" -
// and member_list present and empty is the writer saying empty.
func decodeGroup(id string, payload []byte, readAt time.Time) *GroupSnapshot {
	snapshot := &GroupSnapshot{ID: id, ReadAt: readAt}
	var document struct {
		ModelID      string            `json:"model_id"`
		ModelInstIDs []json.RawMessage `json:"model_inst_ids"`
		MemberList   *[]struct {
			ModelID     string          `json:"model_id"`
			ModelInstID json.RawMessage `json:"model_inst_id"`
			HostID      json.RawMessage `json:"bk_host_id"`
		} `json:"member_list"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		snapshot.Unavailable = targetplan.ReasonJSONInvalid
		return snapshot
	}
	if strings.TrimSpace(document.ModelID) == "" || document.MemberList == nil {
		snapshot.Unavailable = targetplan.ReasonStructureInvalid
		return snapshot
	}
	snapshot.ModelID = document.ModelID
	var listed map[string]struct{}
	if document.ModelInstIDs != nil {
		listed = make(map[string]struct{}, len(document.ModelInstIDs))
		for _, raw := range document.ModelInstIDs {
			if text := rawScalarText(raw); text != "" {
				listed[text] = struct{}{}
			}
		}
	}
	for _, wire := range *document.MemberList {
		instance := rawScalarText(wire.ModelInstID)
		if wire.ModelID != document.ModelID || instance == "" {
			snapshot.Dropped++
			continue
		}
		if listed != nil {
			if _, found := listed[instance]; !found {
				snapshot.Dropped++
				continue
			}
		}
		host := rawScalarText(wire.HostID)
		if host == "0" {
			host = ""
		}
		snapshot.Members = append(snapshot.Members, GroupMember{ModelID: wire.ModelID, ModelInstID: instance, HostID: host})
	}
	return snapshot
}

// GroupLookup is what the store hands a resolver for one id: the snapshot,
// how old it is, and whether a refresh after it failed at the transport,
// which is the one condition under which an old snapshot is served on
// purpose and has to be said.
type GroupLookup struct {
	Snapshot      *GroupSnapshot
	Age           time.Duration
	RefreshFailed bool
	// EmptiedHeld says the store is holding back an empty read of this group
	// (see GroupStore.Refresh) and the snapshot served is the one before it.
	EmptiedHeld bool
	// ReadErr is set when the id was not held and the synchronous first
	// read failed; the snapshot is then nil.
	ReadErr error
}

// GroupStore holds the referenced groups' snapshots and refreshes them on
// the host index's cadence. An id is referenced the first time a resolver
// asks for it; that first ask reads it synchronously, one GET on the
// connection every Slot already writes State and Progress to, so a Plan's
// first Slot after a restart does not run a minute with its group members
// outside the target.
type GroupStore struct {
	reader   *GroupReader
	interval time.Duration
	maxAge   time.Duration
	now      func() time.Time

	mu        sync.RWMutex
	snapshots map[string]*GroupSnapshot
	// referenced is, per id, when it was last asked for and the longest
	// evaluation interval among the Plans that asked. A reference ages out
	// when nobody has asked for it within max(the staleness bound, twice
	// that longest interval): a Plan asks every Slot, so an id nobody asks
	// for that long is one no active Plan references any more, and keeping
	// it would read it every refresh and count it in the health as a
	// standing failure once the writer withdraws it. The bound keeps every
	// Plan on a period up to the staleness bound at zero Slot-path reads,
	// and the registered interval keeps the longer ones there too; no
	// horizon runs close to a Slot's own cadence.
	referenced    map[string]groupReference
	lastFailureAt time.Time
	syncReads     uint64
	lastError     error
	failures      uint64
	refreshes     uint64

	// pending are the groups whose snapshot has members and whose reads have
	// been empty since the time recorded; the snapshot before is served until
	// the emptying is believed (see Refresh). unconfirmed are groups first
	// read empty while others were pending, served as emptied_held until the
	// same. held is how many pending groups the latest refresh judged the
	// source's, emptiedHolds how many refreshes judged so, and emptiedChanged
	// is told the held count and the groups it was judged against whenever
	// that count changes.
	pending        map[string]time.Time
	unconfirmed    map[string]time.Time
	held           int
	emptiedHolds   uint64
	emptiedChanged func(held, candidates int)
}

// GroupStoreOptions mirror StoreOptions: the same cadence and the same
// staleness bound as the host index, on purpose.
type GroupStoreOptions struct {
	RefreshInterval time.Duration
	MaxAge          time.Duration
	Now             func() time.Time
	// EmptiedChanged, when set, is called after a refresh whose held-back
	// emptying changed in size - a hold starting, growing, shrinking or
	// ending - with the groups held and the groups that had members before.
	EmptiedChanged func(held, candidates int)
}

func NewGroupStore(reader *GroupReader, options GroupStoreOptions) (*GroupStore, error) {
	if reader == nil {
		return nil, errors.New("alarmd cmdbcache: a group reader is required")
	}
	if options.RefreshInterval <= 0 {
		return nil, errors.New("alarmd cmdbcache: a positive refresh interval is required")
	}
	if options.MaxAge <= options.RefreshInterval {
		return nil, errors.New("alarmd cmdbcache: the staleness bound must exceed the refresh interval")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &GroupStore{reader: reader, interval: options.RefreshInterval, maxAge: options.MaxAge, now: now,
		snapshots: make(map[string]*GroupSnapshot), referenced: make(map[string]groupReference),
		pending: make(map[string]time.Time), unconfirmed: make(map[string]time.Time), emptiedChanged: options.EmptiedChanged}, nil
}

// MaxAge is the bound past which a held snapshot is not served.
func (store *GroupStore) MaxAge() time.Duration {
	if store == nil {
		return 0
	}
	return store.maxAge
}

// groupReference is one id's registration: when it was last asked for and
// the longest evaluation interval among the Plans that asked.
type groupReference struct {
	askedAt  time.Time
	interval time.Duration
}

// Group answers one id for a Plan evaluated every interval, reading it
// first when it is not held.
func (store *GroupStore) Group(ctx context.Context, id string, interval time.Duration) GroupLookup {
	if store == nil {
		return GroupLookup{ReadErr: errors.New("alarmd cmdbcache: no group store")}
	}
	now := store.now()
	store.mu.Lock()
	reference := store.referenced[id]
	reference.askedAt = now
	if interval > reference.interval {
		reference.interval = interval
	}
	store.referenced[id] = reference
	snapshot, held := store.snapshots[id]
	failedAt := store.lastFailureAt
	_, emptiedHeld := store.pending[id]
	if !held {
		store.syncReads++
	}
	store.mu.Unlock()
	if !held {
		reads, err := store.reader.Read(ctx, []string{id})
		if err != nil {
			return GroupLookup{ReadErr: err}
		}
		snapshot = store.publishFirst(id, reads[id], now)
		failedAt, emptiedHeld = time.Time{}, false
	}
	return GroupLookup{Snapshot: snapshot, Age: now.Sub(snapshot.ReadAt),
		RefreshFailed: failedAt.After(snapshot.ReadAt) || emptiedHeld, EmptiedHeld: emptiedHeld}
}

// snapshotOf is one id's read as a snapshot.
func snapshotOf(id string, read GroupRead, at time.Time) *GroupSnapshot {
	if read.Missing {
		return &GroupSnapshot{ID: id, ReadAt: at, Unavailable: targetplan.ReasonKeyMissing}
	}
	return decodeGroup(id, read.Payload, at)
}

// emptiedRead is a read that says the group has no members: available,
// with none kept and none refused. A group whose members were all refused is
// not empty; it resolves incomplete.
func emptiedRead(snapshot *GroupSnapshot) bool {
	return snapshot.Unavailable == "" && len(snapshot.Members) == 0 && snapshot.Dropped == 0
}

// hasMembers is a snapshot a strategy finds members in.
func hasMembers(snapshot *GroupSnapshot) bool {
	return snapshot != nil && snapshot.Unavailable == "" && len(snapshot.Members) > 0
}

// publishFirst publishes a group's first read. A first read that is empty
// while other groups' emptyings are pending has no snapshot before it to
// judge by, and may be one of them: it is served as emptied_held and decided
// with the pending ones (see Refresh) rather than read as empty.
func (store *GroupStore) publishFirst(id string, read GroupRead, at time.Time) *GroupSnapshot {
	snapshot := snapshotOf(id, read, at)
	store.mu.Lock()
	defer store.mu.Unlock()
	if emptiedRead(snapshot) && len(store.pending) > 0 {
		store.unconfirmed[id] = at
		snapshot = &GroupSnapshot{ID: id, ReadAt: at, Unavailable: targetplan.ReasonEmptiedHeld}
	}
	store.snapshots[id] = snapshot
	return snapshot
}

// groupEmptiedSettle is how long a group that went from members to none has
// to read empty before the emptying is believed: one cycle of the group cache
// writer. The writer rewrites every group once a cycle of ten minutes,
// dispatching one task per group to a worker pool shared with its other
// work; when a cycle's writes land depends on the pool, its concurrency and
// the group count, none of which its source bounds, and the cycle is the one
// bound it states. So every group a cycle rewrote empty is read empty within
// one cycle of the first, and they are judged together.
const groupEmptiedSettle = 10 * time.Minute

// The emptying of groups the store judges the source's rather than the
// groups': the same proportion and floor the strategy cache writer and the
// absent-strategy close judge a shrink of their own sets by.
const (
	emptiedShareDenominator = 5
	emptiedMinimum          = 20
)

// emptiedBySource says whether flipped of candidates groups going from
// members to none within one writer cycle is the source's doing: every group
// that had members, when there were at least two, or at least emptiedMinimum
// and more than a fifth of them. A group emptying alone is a group emptying -
// a service retired, a condition that matches nothing any more - and is read
// as it is once it has stayed empty a cycle; many at once is a writer that
// answered nothing.
func emptiedBySource(flipped, candidates int) bool {
	if flipped < 2 {
		return false
	}
	return flipped == candidates || (flipped >= emptiedMinimum && flipped*emptiedShareDenominator > candidates)
}

// Refresh re-reads every referenced group. A transport failure keeps every
// snapshot and is recorded, so the next lookups say they are served past a
// failed refresh; a missing key replaces the snapshot with an unavailable
// one - the writer withdrew or never wrote it, and that is an answer. A
// reference nobody has asked for within its horizon is forgotten first,
// snapshot and all.
//
// A group read with no members where it had some is the writer saying the
// group emptied, and a group's members are where its strategies look for
// the series they judge: read as it is, every series of those members is
// out of the target, undetected, and its alert closable. The writer writes
// an empty list when its member query answered nothing, which one group
// emptying and a source that answered nothing for everyone both look like,
// and it writes a cycle's groups one task at a time, so neither arrives in
// one refresh. So an emptying is judged over a writer cycle:
//
//   - a group that went from members to none is pending: the snapshot before
//     is still served, as past a failed refresh, and unavailable by name
//     (emptied_held) once older than the staleness bound, which never closes
//     an alert;
//   - the pending groups are judged together against the groups that had
//     members (emptiedBySource). At or above the line they are held for as
//     long as they stay empty; below it, a group that has read empty for a
//     whole cycle (groupEmptiedSettle) is published empty;
//   - a pending group read with members again is published, and the rest's
//     wait starts over, the groups first read during it included: a writer
//     writing members back is partway through a cycle that will bring the
//     others back too;
//   - a group first read empty while others are pending waits with them.
//
// Known boundaries: a store referencing one group cannot tell its emptying
// from the writer's and reads it empty after a cycle. When every group a
// store references (two or more, fewer than emptiedMinimum) really empties
// at once, the hold does not end until members come back, the references go
// or the process restarts: those groups' alerts are not closed, never closed
// wrongly, and the groups read emptied_held. A group that really emptied
// beside groups that empty and come back in turn, one a writer cycle, has
// its wait started over by each return and is not believed while that goes
// on; it is held, named, and believed a cycle after the returns stop. A
// restart forgets every pending group and reads what the writer has then.
func (store *GroupStore) Refresh(ctx context.Context) error {
	now := store.now()
	store.mu.Lock()
	ids := make([]string, 0, len(store.referenced))
	for id, reference := range store.referenced {
		horizon := store.maxAge
		if 2*reference.interval > horizon {
			horizon = 2 * reference.interval
		}
		if now.Sub(reference.askedAt) > horizon {
			delete(store.referenced, id)
			delete(store.snapshots, id)
			delete(store.pending, id)
			delete(store.unconfirmed, id)
			continue
		}
		ids = append(ids, id)
	}
	store.mu.Unlock()
	sort.Strings(ids)
	reads, err := store.reader.Read(ctx, ids)
	if err != nil {
		store.mu.Lock()
		store.lastFailureAt, store.lastError = store.now(), err
		store.failures++
		store.mu.Unlock()
		return err
	}
	at := store.now()
	next := make(map[string]*GroupSnapshot, len(ids))
	for _, id := range ids {
		next[id] = snapshotOf(id, reads[id], at)
	}
	store.mu.Lock()
	returned := false
	for _, id := range ids {
		current := next[id]
		if _, isPending := store.pending[id]; isPending {
			if emptiedRead(current) {
				continue
			}
			delete(store.pending, id)
			returned = returned || hasMembers(current)
			store.snapshots[id] = current
			continue
		}
		if _, isUnconfirmed := store.unconfirmed[id]; isUnconfirmed {
			if emptiedRead(current) {
				continue
			}
			delete(store.unconfirmed, id)
			store.snapshots[id] = current
			continue
		}
		if hasMembers(store.snapshots[id]) && emptiedRead(current) {
			store.pending[id] = at
			continue
		}
		store.snapshots[id] = current
	}
	if returned {
		for id := range store.pending {
			store.pending[id] = at
		}
		for id := range store.unconfirmed {
			store.unconfirmed[id] = at
		}
	}
	candidates := 0
	for _, id := range ids {
		if hasMembers(store.snapshots[id]) {
			candidates++
		}
	}
	holding := emptiedBySource(len(store.pending), candidates)
	if holding {
		store.emptiedHolds++
	} else {
		for id, since := range store.pending {
			if at.Sub(since) >= groupEmptiedSettle {
				store.snapshots[id] = next[id]
				delete(store.pending, id)
			}
		}
	}
	for id, since := range store.unconfirmed {
		if holding || len(store.pending) > 0 || at.Sub(since) < groupEmptiedSettle {
			continue
		}
		store.snapshots[id] = next[id]
		delete(store.unconfirmed, id)
	}
	heldBefore := store.held
	store.held = 0
	if holding {
		store.held = len(store.pending)
	}
	store.lastError = nil
	store.refreshes++
	changed := store.held != heldBefore
	held := store.held
	store.mu.Unlock()
	if changed && store.emptiedChanged != nil {
		store.emptiedChanged(held, candidates)
	}
	return nil
}

// Run keeps the referenced groups fresh until the context ends.
func (store *GroupStore) Run(ctx context.Context) {
	if store == nil {
		return
	}
	ticker := time.NewTicker(store.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = store.Refresh(ctx)
		}
	}
}

// GroupHealth is what the store is currently serving.
type GroupHealth struct {
	Referenced  int
	Loaded      int
	Unavailable int
	// RefreshFailed says the last refresh failed at the transport; the
	// snapshots served are older than the cadence promises.
	RefreshFailed     bool
	ConsecutiveErrors uint64
	Refreshes         uint64
	// SyncReads counts the reads made on a Slot path for an id not held:
	// one per first reference, and none after, is the reading.
	SyncReads uint64
	// EmptiedPending is how many groups' empty reads are held back, pending
	// or judged the source's; EmptiedHeld how many the latest refresh judged
	// the source's; EmptiedHolds how many refreshes have judged so.
	EmptiedPending int
	EmptiedHeld    int
	EmptiedHolds   uint64
}

func (store *GroupStore) Health() GroupHealth {
	if store == nil {
		return GroupHealth{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	health := GroupHealth{Referenced: len(store.referenced), RefreshFailed: store.lastError != nil,
		ConsecutiveErrors: store.failures, Refreshes: store.refreshes, SyncReads: store.syncReads,
		EmptiedPending: len(store.pending) + len(store.unconfirmed), EmptiedHeld: store.held, EmptiedHolds: store.emptiedHolds}
	for _, snapshot := range store.snapshots {
		if snapshot.Unavailable != "" {
			health.Unavailable++
			continue
		}
		health.Loaded++
	}
	return health
}
