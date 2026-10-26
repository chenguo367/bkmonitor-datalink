// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// The words a close is held back under for a reopen (L40), closed: a
// metric label is made of them.
const (
	// HeldReopenedSinceRead: an alarmd process sent a recovery for the
	// fingerprint after the read the close is decided from began, and then
	// an ABNORMAL of it. The alert the read named may have ended, and the
	// fingerprint's active alert be the new one the close would end.
	// Decided again from the next read.
	HeldReopenedSinceRead = "reopened_since_read"
	// HeldRecoveryUnknown: the record could not be read. Nothing is sent on
	// a record that could not say.
	HeldRecoveryUnknown = "recovery_unknown"
)

// Recoveries is the fleet's record of the fingerprints alarmd reopened -
// an ABNORMAL acknowledged after a RECOVERY of the same fingerprint - in
// its runtime Redis: one sorted set per strategy, member the fingerprint,
// score the millisecond the sink acknowledged the RECOVERY the reopen
// followed. A close is decided by fingerprint from a read of the link's
// evidence and arrives later; if in between the alert the read named
// recovered and the series fired again, the close ends the new alert
// (L40). A recovery alone is not that race: a healthy series says RECOVERY
// every cycle, and one at a Level the open alert does not stand at reaches
// the link as an orphan and ends nothing, so a record of recoveries alone
// would hold an alert whose Level an edit removed for as long as the series
// stays healthy at the others. The reopen is sent by the process that owns
// the series and a close may be decided by another - the control leader's
// edit close - so the record is shared, not each process's own.
//
// What makes a reopen is this process's: the recoveries it sent since each
// fingerprint's last ABNORMAL are kept in its memory, and only an ABNORMAL
// that follows one writes. A series that moved to another process between
// the two is not recorded, and its close is not held, as before the record.
type Recoveries struct {
	client redis.UniversalClient
	prefix string

	mu sync.Mutex
	// recovered is when this process last sent a RECOVERY of each
	// fingerprint since its last ABNORMAL; RecoveryRetention, at most
	// recoveryKept, the oldest out first and counted as forgotten.
	recovered map[recoveredFingerprint]time.Time

	noted, noteFailed, readFailed, forgotten atomic.Uint64
}

type recoveredFingerprint struct {
	tenantID, strategyID, fingerprint string
}

// Each strategy's record keeps RecoveryRetention of reopens, at most
// recoveryKept of them: a close is checked against the reopens since a
// read that began at most a calibration's age before it (twice the
// calibration interval), and the record keeps as long; this process
// remembers a recovery as long, and as many. RecoverySkew is how far apart
// two processes' clocks are taken to be: a recovery stamped up to it before
// a read began is still taken as after it, which only holds a close back a
// round.
const (
	RecoveryRetention = time.Hour
	recoveryKept      = 100000
	RecoverySkew      = 30 * time.Second
)

func NewRecoveries(client redis.UniversalClient, prefix string) (*Recoveries, error) {
	if client == nil || strings.TrimSpace(prefix) == "" || strings.ContainsAny(prefix, "{} \t\r\n") {
		return nil, errors.New("alarmd openalerts: recovery record client and prefix are required")
	}
	return &Recoveries{client: client, prefix: prefix, recovered: map[recoveredFingerprint]time.Time{}}, nil
}

func (recoveries *Recoveries) key(tenantID, strategyID string) string {
	return recoveries.prefix + ":reopened:v1:" + tenantKeyDigest(tenantID) + ":" + strategyID
}

// Sent is one event the sink acknowledged: a RECOVERY, or an ABNORMAL.
type Sent struct {
	TenantID    string
	StrategyID  string
	Fingerprint string
	Recovery    bool
}

// Note takes events the sink acknowledged at at, in the order sent. A
// RECOVERY is only remembered here; an ABNORMAL of a fingerprint with a
// RECOVERY remembered is a reopen, written to the record. A batch with no
// reopen - every steady stream of RECOVERY or ABNORMAL - makes no round trip;
// one with reopens makes one, and lets each touched record's old entries go.
func (recoveries *Recoveries) Note(ctx context.Context, at time.Time, sent []Sent) error {
	if recoveries == nil || len(sent) == 0 {
		return nil
	}
	type reopen struct {
		key, fingerprint string
		recoveredAt      time.Time
	}
	var reopened []reopen
	recoveries.mu.Lock()
	for _, event := range sent {
		fingerprint := recoveredFingerprint{tenantID: event.TenantID, strategyID: event.StrategyID, fingerprint: event.Fingerprint}
		if event.Recovery {
			recoveries.recovered[fingerprint] = at
			continue
		}
		recoveredAt, ok := recoveries.recovered[fingerprint]
		if !ok {
			continue
		}
		delete(recoveries.recovered, fingerprint)
		if at.Sub(recoveredAt) <= RecoveryRetention {
			reopened = append(reopened, reopen{key: recoveries.key(event.TenantID, event.StrategyID), fingerprint: event.Fingerprint, recoveredAt: recoveredAt})
		}
	}
	recoveries.bound(at)
	recoveries.mu.Unlock()
	if len(reopened) == 0 {
		return nil
	}
	cutoff := strconv.FormatInt(at.Add(-RecoveryRetention).UnixMilli(), 10)
	pipe := recoveries.client.Pipeline()
	touched := map[string]bool{}
	for _, reopen := range reopened {
		pipe.ZAdd(ctx, reopen.key, &redis.Z{Score: float64(reopen.recoveredAt.UnixMilli()), Member: reopen.fingerprint})
		touched[reopen.key] = true
	}
	for key := range touched {
		pipe.ZRemRangeByScore(ctx, key, "-inf", "("+cutoff)
		pipe.ZRemRangeByRank(ctx, key, 0, -recoveryKept-1)
		pipe.Expire(ctx, key, 2*RecoveryRetention)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		recoveries.noteFailed.Add(uint64(len(reopened)))
		return err
	}
	recoveries.noted.Add(uint64(len(reopened)))
	return nil
}

// bound keeps the remembered recoveries inside recoveryKept: past it, the
// ones older than RecoveryRetention go, then the oldest, down to nine
// tenths, counted as forgotten - a reopen after one of them is not
// recorded. Called with the lock held.
func (recoveries *Recoveries) bound(at time.Time) {
	if len(recoveries.recovered) <= recoveryKept {
		return
	}
	for fingerprint, recoveredAt := range recoveries.recovered {
		if at.Sub(recoveredAt) > RecoveryRetention {
			delete(recoveries.recovered, fingerprint)
		}
	}
	over := len(recoveries.recovered) - recoveryKept*9/10
	if over <= 0 || len(recoveries.recovered) <= recoveryKept {
		return
	}
	type aged struct {
		fingerprint recoveredFingerprint
		at          time.Time
	}
	all := make([]aged, 0, len(recoveries.recovered))
	for fingerprint, recoveredAt := range recoveries.recovered {
		all = append(all, aged{fingerprint: fingerprint, at: recoveredAt})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for _, entry := range all[:over] {
		delete(recoveries.recovered, entry.fingerprint)
	}
	recoveries.forgotten.Add(uint64(over))
}

// ReopenedSince is, of fingerprints, the ones a reopen was recorded for
// whose RECOVERY was at or after readStarted less RecoverySkew.
func (recoveries *Recoveries) ReopenedSince(ctx context.Context, tenantID, strategyID string, fingerprints []string, readStarted time.Time) (map[string]bool, error) {
	if recoveries == nil || len(fingerprints) == 0 {
		return nil, nil
	}
	key := recoveries.key(tenantID, strategyID)
	pipe := recoveries.client.Pipeline()
	scores := make([]*redis.FloatCmd, len(fingerprints))
	for i, fingerprint := range fingerprints {
		scores[i] = pipe.ZScore(ctx, key, fingerprint)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		recoveries.readFailed.Add(1)
		return nil, err
	}
	floor := float64(readStarted.Add(-RecoverySkew).UnixMilli())
	reopened := map[string]bool{}
	for i, score := range scores {
		value, err := score.Result()
		switch {
		case errors.Is(err, redis.Nil):
		case err != nil:
			recoveries.readFailed.Add(1)
			return nil, err
		case value >= floor:
			reopened[fingerprints[i]] = true
		}
	}
	return reopened, nil
}

// Held is, of fingerprints a read begun at readStarted decided to close,
// the ones to hold back and the word that holds them (Held*): every one
// when the record cannot be read. Nil without a record.
func (recoveries *Recoveries) Held(ctx context.Context, tenantID, strategyID string, fingerprints []string, readStarted time.Time) map[string]string {
	if recoveries == nil || len(fingerprints) == 0 {
		return nil
	}
	held := map[string]string{}
	reopened, err := recoveries.ReopenedSince(ctx, tenantID, strategyID, fingerprints, readStarted)
	for _, fingerprint := range fingerprints {
		switch {
		case err != nil:
			held[fingerprint] = HeldRecoveryUnknown
		case reopened[fingerprint]:
			held[fingerprint] = HeldReopenedSinceRead
		}
	}
	return held
}

// RecoveryRecordStats is the record's counts on this process: reopens
// noted and not noted, reads that failed, and recoveries forgotten before
// an ABNORMAL could follow them.
type RecoveryRecordStats struct {
	Noted, NoteFailed, ReadFailed, Forgotten uint64
}

func (recoveries *Recoveries) Stats() RecoveryRecordStats {
	if recoveries == nil {
		return RecoveryRecordStats{}
	}
	return RecoveryRecordStats{Noted: recoveries.noted.Load(), NoteFailed: recoveries.noteFailed.Load(),
		ReadFailed: recoveries.readFailed.Load(), Forgotten: recoveries.forgotten.Load()}
}

// tenantKeyDigest is a tenant as a key segment: digested, as every runtime
// key's tenant is, so a tenant id never reaches a key name.
func tenantKeyDigest(tenantID string) string {
	sum := sha256.Sum256([]byte(tenantID))
	return hex.EncodeToString(sum[:16])
}

// RecoveryRecordOutcomes is every word alarmd_recovery_record_total counts.
var RecoveryRecordOutcomes = []string{"noted", "note_failed", "read_failed", "forgotten"}
