// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package ownership

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/roles"
)

// InstanceRegistration describes a running process's roles and internal
// endpoint. It is separate from WorkerRegistration: control and channel
// instances neither become execution candidates nor enlarge worker counts.
type InstanceRegistration struct {
	InstanceID    string                `json:"instance_id"`
	Roles         roles.Set             `json:"roles"`
	Endpoint      string                `json:"endpoint,omitempty"`
	Incarnation   string                `json:"incarnation"`
	InternalToken string                `json:"internal_token,omitempty"`
	ExpiresAt     time.Time             `json:"expires_at"`
	RoleStatus    map[roles.Role]string `json:"role_status,omitempty"`
	// ControlCandidacy is the class this process stands for the Control
	// Leader lease in: ControlCandidacyPreferred for a process with the
	// control role, ControlCandidacyFallback for a Worker without it that
	// can still run the core control rounds when nobody preferred is there.
	// Empty means the process is no candidate, and it is also what every
	// record written before candidacy existed carries: such a record is read
	// by its roles, as it always was.
	ControlCandidacy string `json:"control_candidacy,omitempty"`
	// ControlCandidacyReady says the process has finished starting and is
	// not stopping, so it may take the lease. A candidate that is not ready
	// does not contend, and a preferred one that is not ready does not hold
	// a fallback off.
	ControlCandidacyReady bool `json:"control_candidacy_ready,omitempty"`
}

// The two candidacy classes. Derived from the roles, never configured: the
// control role makes a process preferred, and a Worker without it is the
// fallback that keeps the deployment led when no preferred process is.
const (
	ControlCandidacyPreferred = "preferred"
	ControlCandidacyFallback  = "fallback"
)

func (instance InstanceRegistration) Validate() error {
	if instance.InstanceID == "" || instance.Incarnation == "" || instance.ExpiresAt.IsZero() || len(instance.Roles) == 0 {
		return errors.New("alarmd ownership: incomplete instance registration")
	}
	if _, err := roles.Resolve(instance.Roles); err != nil {
		return fmt.Errorf("alarmd ownership: instance roles: %w", err)
	}
	for role, status := range instance.RoleStatus {
		if !instance.Roles.Has(role) || status == "" {
			return errors.New("alarmd ownership: status requires a declared role and nonempty value")
		}
	}
	switch instance.ControlCandidacy {
	case "":
		if instance.ControlCandidacyReady {
			return errors.New("alarmd ownership: control candidacy readiness requires a candidacy class")
		}
	case ControlCandidacyPreferred:
		if !instance.Roles.Has(roles.Control) {
			return errors.New("alarmd ownership: a preferred control candidate declares the control role")
		}
	case ControlCandidacyFallback:
		if instance.Roles.Has(roles.Control) || !instance.Roles.Has(roles.Worker) {
			return errors.New("alarmd ownership: a fallback control candidate is a Worker without the control role")
		}
	default:
		return fmt.Errorf("alarmd ownership: unknown control candidacy %q", instance.ControlCandidacy)
	}
	return nil
}

// ControlCapable says the process this record describes can be the Control
// Leader: it declares the control role, or it stands as a fallback
// candidate. A reader that finds the lease held by a process uses this,
// not the role alone, to accept that process as the Leader it talks to.
func (instance InstanceRegistration) ControlCapable() bool {
	return instance.Roles.Has(roles.Control) || instance.ControlCandidacy == ControlCandidacyFallback
}

// CandidacyClass is the class the record stands in, reading a record from
// before candidacy existed by its roles: a control process was always the
// only kind that took the lease, so it reads as preferred.
func (instance InstanceRegistration) CandidacyClass() string {
	if instance.ControlCandidacy == "" && instance.Roles.Has(roles.Control) {
		return ControlCandidacyPreferred
	}
	return instance.ControlCandidacy
}

func (store *RedisStore) instanceKey(instanceID string) string {
	digest := sha256.Sum256([]byte(instanceID))
	return store.prefix + ":instance:" + hex.EncodeToString(digest[:])
}

// RegisterInstance renews only the process's independent registration. The
// Worker registry, QG identities and fencing leases keep their existing keys.
func (store *RedisStore) RegisterInstance(ctx context.Context, instance InstanceRegistration) error {
	if store == nil || store.client == nil {
		return errors.New("alarmd ownership: initialized instance store is required")
	}
	if err := instance.Validate(); err != nil {
		return err
	}
	ttl := time.Until(instance.ExpiresAt)
	if ttl <= 0 {
		return errors.New("alarmd ownership: instance registration is already expired")
	}
	payload, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("alarmd ownership: encode instance registration: %w", err)
	}
	if instance.ControlCandidacy != ControlCandidacyPreferred {
		return store.client.Set(ctx, store.instanceKey(instance.InstanceID), payload, ttl).Err()
	}
	// A preferred candidate is also named in the index a fallback reads, in
	// the same transaction: a record written without its index entry would
	// be a live preferred process a fallback cannot see, free to take a lease
	// that is about to be wanted back. The score is the record's own expiry,
	// so the index never says a record is live longer than the record is.
	_, err = store.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, store.instanceKey(instance.InstanceID), payload, ttl)
		pipe.ZAdd(ctx, store.preferredCandidatesKey(), &redis.Z{
			Score: float64(instance.ExpiresAt.UnixMilli()), Member: instance.InstanceID,
		})
		return nil
	})
	return err
}

// preferredCandidatesKey indexes the processes that stand as preferred
// Control Leader candidates, scored by their record's expiry. Only the
// preferred are indexed: the one question a fallback asks is whether any of
// them is there, and the index then stays as small as the control
// processes rather than growing with the Workers.
func (store *RedisStore) preferredCandidatesKey() string {
	return store.prefix + ":control-preferred-candidates"
}

// ListPreferredControlCandidates reads every preferred candidate whose record
// is live at the given instant, ready or not; the caller judges readiness.
// One round trip prunes the index and reads it, a second reads the records.
//
// Every failure is an error rather than a short list. The caller is a
// fallback deciding whether it may take the lease, or a fallback Leader
// deciding whether to hand it back, and a truncated list reads to the first
// as "no preferred process is there" -- the one answer that lets it contend
// against a preferred process that is.
func (store *RedisStore) ListPreferredControlCandidates(ctx context.Context, at time.Time) ([]InstanceRegistration, error) {
	if store == nil || store.client == nil || at.IsZero() {
		return nil, errors.New("alarmd ownership: candidate listing needs a store and an instant")
	}
	key := store.preferredCandidatesKey()
	cutoff := strconv.FormatInt(at.UnixMilli(), 10)
	var listed *redis.StringSliceCmd
	if _, err := store.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(ctx, key, "-inf", cutoff)
		listed = pipe.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: "(" + cutoff, Max: "+inf"})
		return nil
	}); err != nil {
		return nil, err
	}
	ids := listed.Val()
	if len(ids) == 0 {
		return nil, nil
	}
	replies := make([]*redis.StringCmd, len(ids))
	if _, err := store.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, id := range ids {
			replies[index] = pipe.Get(ctx, store.instanceKey(id))
		}
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	candidates := make([]InstanceRegistration, 0, len(ids))
	for index, reply := range replies {
		payload, err := reply.Bytes()
		if errors.Is(err, redis.Nil) {
			// The record went before the index noticed: a process that
			// stopped registering. Its score lapses on its own.
			continue
		}
		if err != nil {
			return nil, err
		}
		var instance InstanceRegistration
		if err := json.Unmarshal(payload, &instance); err != nil {
			return nil, fmt.Errorf("alarmd ownership: decode instance registration: %w", err)
		}
		if err := instance.Validate(); err != nil {
			return nil, err
		}
		if instance.InstanceID != ids[index] || instance.CandidacyClass() != ControlCandidacyPreferred || !instance.ExpiresAt.After(at) {
			continue
		}
		candidates = append(candidates, instance)
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].InstanceID < candidates[right].InstanceID })
	return candidates, nil
}

// ReadInstance distinguishes absence from an invalid stored record. Expiry is
// returned as a fact for the caller to judge, as it is in ReadWorker.
func (store *RedisStore) ReadInstance(ctx context.Context, instanceID string) (InstanceRegistration, bool, error) {
	if store == nil || store.client == nil || instanceID == "" {
		return InstanceRegistration{}, false, errors.New("alarmd ownership: instance read needs a store and an identity")
	}
	payload, err := store.client.Get(ctx, store.instanceKey(instanceID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return InstanceRegistration{}, false, nil
	}
	if err != nil {
		return InstanceRegistration{}, false, err
	}
	var instance InstanceRegistration
	if err := json.Unmarshal(payload, &instance); err != nil {
		return InstanceRegistration{}, false, fmt.Errorf("alarmd ownership: decode instance registration: %w", err)
	}
	if err := instance.Validate(); err != nil {
		return InstanceRegistration{}, false, err
	}
	if instance.InstanceID != instanceID {
		return InstanceRegistration{}, false, errors.New("alarmd ownership: stored instance identity differs from requested identity")
	}
	return instance, true, nil
}
