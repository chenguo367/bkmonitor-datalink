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
}

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
	return nil
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
	return store.client.Set(ctx, store.instanceKey(instance.InstanceID), payload, ttl).Err()
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
