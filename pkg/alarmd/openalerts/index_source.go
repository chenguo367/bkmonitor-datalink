// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var ErrIncomplete = errors.New("alarmd openalerts: incomplete index observation")
var ErrCapacity = errors.New("alarmd openalerts: cache capacity reached")

// IndexSource returns only a complete bounded read. Empty is an index
// observation, never proof that the alert store has no active alerts.
type IndexSource interface {
	ReadSet(context.Context, StrategyKey) ([]string, error)
}

type Subscriber interface {
	Watch(context.Context, func(bool), func(StrategyKey)) error
}

type ReadLimits struct {
	MaxMembers int
	MaxBytes   int
	MaxPages   int
	PageSize   int64
}

type SetSource struct {
	client redis.Cmdable
	prefix string
	limits ReadLimits
}

func validPrefix(prefix string) bool {
	return prefix != "" && len(prefix) <= 256 && strings.TrimSpace(prefix) == prefix
}

func validStrategyKey(key StrategyKey) bool {
	// A delimiter in either identity aliases another tenant/strategy pair.
	return key.TenantID != "" && len(key.TenantID) <= 256 && key.StrategyID != "" && len(key.StrategyID) <= 1024 &&
		!strings.ContainsAny(key.TenantID+key.StrategyID, ":\x00")
}

func NewSetSource(client redis.Cmdable, prefix string, limits ReadLimits) (*SetSource, error) {
	if client == nil || !validPrefix(prefix) || limits.MaxMembers <= 0 || limits.MaxBytes <= 0 || limits.MaxPages <= 0 || limits.PageSize <= 0 {
		return nil, errors.New("alarmd openalerts: Redis client, prefix and positive read limits are required")
	}
	return &SetSource{client: client, prefix: prefix, limits: limits}, nil
}

func (source *SetSource) ReadSet(ctx context.Context, key StrategyKey) ([]string, error) {
	if !validStrategyKey(key) {
		return nil, errors.New("alarmd openalerts: invalid strategy identity")
	}
	name := source.prefix + ":" + key.TenantID + ":" + key.StrategyID
	before, err := source.client.SCard(ctx, name).Result()
	if err != nil {
		return nil, err
	}
	if before > int64(source.limits.MaxMembers) {
		return nil, ErrIncomplete
	}
	members := make(map[string]struct{})
	var cursor uint64
	bytes := 0
	for page := 0; page < source.limits.MaxPages; page++ {
		values, next, err := source.client.SScan(ctx, name, cursor, "", source.limits.PageSize).Result()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			bytes += len(value)
			if value == "" || len(value) > 4096 || bytes > source.limits.MaxBytes {
				return nil, ErrIncomplete
			}
			members[value] = struct{}{}
			if len(members) > source.limits.MaxMembers {
				return nil, ErrIncomplete
			}
		}
		cursor = next
		if cursor == 0 {
			after, err := source.client.SCard(ctx, name).Result()
			if err != nil {
				return nil, err
			}
			if after != before || int64(len(members)) != after {
				return nil, ErrIncomplete
			}
			result := make([]string, 0, len(members))
			for value := range members {
				result = append(result, value)
			}
			return result, nil
		}
	}
	return nil, ErrIncomplete
}

// RedisSubscriber owns only its PubSub connection, never the supplied client.
// Receive exposes subscription acknowledgements (including reconnects), unlike
// Channel which hides them. Acknowledgement always schedules a bounded reread.
type RedisSubscriber struct {
	client  redis.UniversalClient
	channel string
	retry   time.Duration
}

func NewRedisSubscriber(client redis.UniversalClient, prefix string, retry time.Duration) (*RedisSubscriber, error) {
	if client == nil || !validPrefix(prefix) || retry <= 0 {
		return nil, errors.New("alarmd openalerts: subscriber client, prefix and retry interval are required")
	}
	return &RedisSubscriber{client: client, channel: prefix + ":changes", retry: retry}, nil
}

func (subscriber *RedisSubscriber) Watch(ctx context.Context, ready func(bool), changed func(StrategyKey)) error {
	for ctx.Err() == nil {
		pubsub := subscriber.client.Subscribe(ctx, subscriber.channel)
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = pubsub.Close()
			case <-done:
			}
		}()
		for ctx.Err() == nil {
			message, err := pubsub.Receive(ctx)
			if err != nil {
				break
			}
			switch message := message.(type) {
			case *redis.Subscription:
				if message.Kind == "subscribe" && message.Channel == subscriber.channel {
					ready(true)
				}
			case *redis.Message:
				if message.Channel != subscriber.channel || len(message.Payload) > 64<<10 {
					continue
				}
				var notice struct {
					Tenant   string `json:"bk_tenant_id"`
					Strategy string `json:"strategy_id"`
				}
				decoder := json.NewDecoder(strings.NewReader(message.Payload))
				decoder.DisallowUnknownFields()
				if decoder.Decode(&notice) != nil {
					continue
				}
				var extra any
				if decoder.Decode(&extra) != io.EOF {
					continue
				}
				key := StrategyKey{TenantID: notice.Tenant, StrategyID: notice.Strategy}
				if validStrategyKey(key) {
					changed(key)
				}
			}
		}
		close(done)
		_ = pubsub.Close()
		ready(false)
		if !waitIndex(ctx, subscriber.retry) {
			break
		}
	}
	return ctx.Err()
}

func waitIndex(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// HasOpenAlerts reports whether the link holds at least one unrecovered
// alert for the strategy, as one point read.
//
// It exists for the control leader's difference, which asks the question for
// each strategy the catalog let go. Asked the other way round - walk the
// index and list every strategy in it - the cost follows the size of the
// whole database rather than the number of alerts, because a cursor with a
// pattern still visits every key, and on a deployment that shares this
// database with the runtime state that is a periodic pass over hundreds of
// thousands of keys that have nothing to do with alerts. One point read per
// departed strategy replaces it.
//
// A set exists only while it has a member, so existence is the answer. The
// error is returned rather than folded into false: not read is not the same
// as read and empty, and only the second may be taken as "nothing to close".
func (source *SetSource) HasOpenAlerts(ctx context.Context, key StrategyKey) (bool, error) {
	if source == nil {
		return false, errors.New("alarmd openalerts: no index source")
	}
	if !validStrategyKey(key) {
		return false, errors.New("alarmd openalerts: invalid strategy identity")
	}
	members, err := source.client.SCard(ctx, source.prefix+":"+key.TenantID+":"+key.StrategyID).Result()
	if err != nil {
		return false, err
	}
	return members > 0, nil
}
