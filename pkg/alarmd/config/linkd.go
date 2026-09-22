package config

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

// LinkdConfig binds the external index to the source that consumes this
// worker's native events. An omitted connection reuses runtime Redis.
// Capacity follows the container; operators specify locations and acceptable
// reconciliation delay, not mutually dependent entry/byte/queue limits.
type LinkdConfig struct {
	Connection        *RedisConnectionConfig `yaml:"connection"`
	KeyPrefix         string                 `yaml:"key_prefix"`
	ConsoleURL        string                 `yaml:"console_url"`
	EventSourceID     string                 `yaml:"event_source_id"`
	HookName          string                 `yaml:"hook_name"`
	SharedSources     []string               `yaml:"shared_sources"`
	Username          string                 `yaml:"username"`
	Password          string                 `yaml:"password"`
	ReconcileInterval Duration               `yaml:"reconcile_interval"`
}

func (c LinkdConfig) Prefix() string {
	if c.KeyPrefix == "" {
		return "alarmd:open_alerts"
	}
	return c.KeyPrefix
}

func (c LinkdConfig) CalibrationInterval() time.Duration {
	if c.ReconcileInterval == 0 {
		return 30 * time.Minute
	}
	return c.ReconcileInterval.Duration()
}

func (c LinkdConfig) Validate() error {
	if strings.TrimSpace(c.Prefix()) != c.Prefix() || strings.ContainsAny(c.Prefix(), "\r\n\x00") {
		return errors.New("linkd key_prefix is invalid")
	}
	if c.CalibrationInterval() < time.Minute {
		return errors.New("linkd reconcile_interval must be at least one minute")
	}
	if c.Connection != nil {
		if err := c.Connection.validate("phase_two.linkd.connection"); err != nil {
			return err
		}
	}
	if c.ConsoleURL == "" {
		if c.EventSourceID != "" || c.HookName != "" || c.Username != "" || c.Password != "" {
			return errors.New("linkd console_url is required for source binding and credentials")
		}
		return nil
	}
	u, err := url.Parse(c.ConsoleURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("linkd console_url must be an HTTP service URL without credentials or query")
	}
	if c.EventSourceID == "" || c.HookName == "" {
		return errors.New("linkd reconciliation requires event_source_id and hook_name")
	}
	if c.Username == "" || c.Password == "" {
		return errors.New("linkd console service requires username and password")
	}
	return nil
}

type LinkdCapacity struct{ Bytes, Strategies, Members, LocalEntries, ReadBatch, GroupBatch, CloseBatch int }

func DeriveLinkdCapacity(in CapacityInputs) LinkdCapacity {
	// The index and calibration metadata receive one percent of container
	// memory. Local ACK/owner bookkeeping and the legacy calendar cache have
	// separate derived limits. These admission estimates are not an RSS cap.
	bytes := int(min(in.MemoryLimitBytes/100, uint64(^uint(0)>>1)))
	if bytes < 64<<10 {
		return LinkdCapacity{}
	}
	ops := max(1, min(in.CPUBudget*4, bytes/4096))
	return LinkdCapacity{Bytes: bytes, Strategies: bytes / 2048, Members: bytes / 512,
		LocalEntries: bytes / 2048, ReadBatch: ops, GroupBatch: ops, CloseBatch: min(64, ops*4)}
}
