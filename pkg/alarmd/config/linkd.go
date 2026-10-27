package config

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"time"
)

// LinkdConfig is the alert link as this deployment reaches it: the Console's
// address and its credentials, and nothing else. The link's target - its
// source, hook, prefix, where its sets are and which sources share them - is
// read from the Console, which lists exactly one target for a link alarmd
// reads; nothing is read until the Console names the place.
//
// Connection and KeyPrefix are where discovery found the sets, written back
// once the Console has named them, and are never decoded from the file.
type LinkdConfig struct {
	Connection *RedisConnectionConfig `yaml:"-"`
	KeyPrefix  string                 `yaml:"-"`
	// ConsoleURL is the root of the link's API, whole: alarmd appends only
	// each operation's own path (strategy-index/..., alerts/...,
	// event-sources/...). Where the link serves it is the deployment's to
	// say - the admin's linkd module at <admin><base_path>/api/modules/linkd.
	// A URL naming the server
	// rather than this root is refused at startup discovery (the server's web
	// page answers there), never read as an empty answer.
	ConsoleURL string `yaml:"console_url"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	// UsernameFile and PasswordFile name files holding the credentials
	// (resolveCredentials), cleared once read.
	UsernameFile string `yaml:"username_file"`
	PasswordFile string `yaml:"password_file"`
	// ControlPlane is the link's control plane, whose management API the
	// control leader closes alerts through (package filterclose). Nothing
	// switches it on: with a control plane and its secret named, the leader
	// asks it each round, and a link whose storage cannot search (501) or
	// that does not serve the routes leaves the deployment on its existing
	// close paths, counted.
	ControlPlane LinkdControlPlaneConfig `yaml:"control_plane"`
}

// LinkdControlPlaneConfig is the link's control plane as alarmd reaches it:
// the API's address and the credential its management routes take - an
// Internal-Token signed with the link's dispatch JWT secret, which is given
// by file only (JWTSecretFile) and never written in the file.
type LinkdControlPlaneConfig struct {
	// URL is the link control plane's address, without its /api/v1 routes.
	URL           string `yaml:"url"`
	JWTSecretFile string `yaml:"jwt_secret_file"`
	// JWTSecret is resolved from JWTSecretFile (resolveCredentials).
	JWTSecret string `yaml:"-" json:"-"`
	// InlineJWTSecret is only here to be refused: the secret is given by
	// file, never written in the configuration.
	InlineJWTSecret string `yaml:"jwt_secret" json:"-"`
	// JWTUsername is the caller's identity in the token; alarmd when unset.
	JWTUsername string `yaml:"jwt_username"`
	// EventSourceID is the link's source alarmd writes to; built_in_bk when
	// unset. Every filter alarmd sends names it: the link scopes a search
	// to the tenant only.
	EventSourceID string `yaml:"event_source_id"`
}

// Configured says the deployment names the link's control plane and its
// secret. It is where the link is, not a switch: whether its closes are
// used is the link's answer.
func (c LinkdControlPlaneConfig) Configured() bool { return c.URL != "" && c.JWTSecret != "" }

// Username is the token's caller identity.
func (c LinkdControlPlaneConfig) Username() string {
	if c.JWTUsername == "" {
		return "alarmd"
	}
	return c.JWTUsername
}

// Source is the link's source alarmd's events arrive under.
func (c LinkdControlPlaneConfig) Source() string {
	if c.EventSourceID == "" {
		return "built_in_bk"
	}
	return c.EventSourceID
}

func (c LinkdControlPlaneConfig) validate() error {
	if c.InlineJWTSecret != "" {
		return errors.New("linkd control_plane jwt_secret is given by jwt_secret_file only")
	}
	if c.URL == "" {
		return nil
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("linkd control_plane url must be an HTTP service URL without credentials or query")
	}
	// No secret is not refused: the leader counts credentials_missing each
	// round and the deployment stays on its existing close paths.
	for _, value := range []string{c.Username(), c.Source()} {
		if strings.TrimSpace(value) != value || strings.ContainsAny(value, "/?#") {
			return errors.New("linkd control_plane jwt_username and event_source_id must be canonical names")
		}
	}
	return nil
}

// LinkdCalibrationInterval is how often the copy calibrates against the
// link's full sets, and asks the Console again how they are keyed. A
// program constant: the deployment knows no better than this how often that
// should be.
const LinkdCalibrationInterval = 30 * time.Minute

// The Console's Basic Auth may come from the environment instead of the file,
// so that a deployment can hand alarmd the same Secret the alert link's own
// Console is given - its chart keeps the credentials in an existing Secret -
// rather than restating them in alarmd's configuration.
const (
	LinkdConsoleUsernameEnvironment = "ALARMD_LINKD_CONSOLE_USERNAME"
	LinkdConsolePasswordEnvironment = "ALARMD_LINKD_CONSOLE_PASSWORD"
)

// resolveCredentialsFromEnvironment fills the Console credentials from the
// environment. A credential stated in both places is refused: two sources for
// one secret is a deployment that can rotate one and keep using the other.
func (c *LinkdConfig) resolveCredentialsFromEnvironment() error {
	for _, field := range []struct {
		name  string
		value *string
	}{{LinkdConsoleUsernameEnvironment, &c.Username}, {LinkdConsolePasswordEnvironment, &c.Password}} {
		env, ok := os.LookupEnv(field.name)
		if !ok || env == "" {
			continue
		}
		if *field.value != "" {
			return errors.New("linkd console credentials are set both in the file and in " + field.name)
		}
		*field.value = env
	}
	return nil
}

func (c LinkdConfig) Prefix() string {
	if c.KeyPrefix == "" {
		return "alarmd:open_alerts"
	}
	return c.KeyPrefix
}

func (c LinkdConfig) Validate() error {
	if err := c.ControlPlane.validate(); err != nil {
		return err
	}
	if c.ConsoleURL == "" {
		if c.Username != "" || c.Password != "" {
			return errors.New("linkd console_url is required for credentials")
		}
		return nil
	}
	u, err := url.Parse(c.ConsoleURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("linkd console_url must be an HTTP service URL without credentials or query")
	}
	// The API root always has a path: no server alarmd reads serves the link's
	// operations at its own root, where its web page is.
	if strings.Trim(u.Path, "/") == "" {
		return errors.New("linkd console_url must name the link's API root, not the server: " +
			"<admin><base_path>/api/modules/linkd")
	}
	// A Basic pair is a Console's own credential. Without one the calls carry
	// the token the admin hands over (linkdtoken): the admin serving the API
	// accepts nothing else, and keeps no account for alarmd to configure.
	if (c.Username == "") != (c.Password == "") {
		return errors.New("linkd console username and password come together, or neither for the admin's token")
	}
	return nil
}

type LinkdCapacity struct{ Bytes, Strategies, Members, LocalEntries, ReadBatch, GroupBatch, CloseBatch int }

// linkdCloseBatch is how many closes one step sends. A close is a Kafka
// write, which takes thousands a second; it is not CPU work to ration.
const linkdCloseBatch = 1000

func DeriveLinkdCapacity(in CapacityInputs) LinkdCapacity {
	// The index and calibration metadata receive a sixteenth of container
	// memory, about 512 bytes an alert: 64k alerts and 16k strategies at
	// 512Mi. Local ACK/owner bookkeeping has its own derived limit and the
	// calendar cache its own share. These admission estimates are not an RSS
	// cap, and they are not taken out of the execution share: a share filled
	// to its bound puts the planned heap about a fifteenth over the Go soft
	// limit (40Mi at 512Mi), which is collection pressure, not an OOM, and is
	// reached only at tens of thousands of open alerts. Reserving it would
	// cut a third of the executions for memory that is almost never held.
	bytes := int(min(in.MemoryLimitBytes/16, uint64(^uint(0)>>1)))
	if bytes < 64<<10 {
		return LinkdCapacity{}
	}
	ops := max(1, min(in.CPUBudget*4, bytes/4096))
	return LinkdCapacity{Bytes: bytes, Strategies: bytes / 2048, Members: bytes / 512,
		LocalEntries: bytes / 2048, ReadBatch: ops, GroupBatch: ops, CloseBatch: linkdCloseBatch}
}

// DeriveCalendarCacheCapacity is the legacy calendar (effective time) cache's
// own share: a sixty-fourth of container memory, about 2 KiB an entry.
func DeriveCalendarCacheCapacity(in CapacityInputs) (entries, bytes int) {
	bytes = int(min(in.MemoryLimitBytes/64, uint64(^uint(0)>>1)))
	return bytes / 2048, bytes
}
