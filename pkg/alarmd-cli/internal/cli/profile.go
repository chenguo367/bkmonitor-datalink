// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package cli

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

type Profile struct {
	EnvironmentID   string `json:"environment_id"`
	EnvironmentName string `json:"environment_name"`
	PublicBaseURL   string `json:"public_base_url"`
	// FallbackBaseURLs are the deployment's own addresses, declared by the
	// server at login for a session issued through the admin: where a
	// request goes when the admin entry cannot get it to alarmd. Only the
	// server's answer sets them (validateExchange).
	FallbackBaseURLs []string `json:"fallback_base_urls,omitempty"`
	AccessToken      string   `json:"access_token,omitempty"`
	SessionID        string   `json:"session_id,omitempty"`
	ExpiresAt        string   `json:"expires_at,omitempty"`
	Scope            string   `json:"scope,omitempty"`
	CACert           string   `json:"ca_cert,omitempty"`
	InsecureTLS      bool     `json:"insecure_tls,omitempty"`
	// RefreshToken is the pairing's renewal credential: spent for a new
	// session and the next credential whenever the session runs out.
	RefreshToken string `json:"refresh_token,omitempty"`
	PairingID    string `json:"pairing_id,omitempty"`
	// Pairing is what the server said about pairing this session: paired,
	// pairing_limit_reached, not_offered (a server that does not pair) or
	// upgrade_refused. Empty is a session from before this client, which is
	// offered for pairing once.
	Pairing string `json:"pairing,omitempty"`
	// via is the entry a request through this profile names (onEntry);
	// never stored.
	via string
	// name is the profile's name, its key in profiles.json: set when it is
	// read, or the name a login asked for; never stored inside the profile.
	name string
}

// A profile is named, and the name is its key: several deployments can
// report one environment_id, and each keeps its own entry. A profile from
// before names is keyed by its environment_id, which stays its name.
var profileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$`)

func validProfileName(name string) bool { return profileNamePattern.MatchString(name) }

// ref is how a command names p again: its profile name, or for a profile
// built before names, its environment_id.
func (p Profile) ref() string {
	if p.name != "" {
		return p.name
	}
	return p.EnvironmentID
}

// genericHostLabels are the leading host labels that name a service rather
// than a deployment, skipped when a profile is named after its entry.
var genericHostLabels = map[string]bool{"apps": true, "app": true, "www": true, "api": true, "bkapi": true, "paas": true,
	"alarmd": true, "admin": true, "monitor": true, "ob": true}

// derivedProfileName is the name a login takes from its entry when it is not
// given one: the first label of the host that names the deployment (apps.<name>.
// example.com is <name>), or an address with its port.
func derivedProfileName(publicBaseURL string) string {
	u, err := baseURL(publicBaseURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil {
		name := strings.NewReplacer(".", "-", ":", "-").Replace(host)
		if u.Port() != "" {
			name += "-" + u.Port()
		}
		return name
	}
	labels := strings.Split(host, ".")
	for index, label := range labels {
		if index < len(labels)-2 && !genericHostLabels[label] && validProfileName(label) {
			return label
		}
	}
	return host
}

// ambiguousProfile is an environment_id that more than one profile is bound
// to: the command names one of them.
type ambiguousProfile struct {
	environment string
	names       []string
}

func (e *ambiguousProfile) Error() string {
	return fmt.Sprintf("environment %s has profiles %s; name one with --env <profile>", e.environment, strings.Join(e.names, ", "))
}

// resolve is the profile ref names: a profile of that name, else the one
// profile bound to that environment_id. More than one is ambiguous, and so
// is a ref that names one profile while another profile's environment_id is
// that ref: a command meant for that environment must not reach the profile
// that happens to share its name.
func resolve(c *config, ref string) (string, bool, error) {
	var names []string
	for name, p := range c.Profiles {
		if p.EnvironmentID == ref && name != ref {
			names = append(names, name)
		}
	}
	if _, named := c.Profiles[ref]; named {
		if len(names) == 0 {
			return ref, true, nil
		}
		names = append(names, ref)
	}
	sort.Strings(names)
	switch len(names) {
	case 0:
		return "", false, nil
	case 1:
		return names[0], true, nil
	}
	return "", false, &ambiguousProfile{environment: ref, names: names}
}

// bindingName is the profile a login saves into: the name it asked for; else
// the profile already bound to the same environment at the same entry; else
// the name its entry gives, unless another environment behind the same host
// holds it, when the environment_id is the name, or the entry's name with the
// environment_id after it.
func bindingName(c *config, p Profile) string {
	if p.name != "" {
		return p.name
	}
	var same []string
	for name, old := range c.Profiles {
		if old.EnvironmentID == p.EnvironmentID && sameOrigin(old.PublicBaseURL, p.PublicBaseURL) {
			same = append(same, name)
		}
	}
	if len(same) > 0 {
		sort.Strings(same)
		return same[0]
	}
	derived := derivedProfileName(p.PublicBaseURL)
	if held, taken := c.Profiles[derived]; !taken || held.EnvironmentID == p.EnvironmentID {
		return derived
	}
	// The environment_id names the environment's first deployment only: with
	// another deployment of it kept, a profile named after the environment
	// would take every --env that names the environment.
	if _, taken := c.Profiles[p.EnvironmentID]; !taken && validProfileName(p.EnvironmentID) && !hasEnvironment(c, p.EnvironmentID) {
		return p.EnvironmentID
	}
	return freeName(c, derived+"-"+strings.ReplaceAll(p.EnvironmentID, "/", "-"), "")
}

func hasEnvironment(c *config, environment string) bool {
	for _, p := range c.Profiles {
		if p.EnvironmentID == environment {
			return true
		}
	}
	return false
}

// freeName is name, or name with a number after it, whichever no profile
// holds; reserved counts as held.
func freeName(c *config, name, reserved string) string {
	held := func(candidate string) bool {
		_, taken := c.Profiles[candidate]
		return taken || candidate == reserved || !validProfileName(candidate)
	}
	if !held(name) {
		return name
	}
	for index := 2; ; index++ {
		if candidate := fmt.Sprintf("%s-%d", name, index); !held(candidate) {
			return candidate
		}
	}
}

// profileRename is a profile a login moved to another name.
type profileRename struct {
	From, To string
}

// renameEnvironmentNamed: a profile named after its environment_id is that
// environment's one deployment, until p, another of it, is bound under name.
// It then takes the name its own entry gives (or, from the same entry, p
// replaces it under name), with its credentials and entry, and the default
// environment follows it: the environment_id names no single deployment
// any more, and --env with it is refused as ambiguous.
func renameEnvironmentNamed(c *config, name string, p Profile) *profileRename {
	environment := p.EnvironmentID
	old, exists := c.Profiles[environment]
	if name == environment || !exists || old.EnvironmentID != environment {
		return nil
	}
	to := name
	delete(c.Profiles, environment)
	if !sameOrigin(old.PublicBaseURL, p.PublicBaseURL) {
		derived := derivedProfileName(old.PublicBaseURL)
		if held, taken := c.Profiles[derived]; taken && held.EnvironmentID != environment || derived == name || !validProfileName(derived) {
			derived = derived + "-" + strings.ReplaceAll(environment, "/", "-")
		}
		to = freeName(c, derived, name)
		c.Profiles[to] = old
	}
	if c.DefaultEnvironment == environment {
		c.DefaultEnvironment = to
	}
	return &profileRename{From: environment, To: to}
}

type config struct {
	DefaultEnvironment string             `json:"default_environment,omitempty"`
	Profiles           map[string]Profile `json:"profiles"`
}

type Store struct{ Dir string }

func secureDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("configuration directory must be a real directory")
	}
	return os.Chmod(dir, 0700)
}

func regularFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("configuration path must be a regular file")
	}
	return os.Chmod(path, 0600)
}

func (s Store) locked(fn func(*config) (bool, error)) error {
	if err := secureDir(s.Dir); err != nil {
		return err
	}
	lockPath := filepath.Join(s.Dir, ".lock")
	if err := regularFile(lockPath); err != nil {
		return err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := lockFile(f); err != nil {
		return err
	}
	defer unlockFile(f)
	path := filepath.Join(s.Dir, "profiles.json")
	if err := regularFile(path); err != nil {
		return err
	}
	c := config{Profiles: map[string]Profile{}}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &c); err != nil || c.Profiles == nil {
			return errors.New("invalid profiles.json; restore it before continuing")
		}
	}
	changed, err := fn(&c)
	if err != nil || !changed {
		return err
	}
	data, err = json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(path, append(data, '\n'))
}

func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".alarmd-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}

func (s Store) get(env string) (Profile, error) {
	var p Profile
	err := s.locked(func(c *config) (bool, error) {
		name, ok, err := resolve(c, env)
		if err != nil {
			return false, err
		}
		p = c.Profiles[name]
		p.name = name
		if !ok || (p.AccessToken == "" && p.RefreshToken == "") {
			return false, fmt.Errorf("environment has no session; open the deployment's login page (<entry>/cli) and run the auth listen command it shows, or run: alarmd-cli auth login --env %s", env)
		}
		return false, nil
	})
	return p, err
}

// profile is the environment's binding, with or without a session: what
// auth listen --env needs to know where the page lives.
func (s Store) profile(env string) (Profile, error) {
	var p Profile
	err := s.locked(func(c *config) (bool, error) {
		name, ok, err := resolve(c, env)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, errors.New("environment has no profile; use auth listen --url with the entry the authorization page shows")
		}
		p = c.Profiles[name]
		p.name = name
		return false, nil
	})
	return p, err
}

// checkBinding refuses to bind name to p's entry when the profile of that
// name is bound to another environment, or to another entry without rebind:
// a page cannot move a profile somewhere else unasked. Rebind moves only the
// profile it names.
func checkBinding(c *config, name string, p Profile, rebind bool) error {
	if !validProfileName(name) {
		return errors.New("profile names take letters, digits and . _ / - (up to 64); name one with --profile")
	}
	old, exists := c.Profiles[name]
	if !exists {
		return nil
	}
	if old.EnvironmentID != p.EnvironmentID {
		return fmt.Errorf("profile %s belongs to environment %s; name another with --profile", name, old.EnvironmentID)
	}
	if rebind {
		return nil
	}
	a, err := baseURL(old.PublicBaseURL)
	if err != nil {
		return errors.New("stored environment URL is invalid")
	}
	b, err := baseURL(p.PublicBaseURL)
	if err != nil {
		return err
	}
	if a.Scheme != b.Scheme || a.Host != b.Host {
		return fmt.Errorf("environment origin changed for profile %s; verify the new entry and use auth login --profile %s --rebind, or name another profile", name, name)
	}
	return nil
}

func (s Store) checkBinding(p Profile, rebind bool) error {
	_, err := s.bindingName(p, rebind)
	return err
}

// bindingName is the profile p would be saved into, checked.
func (s Store) bindingName(p Profile, rebind bool) (string, error) {
	var name string
	err := s.locked(func(c *config) (bool, error) {
		name = bindingName(c, p)
		return false, checkBinding(c, name, p, rebind)
	})
	return name, err
}

func (s Store) save(p Profile, rebind bool) error {
	_, _, err := s.bind(p, rebind)
	return err
}

// bind saves p into the profile bindingName gives, checked, and says which,
// and which profile it renamed (renameEnvironmentNamed).
func (s Store) bind(p Profile, rebind bool) (string, *profileRename, error) {
	var name string
	var renamed *profileRename
	err := s.locked(func(c *config) (bool, error) {
		name = bindingName(c, p)
		if err := checkBinding(c, name, p, rebind); err != nil {
			return false, err
		}
		renamed = renameEnvironmentNamed(c, name, p)
		p.name = ""
		c.Profiles[name] = p
		return true, nil
	})
	return name, renamed, err
}

// storedName is the key expected was read under: its name, or, for a
// profile built before names, the one profile bound to its environment.
func storedName(c *config, expected Profile) string {
	if expected.name != "" {
		return expected.name
	}
	name, _, _ := resolve(c, expected.EnvironmentID)
	return name
}

func sameSession(a, b Profile) bool {
	return a.SessionID != "" && a.SessionID == b.SessionID && sha256.Sum256([]byte(a.AccessToken)) == sha256.Sum256([]byte(b.AccessToken))
}

// CAS guards both against late replies replacing a new login and late replies
// resurrecting credentials removed by another process.
func (s Store) updateExpiry(expected Profile, expiry string) error {
	t, err := time.Parse(time.RFC3339, expiry)
	if err != nil {
		return errors.New("invalid session expiry")
	}
	return s.locked(func(c *config) (bool, error) {
		name := storedName(c, expected)
		current, ok := c.Profiles[name]
		if !ok || !sameSession(current, expected) {
			return false, nil
		}
		old, _ := time.Parse(time.RFC3339, current.ExpiresAt)
		if !t.After(old) {
			return false, nil
		}
		current.ExpiresAt = expiry
		c.Profiles[name] = current
		return true, nil
	})
}

// updateFallback stores the fallback entries a server declared for the
// session expected still holds; a profile another login replaced meanwhile
// is left alone.
func (s Store) updateFallback(expected Profile, list []string) error {
	return s.locked(func(c *config) (bool, error) {
		name := storedName(c, expected)
		current, ok := c.Profiles[name]
		if !ok || !sameSession(current, expected) || slices.Equal(current.FallbackBaseURLs, list) {
			return false, nil
		}
		current.FallbackBaseURLs = list
		c.Profiles[name] = current
		return true, nil
	})
}

func (s Store) clear(expected Profile) (bool, error) {
	cleared := false
	err := s.locked(func(c *config) (bool, error) {
		name := storedName(c, expected)
		current, ok := c.Profiles[name]
		if !ok || !sameSession(current, expected) {
			return false, nil
		}
		current.AccessToken, current.SessionID, current.ExpiresAt, current.Scope = "", "", "", ""
		current.RefreshToken, current.PairingID, current.Pairing = "", "", ""
		c.Profiles[name] = current
		cleared = true
		return true, nil
	})
	return cleared, err
}

func (s Store) list() (map[string]any, error) {
	result := map[string]any{}
	err := s.locked(func(c *config) (bool, error) {
		profiles := map[string]any{}
		for name, p := range c.Profiles {
			profiles[name] = map[string]any{"name": name, "environment_id": p.EnvironmentID, "environment_name": p.EnvironmentName, "public_base_url": p.PublicBaseURL, "fallback_base_urls": p.FallbackBaseURLs, "session_id": p.SessionID, "expires_at": p.ExpiresAt, "authenticated": p.AccessToken != "", "paired": p.RefreshToken != "", "ca_cert": p.CACert, "insecure_tls": p.InsecureTLS}
		}
		result["profiles"], result["default_environment"] = profiles, c.DefaultEnvironment
		return false, nil
	})
	return result, err
}

func (s Store) use(env string) error {
	return s.locked(func(c *config) (bool, error) {
		name, ok, err := resolve(c, env)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("environment profile does not exist")
		}
		c.DefaultEnvironment = name
		return true, nil
	})
}
