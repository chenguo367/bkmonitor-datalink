// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package cli

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// deployment is one fake deployment of an environment: it exchanges a code
// for its own session and answers describe as itself, counting the channel
// calls that reach it.
type deployment struct {
	server  *httptest.Server
	profile Profile
	calls   atomic.Int64
}

func newDeployment(t *testing.T, session string) *deployment {
	t.Helper()
	d := &deployment{}
	d.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ob/api/cli/auth/exchange":
			writeJSON(t, w, sessionResponse(d.profile))
		case "/ob/api/cli/channel":
			d.calls.Add(1)
			writeJSON(t, w, envelope(d.profile, "ok", map[string]any{"input_schema": map[string]any{}}))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(d.server.Close)
	d.profile = fixtureProfile(d.server.URL)
	d.profile.SessionID = session
	return d
}

// derivedName is the profile name a login from d takes when none is given.
func (d *deployment) derivedName(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(d.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(u.Hostname(), ".", "-") + "-" + u.Port()
}

func profileNames(t *testing.T, store Store) map[string]map[string]any {
	t.Helper()
	listed, err := store.list()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]any{}
	for name, row := range listed["profiles"].(map[string]any) {
		out[name] = row.(map[string]any)
	}
	return out
}

// Two deployments that report the same environment_id log in side by side:
// each login keeps its own profile, named after its entry, bound to its own
// address. A remote command naming the shared environment_id is refused by
// name, listing the profiles to choose from; one naming a profile goes to
// that profile's entry alone.
func TestTwoDeploymentsOfOneEnvironmentLogInSideBySide(t *testing.T) {
	a, b := newDeployment(t, "session-a"), newDeployment(t, "session-b")
	store := Store{t.TempDir()}
	client := a.server.Client()
	for _, d := range []*deployment{a, b} {
		code, result, out, errText := run(t, store, client, bundle(d.profile), "auth", "login")
		if code != 0 || stringField(objectField(result, "session"), "profile") != d.derivedName(t) {
			t.Fatalf("login from %s: %d %s %s", d.server.URL, code, out, errText)
		}
	}
	names := profileNames(t, store)
	if len(names) != 2 {
		t.Fatalf("profiles %v, want one per deployment", names)
	}
	for _, d := range []*deployment{a, b} {
		row := names[d.derivedName(t)]
		if row["environment_id"] != "env-one" || row["public_base_url"] != d.profile.PublicBaseURL || row["name"] != d.derivedName(t) {
			t.Fatalf("profile %s lists %v", d.derivedName(t), row)
		}
	}
	code, result, out, _ := run(t, store, client, "", "describe", "x", "--env", "env-one")
	if code != 2 || stringField(objectField(result, "error"), "code") != "ambiguous_environment" ||
		!strings.Contains(out, a.derivedName(t)) || !strings.Contains(out, b.derivedName(t)) {
		t.Fatalf("a shared environment_id answered %d %s, want ambiguous_environment naming both profiles", code, out)
	}
	if a.calls.Load()+b.calls.Load() != 0 {
		t.Fatal("an ambiguous --env reached a deployment")
	}
	if code, _, out, _ := run(t, store, client, "", "describe", "x", "--env", b.derivedName(t)); code != 0 || b.calls.Load() != 1 || a.calls.Load() != 0 {
		t.Fatalf("--env %s answered %d %s, a %d b %d calls", b.derivedName(t), code, out, a.calls.Load(), b.calls.Load())
	}
	if code, _, out, _ := run(t, store, client, "", "describe", "x", "--env", a.derivedName(t)); code != 0 || a.calls.Load() != 1 {
		t.Fatalf("--env %s answered %d %s", a.derivedName(t), code, out)
	}
}

// A profile whose entry changed is refused as before; --rebind moves only the
// profile it names, and a profile named for another environment is refused
// outright.
func TestAProfileWhoseEntryChangedIsRefusedAndRebindMovesOnlyIt(t *testing.T) {
	a, b, c := newDeployment(t, "session-a"), newDeployment(t, "session-b"), newDeployment(t, "session-c")
	store := Store{t.TempDir()}
	client := a.server.Client()
	if code, _, out, _ := run(t, store, client, bundle(a.profile), "auth", "login", "--profile", "one"); code != 0 {
		t.Fatalf("login one: %s", out)
	}
	if code, _, out, _ := run(t, store, client, bundle(b.profile), "auth", "login", "--profile", "two"); code != 0 {
		t.Fatalf("login two: %s", out)
	}
	code, _, out, _ := run(t, store, client, bundle(c.profile), "auth", "login", "--profile", "one")
	if code == 0 || !strings.Contains(out, "origin changed") {
		t.Fatalf("profile one moved to another entry without --rebind: %d %s", code, out)
	}
	if code, _, out, _ := run(t, store, client, bundle(c.profile), "auth", "login", "--profile", "one", "--rebind"); code != 0 {
		t.Fatalf("rebind one: %s", out)
	}
	names := profileNames(t, store)
	if names["one"]["public_base_url"] != c.profile.PublicBaseURL || names["two"]["public_base_url"] != b.profile.PublicBaseURL {
		t.Fatalf("after rebinding one: %v", names)
	}
	other := newDeployment(t, "session-other")
	other.profile.EnvironmentID = "env-two"
	if code, _, out, _ := run(t, store, client, bundle(other.profile), "auth", "login", "--profile", "two", "--rebind"); code == 0 {
		t.Fatalf("profile two taken by another environment: %s", out)
	}
	if code, _, _, _ := run(t, store, client, "", "describe", "x", "--env", "one", "--profile", "two"); code != 2 {
		t.Fatal("--profile accepted outside auth login and auth listen")
	}
}

// A profile from before profiles had names is keyed by its environment_id: it
// keeps working without a new login while it is the environment's one
// deployment, and a login from its entry updates it in place. When a second
// deployment of the environment logs in, the old profile takes the name its
// own entry gives, credentials and entry with it, and the login says so; the
// environment_id then names no single deployment and is refused as ambiguous.
func TestALegacyProfileKeyedByItsEnvironmentKeepsWorking(t *testing.T) {
	a, b := newDeployment(t, "session-a"), newDeployment(t, "session-b")
	dir := t.TempDir()
	legacy := map[string]any{"profiles": map[string]any{"env-one": a.profile}, "default_environment": "env-one"}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "profiles.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	store := Store{dir}
	client := a.server.Client()
	if code, _, out, _ := run(t, store, client, "", "describe", "x", "--env", "env-one"); code != 0 || a.calls.Load() != 1 {
		t.Fatalf("legacy profile: %d %s", code, out)
	}
	a.profile.ExpiresAt = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	code, result, out, _ := run(t, store, client, bundle(a.profile), "auth", "login")
	if code != 0 || stringField(objectField(result, "session"), "profile") != "env-one" || len(profileNames(t, store)) != 1 {
		t.Fatalf("a login from the legacy profile's entry: %d %s, profiles %v", code, out, profileNames(t, store))
	}
	code, result, out, errText := run(t, store, client, bundle(b.profile), "auth", "login")
	renamed := objectField(objectField(result, "session"), "profile_renamed")
	if code != 0 || renamed["from"] != "env-one" || renamed["to"] != a.derivedName(t) ||
		!strings.Contains(errText, "env-one") || !strings.Contains(errText, a.derivedName(t)) {
		t.Fatalf("a second deployment's login: %d %s %s, want the old profile renamed and said", code, out, errText)
	}
	names := profileNames(t, store)
	if _, kept := names["env-one"]; kept || names[a.derivedName(t)]["public_base_url"] != a.profile.PublicBaseURL || len(names) != 2 {
		t.Fatalf("profiles after the rename %v", names)
	}
	if listed, _ := store.list(); listed["default_environment"] != a.derivedName(t) {
		t.Fatalf("default environment %v, want it to follow the rename", listed["default_environment"])
	}
	code, result, out, _ = run(t, store, client, "", "describe", "x", "--env", "env-one")
	if code != 2 || stringField(objectField(result, "error"), "code") != "ambiguous_environment" || a.calls.Load() != 1 || b.calls.Load() != 0 {
		t.Fatalf("the environment_id after a second deployment: %d %s", code, out)
	}
	if code, _, out, _ := run(t, store, client, "", "describe", "x", "--env", a.derivedName(t)); code != 0 || a.calls.Load() != 2 {
		t.Fatalf("the renamed profile without a new login: %d %s", code, out)
	}
	// The rename rewrote profiles.json in place: still 0600, and no temporary file of the write left beside it (results/
	// is the commands' evidence, written before the rename).
	info, err := os.Stat(filepath.Join(dir, "profiles.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("profiles.json after the rename: %v %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".alarmd-") {
			t.Fatalf("the rename left %s beside profiles.json", entry.Name())
		}
	}
}

// A ref that names one profile and is another environment's environment_id is refused as ambiguous: --env test meant
// for the environment test must not reach a profile another environment's entry happened to name test.
func TestAProfileNameThatIsAnotherEnvironmentsIDIsAmbiguous(t *testing.T) {
	c := &config{Profiles: map[string]Profile{
		"test":   {EnvironmentID: "team/one", PublicBaseURL: "https://apps.test.example.com/admin/alarmd/"},
		"deploy": {EnvironmentID: "test", PublicBaseURL: "https://apps.deploy.example.com/admin/alarmd/"},
	}}
	_, _, err := resolve(c, "test")
	var ambiguous *ambiguousProfile
	if !errors.As(err, &ambiguous) || len(ambiguous.names) != 2 {
		t.Fatalf("resolve(test) gave %v, want ambiguous over the profile test and environment test's profile", err)
	}
	if name, ok, err := resolve(c, "deploy"); err != nil || !ok || name != "deploy" {
		t.Fatalf("resolve(deploy) %q %v %v", name, ok, err)
	}
}

// A login without --profile is named after the deployment in its entry's host;
// an address keeps its port. Two environments behind one host keep apart: the
// second takes its environment_id when the host's name is the first's.
func TestADefaultProfileNameComesFromTheEntry(t *testing.T) {
	for entry, want := range map[string]string{
		"https://apps.deploy-two.example.com/admin/alarmd/": "deploy-two",
		"https://apps.deploy-one.example.com/admin/alarmd/": "deploy-one",
		"https://monitor.example.com/alarmd/":               "monitor.example.com",
		"https://192.0.2.10:8443/ob/":                       "192-0-2-10-8443",
	} {
		if got := derivedProfileName(entry); got != want {
			t.Errorf("%s names %q, want %q", entry, got, want)
		}
	}
	c := &config{Profiles: map[string]Profile{}}
	first := Profile{EnvironmentID: "team/one", PublicBaseURL: "https://apps.deploy-one.example.com/monitor/"}
	c.Profiles[bindingName(c, first)] = first
	second := Profile{EnvironmentID: "team/two", PublicBaseURL: "https://apps.deploy-one.example.com/admin/alarmd/"}
	if name := bindingName(c, second); name != "team/two" || checkBinding(c, name, second, false) != nil {
		t.Fatalf("a second environment behind the host is named %q", name)
	}
	if name := bindingName(c, first); name != "deploy-one" {
		t.Fatalf("a login again from the first entry is named %q, want its profile", name)
	}
	// team/two's environment_id named its first deployment; a second one, its
	// host's name held by team/one, does not take the environment's name, or
	// --env team/two would reach one deployment without saying so.
	c.Profiles["deploy-two"] = Profile{EnvironmentID: "team/two", PublicBaseURL: "https://apps.deploy-two.example.com/admin/alarmd/"}
	third := Profile{EnvironmentID: "team/two", PublicBaseURL: "https://apps.deploy-one.example.com/other/alarmd/"}
	if name := bindingName(c, third); name == "team/two" || name == "deploy-one" {
		t.Fatalf("another deployment of team/two is named %q", name)
	}
}
