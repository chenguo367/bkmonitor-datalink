package ui

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The authorization page's checks above read its source. This one runs its
// script against a stubbed document and fetch, so the three behaviours an
// operator depends on are executed rather than spelled: the origin check
// before the button, the fallback to the server's own sentence, and the
// answer when whatever replied was not alarmd.
func TestTheAuthorizationPageRunsItsRefusalHandling(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH, so the authorization page's script is NOT executed by this run -- " +
			"only its source was read")
	}
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, httptest.NewRequest("GET", "/cli", nil))
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cli.html"), w.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "run.js"), []byte(cliPageHarness), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(node, filepath.Join(dir, "run.js"), filepath.Join(dir, "cli.html")).CombinedOutput()
	if err != nil {
		t.Fatalf("the page's script failed: %v\n%s", err, output)
	}
	var runs map[string]cliPageRun
	if err := json.Unmarshal(output, &runs); err != nil {
		t.Fatalf("harness output: %v\n%s", err, output)
	}
	run := func(name string) cliPageRun {
		t.Helper()
		result, ok := runs[name]
		if !ok {
			t.Fatalf("no run %q", name)
		}
		return result
	}

	const entry = "http://apps.example.test/kingeye-alarmd/"
	if got := run("same_origin"); got.IssueDisabled || got.Error {
		t.Errorf("opened from the configured entry, generation is offered: %+v", got)
	}
	// The browser's own origin form: a default port and an upper-case host in
	// the address bar are the same origin as the entry.
	if got := run("same_origin_other_spelling"); got.IssueDisabled || got.Error {
		t.Errorf("the same origin spelled with :80 and upper case is still the entry: %+v", got)
	}
	got := run("other_origin")
	if !got.IssueDisabled || !got.Error || !strings.Contains(got.Status, "http://10.0.0.1:8080") ||
		!strings.Contains(got.Status, entry+"cli") || got.Requests != 1 {
		t.Errorf("opened from another origin, the page says where to open it and offers nothing to press: %+v", got)
	}
	if got := run("refused_after_preview"); !strings.Contains(got.Status, entry+"cli") || !got.Error {
		t.Errorf("a generation refused for its origin says the entry the preview named: %+v", got)
	}

	for _, code := range []string{"admin_unauthorized", "admin_not_configured", "auth_rate_limited",
		"auth_busy", "auth_store_unavailable", "not_found"} {
		got := run("code:" + code)
		if !got.Error || got.Status == "" || got.Status == "server sentence for "+code {
			t.Errorf("%s reaches the operator as what to do, not the server's sentence: %+v", code, got)
		}
		if got.KeyKept {
			t.Errorf("%s: a refused key stays in the field", code)
		}
	}
	// Codes the page has no line for, including ones that name a property
	// every object inherits, fall back to the server's sentence.
	for _, code := range []string{"something_new", "constructor", "toString", "__proto__"} {
		if got := run("code:" + code); got.Status != "server sentence for "+code || !got.Error {
			t.Errorf("%s falls back to the server's sentence: %+v", code, got)
		}
	}
	if got := run("code_without_sentence"); got.Status == "" || !got.Error {
		t.Errorf("a refusal with neither a known code nor a sentence still says something: %+v", got)
	}

	// A body that is not JSON and a request that never got an answer are the
	// same fact for the operator: alarmd's route did not answer.
	notJSON, network := run("not_json"), run("network_failure")
	if notJSON.Status == "" || notJSON.Status != network.Status || !notJSON.Error ||
		!strings.Contains(notJSON.Status, "/api/cli/") {
		t.Errorf("an answer that is not alarmd's names the route to check: %+v / %+v", notJSON, network)
	}
}

type cliPageRun struct {
	Status        string `json:"status"`
	Error         bool   `json:"error"`
	IssueDisabled bool   `json:"issue_disabled"`
	KeyKept       bool   `json:"key_kept"`
	Requests      int    `json:"requests"`
}

const cliPageHarness = `
'use strict';
const fs = require('fs');
const html = fs.readFileSync(process.argv[2], 'utf8');
const script = html.slice(html.indexOf('<script>') + 8, html.lastIndexOf('</script>'));

function load(pageURL, respond) {
  const elements = {};
  const element = id => elements[id] || (elements[id] = {
    id, textContent: '', value: '', disabled: false, hidden: false, className: '', href: '', listeners: {},
    addEventListener(type, listener) { this.listeners[type] = listener; }, focus() {}, select() {},
  });
  let requests = 0;
  const context = {
    document: { getElementById: element },
    location: new URL(pageURL),
    addEventListener() {},
    navigator: { clipboard: { writeText: async () => {} } },
    fetch: async (url, options) => { requests++; return respond(url, options); },
  };
  new Function(...Object.keys(context), script)(...Object.values(context));
  return { elements, requests: () => requests };
}

const answer = (status, body) => ({ ok: status < 400, status, json: async () => body });
const preview = { environment_id: 'ns/release', environment_name: 'ns/release',
  public_base_url: 'http://apps.example.test/kingeye-alarmd/' };
const entryPage = 'http://apps.example.test/kingeye-alarmd/cli';

async function inspect(pageURL, respond, then) {
  const page = load(pageURL, respond);
  const e = page.elements;
  e['admin-key'].value = 'k'.repeat(40);
  await e.inspect.listeners.click();
  if (then) await then(e);
  return { status: e.status.textContent, error: e.status.className === 'error', issue_disabled: e.issue.disabled,
    key_kept: e['admin-key'].value !== '', requests: page.requests() };
}

(async () => {
  const runs = {};
  runs.same_origin = await inspect(entryPage, () => answer(200, preview));
  runs.same_origin_other_spelling = await inspect('http://APPS.example.test:80/kingeye-alarmd/cli', () => answer(200, preview));
  runs.other_origin = await inspect('http://10.0.0.1:8080/kingeye-alarmd/cli', () => answer(200, preview));
  runs.refused_after_preview = await inspect(entryPage,
    (url, options) => options.method === 'GET' ? answer(200, preview)
      : answer(403, { status: 'error', error: { code: 'origin_denied', message: 'server sentence for origin_denied' } }),
    e => e.issue.listeners.click());
  for (const code of ['admin_unauthorized', 'admin_not_configured', 'auth_rate_limited', 'auth_busy',
    'auth_store_unavailable', 'not_found', 'something_new', 'constructor', 'toString', '__proto__']) {
    runs['code:' + code] = await inspect(entryPage,
      () => answer(403, { status: 'error', error: { code, message: 'server sentence for ' + code } }));
  }
  runs.code_without_sentence = await inspect(entryPage, () => answer(500, { status: 'error', error: { code: 'something_new' } }));
  runs.not_json = await inspect(entryPage,
    () => ({ ok: false, status: 404, json: async () => { throw new SyntaxError('Unexpected token <'); } }));
  runs.network_failure = await inspect(entryPage, () => { throw new TypeError('Failed to fetch'); });
  process.stdout.write(JSON.stringify(runs));
})().catch(error => { console.error(error); process.exit(1); });
`
