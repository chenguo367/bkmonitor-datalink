package ui

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCLIAuthorizationPageIsEmbeddedAndNotCached(t *testing.T) {
	for _, path := range []string{"/cli", "/cli/", "/cli.html"} {
		w := httptest.NewRecorder()
		Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: %d", path, w.Code)
		}
		body := w.Body.String()
		for _, term := range []string{"api/cli/auth/grants", "confirm:true", "textContent", "pagehide", "alarmd-cli auth login"} {
			if !strings.Contains(body, term) {
				t.Fatalf("missing page contract %s", term)
			}
		}
		if strings.Contains(body, "X-Alarmd-Issuer-Key") || strings.Contains(body, "localStorage") {
			t.Fatal("issuer secret or persistent grant handling in page")
		}
	}
}

func TestCLIAuthorizationPageUsesEphemeralDeploymentKey(t *testing.T) {
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, httptest.NewRequest("GET", "/cli", nil))
	body := w.Body.String()
	for _, term := range []string{`type="password"`, `autocomplete="off"`, `credentials:'omit'`, `'Authorization':'Bearer '+key`, `currentRevision !== revision`, `adminKey.value = ''`, `issue.disabled = true`} {
		if !strings.Contains(body, term) {
			t.Fatalf("missing direct authorization contract: %s", term)
		}
	}
	for _, term := range []string{"localStorage", "sessionStorage", "X-Alarmd-Principal", "credentials:'same-origin'"} {
		if strings.Contains(body, term) {
			t.Fatalf("unwanted host or persistent credential use: %s", term)
		}
	}
}
