package noderunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testRunner(t *testing.T) *Runner {
	t.Helper()
	r, e := New("execution-a", "filesystem", t.TempDir(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func startRequest(script string) Request {
	return Request{Action: "start", ExecutionID: "execution-a", Mode: "filesystem", Cwd: "/tmp", Argv: []string{"/bin/sh", "-s"}, Stdin: script, TimeoutSeconds: 5, OutputLimitBytes: 4096}
}
func terminal(t *testing.T, r *Runner) Response {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(8 * time.Second):
		t.Fatal("execution did not finish")
	}
	return r.Handle(Request{Action: "get", ExecutionID: r.id})
}
func contents(v Response) string {
	var s strings.Builder
	for _, c := range v.Output {
		s.WriteString(c.Data)
	}
	return s.String()
}
func TestOnlyOneStartAndCursor(t *testing.T) {
	r := testRunner(t)
	file := filepath.Join(t.TempDir(), "count")
	q := startRequest("echo once >> " + file + "; printf abcdef")
	r.Handle(q)
	r.Handle(q)
	v := terminal(t, r)
	if v.State != "succeeded" {
		t.Fatalf("%+v", v)
	}
	data, e := os.ReadFile(file)
	if e != nil || string(data) != "once\n" {
		t.Fatalf("executed more than once: %q %v", data, e)
	}
	p := r.Handle(Request{Action: "get", ExecutionID: r.id, MaxBytes: 3})
	if contents(p) != "abc" {
		t.Fatal(p)
	}
	p = r.Handle(Request{Action: "get", ExecutionID: r.id, OutputCursor: p.NextCursor, MaxBytes: 3})
	if contents(p) != "def" {
		t.Fatal(p)
	}
	if r.Handle(Request{Action: "get", ExecutionID: r.id, OutputCursor: "bad"}).Error == "" {
		t.Fatal("invalid cursor accepted")
	}
}
func TestCancelBeforeStart(t *testing.T) {
	r := testRunner(t)
	r.Handle(Request{Action: "cancel", ExecutionID: r.id})
	v := r.Handle(startRequest("exit 99"))
	if v.State != "cancelled" || !v.ConfirmedTerminated || v.StartedAt != nil {
		t.Fatal(v)
	}
}
func TestCancelAndDeadline(t *testing.T) {
	for _, cancel := range []bool{true, false} {
		t.Run(map[bool]string{true: "cancel", false: "deadline"}[cancel], func(t *testing.T) {
			r := testRunner(t)
			q := startRequest("sleep 30")
			q.TimeoutSeconds = 1
			r.Handle(q)
			want := "timed_out"
			if cancel {
				r.Handle(Request{Action: "cancel", ExecutionID: r.id})
				want = "cancelled"
			}
			v := terminal(t, r)
			if v.State != want {
				t.Fatal(v)
			}
		})
	}
}
func TestRedactionAcrossWritesAndBoundedLines(t *testing.T) {
	r := testRunner(t)
	q := startRequest("printf 'access_to'; sleep 0.1; printf 'ken=topsecret\\n'; printf 'Bearer split'; sleep 0.1; printf 'token\\n'; i=0; while [ $i -lt 70000 ]; do printf x; i=$((i+1)); done; printf '\\n'")
	q.TimeoutSeconds = 5
	r.Handle(q)
	v := terminal(t, r)
	s := contents(v)
	if strings.Contains(s, "topsecret") || strings.Contains(s, "splittoken") || !strings.Contains(s, "REDACTED") || !v.Truncated || v.DroppedBytes < 70000 {
		t.Fatalf("%+v", v)
	}
}
func TestOutputLimitDrains(t *testing.T) {
	r := testRunner(t)
	q := startRequest("i=0; while [ $i -lt 10000 ]; do echo abcdef; i=$((i+1)); done")
	q.OutputLimitBytes = 32
	r.Handle(q)
	v := terminal(t, r)
	if v.State != "succeeded" || v.TotalBytes > 32 || !v.Truncated || v.DroppedBytes == 0 {
		t.Fatal(v)
	}
}
func TestEmptyCursorCanGrow(t *testing.T) {
	r := testRunner(t)
	v := r.Handle(Request{Action: "get", ExecutionID: r.id})
	r.Handle(startRequest("printf hello"))
	terminal(t, r)
	v = r.Handle(Request{Action: "get", ExecutionID: r.id, OutputCursor: v.NextCursor})
	if contents(v) != "hello" {
		t.Fatal(v)
	}
}

func TestPEMRedaction(t *testing.T) {
	r := testRunner(t)
	r.Handle(startRequest("printf '%s\\n' '-----BEGIN PRIVATE KEY-----' sensitivebody '-----END PRIVATE KEY-----' visible"))
	v := terminal(t, r)
	if strings.Contains(contents(v), "sensitivebody") || !strings.Contains(contents(v), "visible") {
		t.Fatal(v)
	}
}

func TestCursorRejectsAnotherExecution(t *testing.T) {
	r := testRunner(t)
	other, e := New("execution-b", "filesystem", t.TempDir(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	cursor := other.Handle(Request{Action: "get", ExecutionID: "execution-b"}).NextCursor
	if r.Handle(Request{Action: "get", ExecutionID: r.id, OutputCursor: cursor}).Error == "" {
		t.Fatal("cross execution cursor accepted")
	}
}
func TestTerminalOutputEvidenceIsStable(t *testing.T) {
	r := testRunner(t)
	r.Handle(startRequest("echo stdout; echo stderr >&2; exit 7"))
	v := terminal(t, r)
	if v.State != "failed" || v.ExitCode == nil || *v.ExitCode != 7 {
		t.Fatal(v)
	}
	first := r.Handle(Request{Action: "get", ExecutionID: r.id})
	second := r.Handle(Request{Action: "get", ExecutionID: r.id})
	if contents(first) != contents(second) || first.NextCursor != second.NextCursor {
		t.Fatal("output replay changed")
	}
}
func TestJSONEscapedStdinLimit(t *testing.T) {
	r := testRunner(t)
	q := startRequest(":" + strings.Repeat("\t", 1<<20))
	v := r.Handle(q)
	if v.State != "failed" || v.StartedAt != nil {
		t.Fatal("oversized stdin accepted")
	}
}

func TestExecutableUsesRequestedPATH(t *testing.T) {
	r := testRunner(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "custom-tool"), []byte("#!/bin/sh\necho chosen\n"), 0700); err != nil {
		t.Fatal(err)
	}
	q := startRequest("")
	q.Argv = []string{"custom-tool"}
	q.Env = map[string]string{"PATH": dir}
	r.Handle(q)
	v := terminal(t, r)
	if v.State != "succeeded" || contents(v) != "chosen\n" {
		t.Fatal(v)
	}
}
