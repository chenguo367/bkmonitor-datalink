// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package uq

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// heldServer answers its first request only once release is called (at the
// latest when the test ends); every later request is answered at once.
func heldServer(t *testing.T) (*httptest.Server, chan struct{}, func()) {
	t.Helper()
	started, held := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(held) }) }
	var first atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(false, true) {
			close(started)
			<-held
		}
		_, _ = io.WriteString(w, `{"series":[]}`)
	}))
	t.Cleanup(s.Close)
	t.Cleanup(release)
	return s, started, release
}

// A query finding the egress taken waits for it, and runs once the query in
// flight ends inside the wait. Before, it was refused at once.
func TestAQueryFindingTheEgressTakenWaitsAndRunsWhenItFrees(t *testing.T) {
	s, started, release := heldServer(t)
	p, _ := NewWithQuerySource(fixture(s.URL), nil, "alarmd-sre")
	defer p.Close()
	p.wait = 5 * time.Second
	first := make(chan error, 1)
	go func() { _, err := p.Query(context.Background(), request()); first <- err }()
	<-started
	second := make(chan error, 1)
	go func() { _, err := p.Query(context.Background(), request()); second <- err }()
	select {
	case err := <-second:
		t.Fatalf("the second query returned while the first held the egress: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatalf("the waiting query = %v, want it run once the egress freed", err)
	}
}

// Still taken past the wait, the query is refused, and the refusal says how
// long it waited and how long the query in flight has held the egress.
func TestAQueryStillWaitingPastTheWaitIsRefusedSayingTheWaitAndTheHold(t *testing.T) {
	s, started, _ := heldServer(t)
	p, _ := NewWithQuerySource(fixture(s.URL), nil, "alarmd-sre")
	defer p.Close()
	p.wait = 300 * time.Millisecond
	go func() { _, _ = p.Query(context.Background(), request()) }()
	<-started
	time.Sleep(200 * time.Millisecond)
	begin := time.Now()
	_, err := p.Query(context.Background(), request())
	waited := time.Since(begin)
	if code(err) != "uq_busy" || waited < p.wait {
		t.Fatalf("refusal %v after %s, want uq_busy after the %s wait", err, waited, p.wait)
	}
	message := err.Error()
	if !regexp.MustCompile(`waited 0\.[3-9]s`).MatchString(message) || !regexp.MustCompile(`held the egress for 0\.[5-9]s`).MatchString(message) {
		t.Fatalf("refusal %q, want the wait (0.3s) and the hold (0.5s)", message)
	}
}

// A waiting query whose caller goes away stops waiting at once.
func TestAWaitingQueryStopsWhenItsCallerGoesAway(t *testing.T) {
	s, started, _ := heldServer(t)
	p, _ := NewWithQuerySource(fixture(s.URL), nil, "alarmd-sre")
	defer p.Close()
	p.wait = 5 * time.Second
	go func() { _, _ = p.Query(context.Background(), request()) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	begin := time.Now()
	_, err := p.Query(ctx, request())
	if code(err) != "uq_canceled" || time.Since(begin) > time.Second {
		t.Fatalf("canceled wait = %v after %s, want uq_canceled at once", err, time.Since(begin))
	}
}

// A waiting query whose deadline ends first is refused as busy: the egress
// being taken is why it never ran.
func TestAWaitEndedByTheDeadlineIsBusy(t *testing.T) {
	s, started, _ := heldServer(t)
	p, _ := NewWithQuerySource(fixture(s.URL), nil, "alarmd-sre")
	defer p.Close()
	p.wait = 5 * time.Second
	go func() { _, _ = p.Query(context.Background(), request()) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := p.Query(ctx, request())
	if code(err) != "uq_busy" || !regexp.MustCompile(`waited 0\.[1-9]s`).MatchString(err.Error()) {
		t.Fatalf("deadline during the wait = %v, want uq_busy saying it waited 0.1s", err)
	}
}

// Two failure reads at once both complete: each page frees the egress, the
// other's waiting page runs next, and neither is refused.
func TestTwoFailureReadsAtOnceBothComplete(t *testing.T) {
	base := failureNow.Add(-time.Hour).UnixMilli()
	server := &failureServer{rows: func(from int) ([]map[string]any, int) {
		time.Sleep(40 * time.Millisecond)
		page := make([]map[string]any, 0, MaxTraceRows)
		for i := 0; i < MaxTraceRows; i++ {
			page = append(page, span(fmt.Sprintf("t%d", from+i), "web", "/busy", 500, nil, base+int64(from+i), 1, nil))
		}
		return page, 3 * MaxTraceRows
	}}
	p := failureProvider(t, server, false)
	p.wait = 2 * time.Second
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answer, err := p.TopFailures(context.Background(), TopFailuresRequest{Egress: "primary", Target: "fixture", Now: failureNow})
			if err == nil && answer.Scanned != 3*MaxTraceRows {
				err = fmt.Errorf("scanned %d, want %d", answer.Scanned, 3*MaxTraceRows)
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("read %d = %v, want both reads complete", i, err)
		}
	}
}

// Each receipt keeps its own response: a later query does not write into
// the bytes an earlier receipt holds.
func TestAReceiptKeepsItsOwnResponseAcrossLaterQueries(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"series":[],"page":"page-%d"}`, calls.Add(1))
	}))
	defer s.Close()
	p, _ := NewWithQuerySource(fixture(s.URL), nil, "alarmd-sre")
	defer p.Close()
	first, err := p.Query(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	kept := string(first.Native)
	if _, err := p.Query(context.Background(), request()); err != nil {
		t.Fatal(err)
	}
	if string(first.Native) != kept || !strings.Contains(kept, "page-1") {
		t.Fatalf("first receipt %s after a second query, want it unchanged (%s)", first.Native, kept)
	}
}
