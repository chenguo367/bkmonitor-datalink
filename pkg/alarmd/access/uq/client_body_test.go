package uq

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const bodyTestSeries = `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],"group_values":["127.0.0.1"],"values":[[1700123456789,12.5]]}`

// partialBodyServer answers 200 with the start of a body and one whole
// series, then either stalls until the request goes away or, cut, drops the
// connection under a body it declared longer.
func partialBodyServer(t *testing.T, cut bool) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		head := `{"series":[` + bodyTestSeries + `,`
		if cut {
			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				t.Error("setup: the test server cannot hand over its connection")
				return
			}
			connection, buffered, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer connection.Close()
			_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100000\r\n\r\n" + head)
			_ = buffered.Flush()
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(head))
		writer.(http.Flusher).Flush()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-request.Context().Done():
		case <-timer.C:
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// slowSink takes its time over every batch, as a delivery that does not keep
// up with the answer would.
type slowSink struct {
	collectingSink
	delay time.Duration
}

func (sink *slowSink) ConsumeProviderSeries(ctx context.Context, batch execution.ProviderSeriesBatch) error {
	time.Sleep(sink.delay)
	return sink.collectingSink.ConsumeProviderSeries(ctx, batch)
}

// The worker finds a failure's name, detail and timing by these methods; a
// body failure that stopped answering one would reach it unnamed again.
var _ interface {
	QueryFailure() (string, string)
	QueryFailureDetail() string
	QueryFailureTiming() *execution.AttemptTiming
} = (*bodyFailureError)(nil)

func bodyFailureOf(t *testing.T, err error) *bodyFailureError {
	t.Helper()
	var failure *bodyFailureError
	if !errors.As(err, &failure) {
		t.Fatalf("error = %v, want a named body failure", err)
	}
	return failure
}

// A query whose answer began and whose body did not arrive in full names
// itself as the same timeout or unavailability it would have been before
// the answer began, with a body detail and its timing, rather than reading
// as an unclassified internal error. The timing's local part is what tells
// the backend apart from alarmd's own delivery: a body that stopped coming
// spends almost none of the time locally, a sink that did not keep up spends
// most of it. Series handed on before the failure stay delivered - it is
// still an error, not a completion.
func TestAQueryWhoseBodyStopsPartwayNamesItself(t *testing.T) {
	whole := func(timing *execution.AttemptTiming, attempt execution.QueryAttempt) bool {
		return timing.SettleMillis+timing.StartLateMillis+timing.BudgetMillis == attempt.DeadlineUnixMilli-attempt.BudgetStartUnixMilli
	}
	slot := func() execution.QueryAttempt {
		attempt := validAttempt(t)
		now := time.Now()
		attempt.BudgetStartUnixMilli = now.Add(-time.Second).UnixMilli()
		attempt.ReadyAtUnixMilli = attempt.BudgetStartUnixMilli
		attempt.DeadlineUnixMilli = now.Add(300 * time.Millisecond).UnixMilli()
		return attempt
	}

	stalled := partialBodyServer(t, false)
	client, err := NewClient(stalled.URL, "alarmd-shadow", stalled.Client())
	if err != nil {
		t.Fatal(err)
	}
	attempt := slot()
	sink := &collectingSink{}
	_, err = client.Execute(context.Background(), attempt, sink)
	failure := bodyFailureOf(t, err)
	category, code := failure.QueryFailure()
	timing := failure.QueryFailureTiming()
	if category != "provider_transport" || code != "QUERY_TIMEOUT" || failure.QueryFailureDetail() != "body=timeout" || timing == nil {
		t.Fatalf("a body that stopped coming = %s/%s %q timing %+v, want provider_transport/QUERY_TIMEOUT body=timeout, timed",
			category, code, failure.QueryFailureDetail(), timing)
	}
	if !whole(timing, attempt) || timing.ElapsedMillis < timing.BudgetMillis-50 || timing.LocalMillis < 0 || timing.LocalMillis > 100 {
		t.Fatalf("timing = %+v, want the budget used up waiting on the backend, little of it local", *timing)
	}
	if len(sink.batches) != 1 {
		t.Fatalf("delivered %d batches, want the one series that arrived", len(sink.batches))
	}

	slow := fixtureClient(t, http.StatusOK, `{"series":[`+bodyTestSeries+`],"is_partial":false}`, DefaultLimits())
	attempt = slot()
	_, err = slow.Execute(context.Background(), attempt, &slowSink{delay: 500 * time.Millisecond})
	failure = bodyFailureOf(t, err)
	timing = failure.QueryFailureTiming()
	if _, code := failure.QueryFailure(); code != "QUERY_TIMEOUT" || failure.QueryFailureDetail() != "body=timeout" || timing == nil ||
		!whole(timing, attempt) || timing.LocalMillis < 400 || timing.LocalMillis > timing.ElapsedMillis {
		t.Fatalf("a sink that did not keep up = %s %q timing %+v, want a body timeout spent mostly local", code, failure.QueryFailureDetail(), timing)
	}

	cut := partialBodyServer(t, true)
	client, err = NewClient(cut.URL, "alarmd-shadow", cut.Client())
	if err != nil {
		t.Fatal(err)
	}
	attempt = slot()
	attempt.DeadlineUnixMilli = time.Now().Add(time.Minute).UnixMilli()
	_, err = client.Execute(context.Background(), attempt, &collectingSink{})
	failure = bodyFailureOf(t, err)
	if category, code := failure.QueryFailure(); category != "provider_transport" || code != "QUERY_UNAVAILABLE" ||
		failure.QueryFailureDetail() != "body=eof" || failure.QueryFailureTiming() == nil || !whole(failure.QueryFailureTiming(), attempt) {
		t.Fatalf("a connection dropped under its body = %s/%s %q timing %+v, want provider_transport/QUERY_UNAVAILABLE body=eof, timed",
			category, code, failure.QueryFailureDetail(), failure.QueryFailureTiming())
	}
	if !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("the named failure lost its own text: %v", err)
	}
}

// Only a transport failure of the body is named. A body that breaks the
// wire contract is a different failure - one cut short within the length it
// declared, or empty, ends where the server ended it - a response budget
// keeps its own name, and a caller that gave up keeps its own error, as each
// does before the answer begins.
func TestOnlyABodysTransportFailureIsNamed(t *testing.T) {
	var failure *bodyFailureError
	for name, body := range map[string]string{"malformed": `{"series":[{"name":`, "empty": ""} {
		client := fixtureClient(t, http.StatusOK, body, DefaultLimits())
		if _, err := client.Execute(context.Background(), validAttempt(t), &collectingSink{}); err == nil || errors.As(err, &failure) {
			t.Fatalf("a %s body = %v, want its own error unnamed", name, err)
		}
	}

	limits := DefaultLimits()
	limits.MaxBodyBytes, limits.MaxSeriesBytes = 64, 64
	oversized := fixtureClient(t, http.StatusOK, `{"series":[`+bodyTestSeries+`]}`, limits)
	if _, err := oversized.Execute(context.Background(), validAttempt(t), &collectingSink{}); !errors.Is(err, ErrResponseBytesExceeded) || errors.As(err, &failure) {
		t.Fatalf("an oversized body = %v, want the response budget's own name", err)
	}

	stalled := partialBodyServer(t, false)
	client, err := NewClient(stalled.URL, "alarmd-shadow", stalled.Client())
	if err != nil {
		t.Fatal(err)
	}
	attempt := validAttempt(t)
	attempt.DeadlineUnixMilli = time.Now().Add(time.Minute).UnixMilli()
	caller, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := client.Execute(caller, attempt, &collectingSink{}); err == nil || errors.As(err, &failure) {
		t.Fatalf("a caller that gave up = %v, want its own error unnamed", err)
	}
}
