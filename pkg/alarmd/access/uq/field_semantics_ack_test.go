package uq

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

type semanticsAckTransport struct{ response *http.Response }

func (transport semanticsAckTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return transport.response, nil
}

type semanticsAckBody struct {
	io.Reader
	reads  int
	closed bool
}

func (body *semanticsAckBody) Read(p []byte) (int, error) { body.reads++; return body.Reader.Read(p) }
func (body *semanticsAckBody) Close() error               { body.closed = true; return nil }

func TestFTARequiresResponseFieldSemanticsBeforeDecoding(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fta       bool
		headers   []string
		available bool
	}{
		{"ordinary_without_header", false, nil, true},
		{"fta_confirmed", true, []string{"fta_event_tags/v1"}, true},
		{"old_uq_without_header", true, nil, false},
		{"different_semantics", true, []string{"fta_event_tags/v2"}, false},
		{"ambiguous_headers", true, []string{"fta_event_tags/v1", "fta_event_tags/v2"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := validAttempt(t)
			if tc.fta {
				facts := attempt.Spec.PlanFacts
				facts.QueryRevision = ""
				facts.QueryList[0].FieldSemantics = "fta_event_tags/v1"
				facts.TSDBMap = map[string][]execution.QueryStorage{"a": {{TableID: "events", StorageID: "17", StorageType: "elasticsearch", DB: "bkfta_event_*_read", Measurement: "__default__", TimeField: execution.QueryTimeField{Name: "time", Type: "date", Unit: "millisecond"}}}}
				var err error
				facts, err = execution.BuildQueryPlanFacts(facts)
				if err != nil {
					t.Fatal(err)
				}
				attempt.Spec, err = execution.BuildPhysicalQuerySpec(execution.PhysicalQuerySpec{PlanFacts: facts, LogicalWindow: attempt.Spec.LogicalWindow, ProviderRange: attempt.Spec.ProviderRange, AcceptedRange: attempt.Spec.AcceptedRange, RequiredColumns: attempt.Spec.RequiredColumns})
				if err != nil {
					t.Fatal(err)
				}
			}
			payload := `{"series":[{"name":"series0","columns":["_time","_value"],"types":["float","float"],"group_keys":["bk_target_ip"],"group_values":["127.0.0.1"],"values":[[1700123456789,0]]}],"is_partial":false}`
			if !tc.available {
				payload = "must not decode this invalid response"
			}
			body := &semanticsAckBody{Reader: strings.NewReader(payload)}
			headers := make(http.Header)
			for _, value := range tc.headers {
				headers.Add("X-Bk-Query-Field-Semantics", value)
			}
			client, err := NewClient("http://uq", "alarmd", &http.Client{Transport: semanticsAckTransport{&http.Response{StatusCode: 200, Header: headers, Body: body}}})
			if err != nil {
				t.Fatal(err)
			}
			sink := &collectingSink{}
			completion, err := client.Execute(context.Background(), attempt, sink)
			if err != nil {
				t.Fatal(err)
			}
			if !body.closed {
				t.Fatal("response body was not closed")
			}
			if tc.available {
				if completion.Completeness != execution.CompletenessFull || len(sink.batches) != 1 {
					t.Fatalf("completion=%+v batches=%d", completion, len(sink.batches))
				}
				return
			}
			if completion.Completeness != execution.CompletenessUnavailable || completion.DataState != execution.DataStateUnknown || len(sink.batches) != 0 || body.reads != 0 {
				t.Fatalf("completion=%+v batches=%d reads=%d", completion, len(sink.batches), body.reads)
			}
			if completion.RouteFacts.Attempts[0].Detail != "response=field_semantics_unconfirmed" {
				t.Fatalf("route=%+v", completion.RouteFacts)
			}
		})
	}
}
