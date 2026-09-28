// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package uq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const acknowledgedBody = `{"series":[{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],` +
	`"group_keys":["bk_target_ip"],"group_values":["127.0.0.1"],"values":[[1700123456789,12.5]]}],"is_partial":false}`

// ftaAttempt is an FTA event query as the polling compiler builds one: its
// field semantics, its intrinsic source filter and its event storage.
func ftaAttempt(t *testing.T) execution.QueryAttempt {
	t.Helper()
	attempt := validAttempt(t)
	facts := attempt.Spec.PlanFacts
	facts.QueryRevision = ""
	facts.QueryList = append([]execution.QueryClause(nil), facts.QueryList...)
	q := &facts.QueryList[0]
	q.FieldSemantics = "fta_event_tags/v1"
	q.SourceConditions = &execution.QueryConditions{Fields: []execution.QueryConditionField{{Field: "status", Operator: "eq",
		Values: []execution.QueryScalar{{Kind: execution.QueryScalarString, StringValue: "ABNORMAL"}}}}}
	facts.TSDBMap = map[string][]execution.QueryStorage{"a": {{TableID: "events", StorageID: "17", StorageType: "elasticsearch",
		DB: "bkfta_event_*_read", Measurement: "__default__", TimeField: execution.QueryTimeField{Name: "time", Type: "date", Unit: "millisecond"}}}}
	var err error
	if facts, err = execution.BuildQueryPlanFacts(facts); err != nil {
		t.Fatal(err)
	}
	attempt.Spec, err = execution.BuildPhysicalQuerySpec(execution.PhysicalQuerySpec{PlanFacts: facts,
		LogicalWindow: attempt.Spec.LogicalWindow, ProviderRange: attempt.Spec.ProviderRange,
		AcceptedRange: attempt.Spec.AcceptedRange, RequiredColumns: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

// acknowledgingClient answers every query with body, carrying the field
// semantics acknowledgement when one is given.
func acknowledgingClient(t *testing.T, acknowledgement *string) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if acknowledgement != nil {
			writer.Header().Set(headerFieldSemantics, *acknowledgement)
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(acknowledgedBody))
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "alarmd-shadow", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func header(value string) *string { return &value }

// A provider that does not know field_semantics runs the query without it
// and without its source filter -- a wider query, answered as if nothing
// were wrong. Only its acknowledgement tells the two apart, so a query that
// asked for field semantics and is not acknowledged in the version it asked
// for is a gap, and none of the wider answer reaches the sink.
func TestAnFTAQueryTheProviderDidNotAcknowledgeIsAGap(t *testing.T) {
	for _, test := range []struct {
		name            string
		acknowledgement *string
	}{
		{"no acknowledgement", nil},
		{"an empty acknowledgement", header("")},
		{"another version acknowledged", header("fta_event_tags/v2")},
	} {
		t.Run(test.name, func(t *testing.T) {
			sink := &collectingSink{}
			completion, err := acknowledgingClient(t, test.acknowledgement).Execute(context.Background(), ftaAttempt(t), sink)
			if err != nil || completion.Completeness != execution.CompletenessUnavailable || len(completion.RouteFacts.Attempts) != 1 {
				t.Fatalf("completion=%+v error=%v, want UNAVAILABLE", completion, err)
			}
			attempt := completion.RouteFacts.Attempts[0]
			if attempt.Detail != "response=field_semantics_unacknowledged" ||
				attempt.ReasonCode != execution.ReasonCode(contract.ReasonQueryUnavailable) {
				t.Fatalf("attempt=%+v, want QUERY_UNAVAILABLE response=field_semantics_unacknowledged", attempt)
			}
			if len(sink.batches) != 0 {
				t.Fatalf("%d series of the unacknowledged answer reached the sink", len(sink.batches))
			}
		})
	}
	sink := &collectingSink{}
	completion, err := acknowledgingClient(t, header("fta_event_tags/v1")).Execute(context.Background(), ftaAttempt(t), sink)
	if err != nil || completion.Completeness != execution.CompletenessFull || len(sink.batches) != 1 {
		t.Fatalf("acknowledged query: completion=%+v batches=%d error=%v, want FULL with its series", completion, len(sink.batches), err)
	}
}

// A query that asked for no field semantics does not read the header: it
// completes the same with no acknowledgement and with a stray one.
func TestAQueryWithoutFieldSemanticsDoesNotReadTheAcknowledgement(t *testing.T) {
	complete := func(acknowledgement *string) (execution.ProviderCompletion, int) {
		sink := &collectingSink{}
		completion, err := acknowledgingClient(t, acknowledgement).Execute(context.Background(), validAttempt(t), sink)
		if err != nil || completion.Completeness != execution.CompletenessFull {
			t.Fatalf("completion=%+v error=%v, want FULL", completion, err)
		}
		completion.Stats.QueryMillis, completion.Stats.DecodeMillis = 0, 0
		completion.RouteFacts.Attempts[0].Endpoint = ""
		return completion, len(sink.batches)
	}
	plain, plainBatches := complete(nil)
	stray, strayBatches := complete(header("something else"))
	if !reflect.DeepEqual(plain, stray) || plainBatches != 1 || strayBatches != 1 {
		t.Fatalf("a stray acknowledgement changed a plain query:\n%+v\n%+v", plain, stray)
	}
}

// One header names one version: clauses asking for different versions can
// never be acknowledged.
func TestClausesAskingForDifferentVersionsCannotBeAcknowledged(t *testing.T) {
	spec := func(versions ...string) execution.PhysicalQuerySpec {
		clauses := make([]execution.QueryClause, len(versions))
		for index, version := range versions {
			clauses[index].FieldSemantics = version
		}
		return execution.PhysicalQuerySpec{PlanFacts: execution.QueryPlanFacts{QueryList: clauses}}
	}
	for _, test := range []struct {
		versions []string
		want     string
	}{
		{[]string{"", ""}, ""},
		{[]string{"", "fta_event_tags/v1"}, "fta_event_tags/v1"},
		{[]string{"fta_event_tags/v1", "fta_event_tags/v1"}, "fta_event_tags/v1"},
	} {
		if got := requestedFieldSemantics(spec(test.versions...)); got != test.want {
			t.Errorf("requestedFieldSemantics(%q) = %q, want %q", test.versions, got, test.want)
		}
	}
	if mixed := requestedFieldSemantics(spec("fta_event_tags/v1", "fta_event_tags/v2")); mixed == "fta_event_tags/v1" || mixed == "fta_event_tags/v2" || mixed == "" {
		t.Fatalf("mixed versions asked for %q, which one header could acknowledge", mixed)
	}
}
