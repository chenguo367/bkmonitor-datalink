package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func pollingTestSource(raw string) PrimaryQuerySource {
	return PrimaryQuerySource{Identity: SourceIdentity{TenantID: "tenant", BusinessID: "2", SpaceScope: "bkcc__2"},
		StrategyID: "14", ItemID: "3", QueryMD5: "query", Expression: "a", QueryConfigs: []json.RawMessage{json.RawMessage(raw)}}
}

func TestPollingSourceQuerySemantics(t *testing.T) {
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	for _, tc := range []struct {
		name, config, table, field, dataSource, dimension, aggregation string
		delay                                                          int64
	}{
		{"custom", `{"data_source_label":"custom","data_type_label":"time_series","result_table_id":"Custom.Metric","metric_field":"value","alias":"a","agg_method":"AVG","agg_interval":60,"agg_dimension":["host"]}`, "custom.metric", "value", "", "host", "avg_over_time", 0},
		{"log-count", `{"data_source_label":"bk_log_search","data_type_label":"log","index_set_id":7,"agg_method":"COUNT","alias":"a","agg_interval":45,"agg_dimension":["host"]}`, "bklog_index_set_7", "_index", "bklog", "host", "count_over_time", 90},
		{"monitor-event-count", `{"data_source_label":"bk_monitor","data_type_label":"log","result_table_id":"system.event","agg_method":"COUNT","agg_interval":60,"agg_dimension":["host"]}`, "system.event.__default__", "event.count", "bkapm", "dimensions.host", "sum_over_time", 0},
		{"custom-event", `{"data_source_label":"custom","data_type_label":"event","result_table_id":"k8s_event","custom_event_name":"PodError","agg_interval":60,"agg_dimension":["namespace"]}`, "k8s_event", "_index", "bkapm", "dimensions.namespace", "count_over_time", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts, err := planner.CompilePrimaryQuery(context.Background(), pollingTestSource(tc.config))
			if err != nil {
				t.Fatal(err)
			}
			q := facts.QueryList[0]
			if q.TableID != tc.table || q.FieldName != tc.field || q.DataSource != tc.dataSource || q.Dimensions[0] != tc.dimension || q.TimeAggregation.Method != tc.aggregation || facts.QueryDelaySeconds != tc.delay {
				t.Fatalf("query=%+v delay=%d", q, facts.QueryDelaySeconds)
			}
			if tc.name == "custom-event" && (len(q.Conditions.Fields) != 2 || q.Conditions.Fields[1].Field != "dimensions.event_type" || q.Conditions.Fields[1].Operator != "ne") {
				t.Fatalf("custom constructor filters=%+v", q.Conditions)
			}
			if tc.name == "monitor-event-count" && facts.Normalization.DatasetContract.IdentityFields[0] != "host" {
				t.Fatalf("canonical identity=%+v", facts.Normalization)
			}
		})
	}
}

func TestPollingPromQLDynamicIdentityAndImmutableSourceFacts(t *testing.T) {
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	source := pollingTestSource(`{"data_source_label":"prometheus","data_type_label":"time_series","promql":"sum by (pod) (up)","agg_interval":60,"filter_dict":{"labels":{"job":"api"},"ignored":1}}`)
	facts, err := planner.CompilePrimaryQuery(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if facts.PromQL == nil || facts.PromQL.Match != "{job='api'}" || !facts.Normalization.DatasetContract.DynamicDimensions || len(facts.QueryList) > 0 || facts.MetricMerge != "" {
		t.Fatalf("facts=%+v", facts)
	}
	source.TimeDelaySeconds = 61
	delayed, err := planner.CompilePrimaryQuery(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if delayed.QueryDelaySeconds != 120 || delayed.QueryRevision == facts.QueryRevision {
		t.Fatalf("delayed=%+v", delayed)
	}
	facts.QueryRevision = ""
	facts.MetricMerge = "a"
	if _, err := execution.BuildQueryPlanFacts(facts); err == nil {
		t.Fatal("mixed query endpoints accepted")
	}
}

// FTA event sources are refused by name, whatever their configuration: the
// FTA query is not supported, and nothing in the compiler's facts turns it on.
func TestAnFTASourceIsRefusedAsUnsupported(t *testing.T) {
	planner, err := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range []string{
		`{"data_source_label":"bk_fta","data_type_label":"event","alert_name":"__EVENT_PLUGIN__zabbix","alias":"a","agg_interval":60,"agg_dimension":["tags.env"],"agg_condition":[{"key":"tags.env","method":"include","value":["prod"]}]}`,
		`{"data_source_label":"bk_fta","data_type_label":"event","alert_name":"CPUHigh","agg_interval":120}`,
		`{"data_source_label":"bk_fta","data_type_label":"event","agg_interval":30}`,
	} {
		_, err := planner.CompilePrimaryQuery(context.Background(), pollingTestSource(config))
		var failure *QueryPlanCompileError
		if !errors.As(err, &failure) || failure.Disposition != DispositionUnsupported || failure.Reason != "QUERY_FTA_UNSUPPORTED" {
			t.Fatalf("%s: %v, want UNSUPPORTED_PHASE2_CAPABILITY QUERY_FTA_UNSUPPORTED", config, err)
		}
	}
}

func TestPollingBKDataLocalTimeBoundary(t *testing.T) {
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	source := pollingTestSource(`{"data_source_label":"bk_data","data_type_label":"time_series","result_table_id":"2_metric","metric_field":"value","alias":"a","agg_method":"AVG","agg_interval":60}`)
	for _, functions := range []string{`[]`, `[{"id":"abs","params":[]}]`} {
		var query map[string]json.RawMessage
		if err := json.Unmarshal(source.QueryConfigs[0], &query); err != nil {
			t.Fatal(err)
		}
		query["functions"] = json.RawMessage(functions)
		source.QueryConfigs[0], _ = json.Marshal(query)
		_, err := planner.CompilePrimaryQuery(context.Background(), source)
		var failure *QueryPlanCompileError
		if !errors.As(err, &failure) || failure.Disposition != DispositionUnsupported || failure.Reason != "QUERY_BK_DATA_LOCAL_TIME_NOT_MIGRATED" {
			t.Fatalf("functions=%s localTime boundary=%v", functions, err)
		}
	}
	source.Expression = "a + 1"
	facts, err := planner.CompilePrimaryQuery(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if facts.QueryList[0].TimeField != "dtEventTimeStamp" {
		t.Fatalf("query=%+v", facts.QueryList)
	}
}

func TestLogSearchQueryString(t *testing.T) {
	for raw, want := range map[string]string{"timeout": "*timeout*", "   ": "*", "a &amp;&amp; b": "a && b", "field:value": "field:value", "foo AND bar": "foo AND bar"} {
		if got := logSearchQueryString(raw); got != want {
			t.Errorf("%q => %q want %q", raw, got, want)
		}
	}
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	source := pollingTestSource(`{"data_source_label":"bk_log_search","data_type_label":"time_series","index_set_id":7,"result_table_id":"unused","agg_interval":60,"agg_condition":[{"key":"__dist_01","method":"eq","value":["a"]}]}`)
	facts, err := planner.CompilePrimaryQuery(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if facts.QueryList[0].TableID != "bklog_index_set_7_clustered" {
		t.Fatalf("query=%+v", facts.QueryList[0])
	}
}

// The event name and the recovery filter go into every OR group of a custom
// event strategy's conditions, as the platform's _filter_dict_to_conditions
// puts its filters, not once after the last group: appended at the end they
// filtered only that group, and every other group counted every event of the
// table, recoveries included.
func TestCustomEventFiltersAreAndedIntoEveryOrGroup(t *testing.T) {
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	facts, err := planner.CompilePrimaryQuery(context.Background(), pollingTestSource(`{"data_source_label":"custom","data_type_label":"event",
		"result_table_id":"base_tenant_2_event","custom_event_name":"AgentLost","agg_method":"COUNT","agg_interval":60,
		"agg_dimension":["bk_target_ip","bk_target_cloud_id","bk_agent_id"],
		"agg_condition":[{"key":"bk_target_ip","method":"eq","value":["192.0.2.10"]},
			{"condition":"or","key":"bk_target_ip","method":"eq","value":["192.0.2.11"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	conditions := facts.QueryList[0].Conditions
	want := []string{"dimensions.bk_target_ip", "event_name", "dimensions.event_type", "dimensions.bk_target_ip", "event_name", "dimensions.event_type"}
	wantConnectors := []string{"and", "and", "or", "and", "and"}
	if len(conditions.Fields) != len(want) || len(conditions.Connectors) != len(wantConnectors) {
		t.Fatalf("conditions %+v, want both OR groups filtered", conditions)
	}
	for index, field := range want {
		if conditions.Fields[index].Field != field {
			t.Fatalf("condition %d is %q, want %q: %+v", index, conditions.Fields[index].Field, field, conditions)
		}
	}
	for index, connector := range wantConnectors {
		if conditions.Connectors[index] != connector {
			t.Fatalf("connector %d is %q, want %q: %+v", index, conditions.Connectors[index], connector, conditions)
		}
	}
}

// A dimension or condition key that already names a dimensions field keeps
// its name, as the platform adds the prefix only to one that does not start
// with it (is_dimensions_field).
func TestAnEventFieldAlreadyUnderDimensionsIsNotPrefixedTwice(t *testing.T) {
	planner, _ := NewLegacyPrimaryQueryCompiler("uq", "UTC", LegacyQueryRuntimeFacts{})
	facts, err := planner.CompilePrimaryQuery(context.Background(), pollingTestSource(`{"data_source_label":"custom","data_type_label":"event",
		"result_table_id":"k8s_event","custom_event_name":"PodError","agg_interval":60,"agg_dimension":["dimensions.namespace"],
		"agg_condition":[{"key":"dimensions.pod","method":"eq","value":["api-0"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	clause := facts.QueryList[0]
	if clause.Dimensions[0] != "dimensions.namespace" {
		t.Fatalf("dimension %q, want dimensions.namespace", clause.Dimensions[0])
	}
	for _, field := range clause.Conditions.Fields {
		if field.Field == "dimensions.dimensions.pod" {
			t.Fatalf("condition key prefixed twice: %+v", clause.Conditions)
		}
	}
}
