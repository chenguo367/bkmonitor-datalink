package obevidence

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProjectionOnlyExposesKnownFieldsAndRedactsNamedCredentials(t *testing.T) {
	raw := []byte(`{"id":7,"items":[{"query_configs":[{"functions":[{"id":"test","params":[{"id":"Authorization","value":"PARAM_SECRET"}]}],"agg_condition":[{"key":"password","method":"eq","value":["CONDITION_SECRET"]},{"key":"bk_host_id","method":"eq","value":["42"]}]}]}],"new_config":{"public_name":"EXTENSION_SECRET"}}`)
	value, omitted, err := projectJSON(raw, sourcePolicy)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(value)
	if strings.Contains(string(data), "SECRET") || !strings.Contains(string(data), `"42"`) {
		t.Fatalf("projection: %s", data)
	}
	if len(omitted) != 3 {
		t.Fatalf("omissions: %+v", omitted)
	}
}

func TestProjectionRejectsNonObjectsAndTrailingDocuments(t *testing.T) {
	for _, raw := range []string{`[]`, `null`, `"text"`, `{} {}`, `{`} {
		if _, _, err := projectJSON([]byte(raw), sourcePolicy); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestSourceTargetValuesPreserveOrdinaryMembersButOmitCredentials(t *testing.T) {
	value, omitted, err := projectJSON([]byte(`{"items":[{"target":[[{"key":"Authorization","method":"eq","value":["TARGET_SECRET"]},{"key":"bk_host_id","method":"eq","value":[42]}]]}]}`), sourcePolicy)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(value)
	if strings.Contains(string(data), "TARGET_SECRET") || !strings.Contains(string(data), "42") || len(omitted) != 1 || omitted[0].Reason != "credential_parameter" {
		t.Fatalf("unsafe or lossy target projection: %s %+v", data, omitted)
	}
}

// The writer's effective-time snapshot is readable in the source view -- its
// status and reason decide EFFECTIVE_TIME_SNAPSHOT_* -- and nothing outside
// the compiler's own fields passes.
func TestTheEffectiveTimeSnapshotShowsWhatTheCompilerReads(t *testing.T) {
	raw := []byte(`{"id":379,"effective_time_snapshot":{"schema_version":1,"status":"UNAVAILABLE","reason":"calendar service timeout","business_timezone":"Asia/Shanghai",` +
		`"calendars":[{"id":3,"bk_tenant_id":"system","status":"ok","token":"CAL_SECRET","items":[{"id":9,"time_kind":"repeat","start_time":1,"end_time":2,"time_zone":"Asia/Shanghai",` +
		`"repeat":{"freq":"week","interval":1,"every":[1,2],"exclude_date":[3]}}]}],"extra":"EXTENSION_SECRET"}}`)
	value, omitted, err := projectJSON(raw, sourcePolicy)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(value)
	for _, want := range []string{`"status":"UNAVAILABLE"`, `"reason":"calendar service timeout"`, `"schema_version":1`, `"freq":"week"`, `"every":[1,2]`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("snapshot projection lacks %s: %s", want, data)
		}
	}
	if strings.Contains(string(data), "SECRET") || len(omitted) != 2 {
		t.Fatalf("snapshot projection passed an unknown field: %s %+v", data, omitted)
	}
}

// A Kubernetes static target's match, a model_inst_id rule's model mapping and
// a dynamic group's id read in the source view; a field outside them does not.
func TestTheTargetPlanShowsStaticMatchesAndTheModelMapping(t *testing.T) {
	raw := []byte(`{"items":[{"target_plan":{"schema_version":1,"model_id":"k8s","target_rule":"match","model_match":{"cw_object_model_id":"23","token":"MAP_SECRET"},` +
		`"static_targets":[{"model_id":"k8s","match":{"bcs_cluster_id":"BCS-K8S-00001","namespace":"prod","workload_kind":"Deployment","workload_name":"api","password":"MATCH_SECRET"}}],` +
		`"dynamic_groups":[],"dynamic_topologies":[]}},` +
		`{"target_plan":{"schema_version":1,"model_id":"host","target_rule":"host_id","dynamic_groups":[{"dynamic_group_id":"g1","token":"GROUP_SECRET"}]}}]}`)
	value, omitted, err := projectJSON(raw, sourcePolicy)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(value)
	for _, want := range []string{`"bcs_cluster_id":"BCS-K8S-00001"`, `"workload_name":"api"`, `"cw_object_model_id":"23"`, `"dynamic_groups":[]`, `"dynamic_group_id":"g1"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("target plan projection lacks %s: %s", want, data)
		}
	}
	if strings.Contains(string(data), "SECRET") || len(omitted) != 3 {
		t.Fatalf("target plan projection passed an unknown field: %s %+v", data, omitted)
	}
}

// The published plan freezes each dynamic group as its bare id; the object the
// writer's source spells does not replace it there.
func TestTheFrozenTargetPlanKeepsItsDynamicGroupIDs(t *testing.T) {
	raw := []byte(`{"plans":[{"target_plan":{"schema_version":1,"model_id":"host","target_rule":"host_id","dynamic_groups":["g1","g2"]},` +
		`"target_scope":{"schema_version":1,"model_id":"host","dynamic_groups":["g3"]}}]}`)
	value, omitted, err := projectJSON(raw, publishedPolicy)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(value)
	for _, want := range []string{`"dynamic_groups":["g1","g2"]`, `"dynamic_groups":["g3"]`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("frozen target plan lacks %s: %s %+v", want, data, omitted)
		}
	}
	if len(omitted) != 0 {
		t.Fatalf("frozen dynamic group ids were omitted: %+v", omitted)
	}
}
