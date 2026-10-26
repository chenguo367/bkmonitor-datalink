package contract

import (
	"encoding/json"
	"testing"
)

// The expected digests were computed by bk-monitor's own functions,
// get_agg_condition_md5 (alarm_backends/service/alert/manager/checker/
// close.py) over count_md5 (bkmonitor/utils/common_utils.py), copied
// verbatim into a scratch script and run with python3 on these lists.
func TestAggConditionMD5MatchesThePlatform(t *testing.T) {
	for _, vector := range []struct{ name, conditions, want string }{
		{"empty", `[]`, "d751713988987e9331980363e24189ce"},
		{"one", `[{"key":"ip","value":["192.0.2.1"],"method":"eq"}]`, "92811130e44a4e5eb434d4dd5acbfdaa"},
		{"one with a first connector", `[{"key":"ip","value":["192.0.2.1"],"method":"eq","condition":"and"}]`, "92811130e44a4e5eb434d4dd5acbfdaa"},
		{"two", `[{"key":"ip","value":["192.0.2.1","192.0.2.2"],"method":"eq"},{"condition":"or","key":"device","value":["eth0"],"method":"neq"}]`,
			"535ab9226fef3171fd744a3d4fc3f603"},
		{"two, first connector changed", `[{"condition":"or","key":"ip","value":["192.0.2.1","192.0.2.2"],"method":"eq"},{"condition":"or","key":"device","value":["eth0"],"method":"neq"}]`,
			"535ab9226fef3171fd744a3d4fc3f603"},
		{"two, second connector changed", `[{"key":"ip","value":["192.0.2.1","192.0.2.2"],"method":"eq"},{"condition":"and","key":"device","value":["eth0"],"method":"neq"}]`,
			"c49ebad2180794dd876ec4565ac72633"},
		{"numbers", `[{"key":"code","value":[200,1.5,100000.0,1e16,0.00001,-3],"method":"eq"}]`, "ae4d3cf2814e8bb61490da37639e52a9"},
		{"other keys ignored", `[{"key":"ip","value":["192.0.2.1"],"method":"eq","dimension_name":"IP"}]`, "92811130e44a4e5eb434d4dd5acbfdaa"},
		{"unicode", `[{"key":"name","value":["中文"],"method":"include"}]`, "55a995a9443091788e434e6de47b7b7b"},
		{"scalar value", `[{"key":"name","value":"x","method":"reg"}]`, "3876dd551988991bbdb280ac2fd7018d"},
		{"bool and null", `[{"key":"flag","value":[true,null,false],"method":"eq"}]`, "fbae7f87302a62dcd8dd4b97160a5de3"},
		{"value order", `[{"key":"ip","value":["192.0.2.2","192.0.2.1"],"method":"eq"}]`, "26706758f34f916c0dbde3d2121dfd33"},
	} {
		got, err := AggConditionMD5(json.RawMessage(vector.conditions))
		if err != nil || got != vector.want {
			t.Errorf("%s: AggConditionMD5 = %q, %v; the platform's is %q", vector.name, got, err, vector.want)
		}
	}
	for _, refused := range []string{`null`, `{}`, `[1]`, `not json`} {
		if _, err := AggConditionMD5(json.RawMessage(refused)); err == nil {
			t.Errorf("agg_condition %s was digested", refused)
		}
	}
}

// The facts of one query config are its metric digest and the platform's
// condition md5; a missing agg_condition reads as an empty one, and a
// query config without a metric_id has no facts.
func TestDeriveStrategyFactsV1(t *testing.T) {
	facts, err := DeriveStrategyFactsV1([]json.RawMessage{json.RawMessage(
		`{"metric_id":"bk_monitor.system.cpu.usage","agg_condition":[{"key":"ip","value":["192.0.2.1"],"method":"eq","condition":"and"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	// md5 of ["bk_monitor.system.cpu.usage"].
	if *facts != (StrategyFactsV1{Schema: "v1", MetricDigest: "3815df5a36678b1d738aa6f9a1522549", ConditionMD5: "92811130e44a4e5eb434d4dd5acbfdaa"}) {
		t.Fatalf("facts %+v", *facts)
	}
	bare, err := DeriveStrategyFactsV1([]json.RawMessage{json.RawMessage(`{"metric_id":"bk_monitor.system.cpu.usage"}`)})
	if err != nil || bare.ConditionMD5 != "d751713988987e9331980363e24189ce" || bare.MetricDigest != facts.MetricDigest {
		t.Fatalf("facts %+v err %v, want an empty condition", bare, err)
	}
	// Two queries: the metric ids in order, the conditions joined in order.
	two, err := DeriveStrategyFactsV1([]json.RawMessage{json.RawMessage(`{"metric_id":"a","agg_condition":[]}`),
		json.RawMessage(`{"metric_id":"b","agg_condition":[]}`)})
	swapped, _ := DeriveStrategyFactsV1([]json.RawMessage{json.RawMessage(`{"metric_id":"b","agg_condition":[]}`),
		json.RawMessage(`{"metric_id":"a","agg_condition":[]}`)})
	// md5 of ["a","b"]; md5 of the two empty-condition md5s joined by ",".
	if err != nil || two.MetricDigest != "e53f04d1bcc1428d9e8db9a93578c5eb" || two.ConditionMD5 != "4a9031297eb2b5f8f7aae3af652c454a" ||
		swapped.MetricDigest == two.MetricDigest {
		t.Fatalf("facts %+v / %+v err %v", two, swapped, err)
	}
	if _, err := DeriveStrategyFactsV1([]json.RawMessage{json.RawMessage(`{"agg_condition":[]}`)}); err == nil {
		t.Fatal("a query config without a metric_id had facts")
	}
}
