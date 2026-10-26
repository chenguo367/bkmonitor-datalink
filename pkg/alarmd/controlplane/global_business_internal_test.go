// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

import (
	"encoding/json"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The source facts digest hashes the strategy's identity, and it names the
// observation a round confirms. An ordinary strategy's digest is the one
// the identity had before it could say global: a field every strategy
// serialized would move every observation on the upgrade and send every
// strategy through confirmation again for no change. A global strategy's
// digest differs, as its facts do.
func TestAnOrdinaryStrategysSourceDigestIsWhatItWas(t *testing.T) {
	strategy := SourceStrategy{SourceID: "1001", Document: json.RawMessage(`{"id":1001,"bk_biz_id":2}`),
		Identity: SourceIdentity{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}}
	got, err := sourceFactsDigest(strategy)
	if err != nil {
		t.Fatal(err)
	}
	type identityBefore struct {
		TenantID   string
		BusinessID string
		SpaceScope string
	}
	before, err := contract.DeriveCanonicalDigestV2("alarmd-source-object-v2", struct {
		Document    []byte             `json:"document"`
		Identity    identityBefore     `json:"identity"`
		Disposition *ObjectDisposition `json:"disposition,omitempty"`
	}{Document: strategy.Document, Identity: identityBefore{TenantID: "tenant-a", BusinessID: "2", SpaceScope: "bkcc__2"}})
	if err != nil {
		t.Fatal(err)
	}
	if got != before {
		t.Fatalf("ordinary source digest = %s, want the pre-change %s", got, before)
	}
	strategy.Identity.GlobalBusiness = true
	global, err := sourceFactsDigest(strategy)
	if err != nil {
		t.Fatal(err)
	}
	if global == got {
		t.Fatal("a global strategy's source digest is the ordinary one's")
	}
}

// A global business Plan's PromQL is admitted when every vector selector
// names a data label or a table in its metric name, the only thing the
// provider routes a PromQL selector by once the space is skipped; one bare
// name anywhere refuses the whole expression and is named in the detail.
// The expressions are the shapes strategies write: the global APM latency
// strategy's histogram_quantile, functions, aggregations with their
// grouping first or last, binary operators with vector matching, subqueries,
// offsets, strings and comments that mention metric-like words.
func TestAGlobalBusinessPromQLQueryIsAdmittedOnlyWhenEverySelectorRoutes(t *testing.T) {
	admitted := map[string]string{
		"the global APM latency strategy": "(histogram_quantile(0.95,sum by (le,app_name,bk_instance_id,service_name,span_name) " +
			"(increase(bkmonitor:apm:bk_apm_duration_bucket[60s])))) / 1000000",
		"a table":                            "sum(rate(bkmonitor:2_bkmonitor_time_series_524307:__default__:alarmd_query_failure_total[5m]))",
		"a data label without the source":    "rate(k8s:container_cpu_usage_seconds_total[2m])",
		"a custom data label":                "custom:app_metrics:requests_total > 0",
		"two selectors with vector matching": "k8s:container_memory_working_set_bytes / on (pod) group_left (node) k8s:kube_pod_info",
		"a name matcher":                     `{__name__="bkmonitor:k8s:up", pod="a"}`,
		"a name regexp":                      `count({__name__=~"bkmonitor:k8s:kube_pod_.*"})`,
		"grouping written first":             "sum without (instance) (k8s:node_load1)",
		"set operators":                      "k8s:up and k8s:ready unless k8s:cordoned",
		"a subquery and an offset":           "max_over_time(k8s:node_load1[10m:1m] offset 5m)",
		"strings that look like metrics": `label_replace(k8s:up, "dst", "bkmonitor:up", "src", "(.*)") ` +
			`+ on () group_left count_values("up", k8s:ready)`,
		"topk and scalars":   "topk(5, k8s:node_load1) * 2 + 1e3 - .5",
		"a comment":          "k8s:up # up is the bare name this comment mentions\n",
		"single quotes":      `{__name__='bkmonitor:k8s:up', job='a\'b'}`,
		"the bool modifier":  "k8s:up == bool 1",
		"an aggregation tag": "sum(k8s:up) by (pod)",
	}
	for name, expression := range admitted {
		t.Run(name, func(t *testing.T) {
			facts := execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: expression}}
			if refusal, word := globalBusinessRefusal("1001", SourceIdentity{GlobalBusiness: true}, nil, facts); refusal != nil || word != "" {
				t.Fatalf("%q refused: %+v %q", expression, refusal, word)
			}
		})
	}
	refused := map[string]struct{ expression, reason, metric string }{
		"a bare name":                   {"sum(up)", GlobalBusinessQueryTable, "up"},
		"one bare among labelled":       {"k8s:container_memory_working_set_bytes / on (pod) group_left up", GlobalBusinessQueryTable, "up"},
		"a bare name with its source":   {"bkmonitor:up", GlobalBusinessQueryTable, "bkmonitor:up"},
		"a name with too many parts":    {"rate(a:b:c:d:e[1m])", GlobalBusinessQueryTable, "a:b:c:d:e"},
		"an empty metric part":          {"k8s:", GlobalBusinessQueryTable, "k8s:"},
		"an empty data label":           {"bkmonitor::up", GlobalBusinessQueryTable, "bkmonitor::up"},
		"a bare name matcher":           {`{__name__="up"}`, GlobalBusinessQueryTable, "up"},
		"a negative name matcher alone": {`{__name__!="bkmonitor:k8s:up"}`, GlobalBusinessQueryTable, ""},
		"a selector with no name":       {`sum({pod="a"})`, GlobalBusinessQueryTable, ""},
		"no selector at all":            {"1 + 1", GlobalBusinessQueryTable, ""},
		"a log source":                  {"count(bklog:index_set:m)", GlobalBusinessQueryKind, "bklog:index_set:m"},
		"a computing platform table":    {"bkdata:db:table:m", GlobalBusinessQueryKind, "bkdata:db:table:m"},
		"an unterminated matcher":       {`k8s:up{pod="a`, GlobalBusinessQueryKind, "unreadable"},
		"an unterminated range":         {`rate(k8s:up[5m)`, GlobalBusinessQueryKind, "unreadable"},
	}
	for name, test := range refused {
		t.Run(name, func(t *testing.T) {
			facts := execution.QueryPlanFacts{PromQL: &execution.PromQLQuery{Expression: test.expression}}
			refusal, word := globalBusinessRefusal("1001", SourceIdentity{GlobalBusiness: true}, nil, facts)
			want := "reason=" + test.reason + " source=" + GlobalQuerySourcePromQL + " metric=" + test.metric
			if refusal == nil || refusal.Detail != want || word != test.reason || refusal.Reason != ReasonGlobalStrategyUnsupported {
				t.Fatalf("%q: refusal = %+v word %q, want detail %q and word %s", test.expression, refusal, word, want, test.reason)
			}
			// An ordinary strategy's PromQL is scoped by its space and is
			// none of this admission's business.
			if refusal, word := globalBusinessRefusal("1001", SourceIdentity{}, nil, facts); refusal != nil || word != "" {
				t.Fatalf("an ordinary PromQL strategy was refused: %+v %q", refusal, word)
			}
		})
	}
}
