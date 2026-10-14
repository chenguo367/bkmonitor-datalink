// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	accessuq "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/access/uq"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

// The trends read this deployment's numbers back from the platform's
// collection of them, in the space the platform files them in. Which space
// that is was decided where the collection was wired, and the wiring says
// so: bk-monitor-operator's DataID resources carry the business each kind
// of data lands in. A ServiceMonitor without a DataID of its own lands in
// the common custom-metric one; container metrics in the system k8s metric
// one. A cluster registered with more than one platform has one of each
// per platform, so every candidate business is asked, and the first space
// that has the metric is the one.

// dataIDResource is bk-monitor-operator's DataID, cluster-scoped.
var dataIDResource = schema.GroupVersionResource{Group: "monitoring.bk.tencent.com", Version: "v1beta1", Resource: "dataids"}

// trendContainer is the alarmd container's name in the workloads the chart
// renders.
const trendContainer = "alarmd"

// How long a derived space answers: a resolved one for ten minutes, an
// unresolved one for a minute, since the reason may be a collection that
// has only just started.
const (
	trendSpaceResolvedFor   = 10 * time.Minute
	trendSpaceUnresolvedFor = time.Minute
)

// trendProbeWindow is how far back a candidate space is asked for the
// metric that says it is the one.
const trendProbeWindow = 10 * time.Minute

type dataIDLister func(ctx context.Context) ([]unstructured.Unstructured, error)

// spaceProbe says whether a space has samples of an expression lately.
type spaceProbe func(ctx context.Context, space, promQL string) (bool, error)

// trendSpaces derives the two spaces and keeps them for their answer time.
type trendSpaces struct {
	list      dataIDLister
	probe     spaceProbe
	namespace string
	now       func() time.Time

	mu     sync.Mutex
	at     time.Time
	ttl    time.Duration
	spaces map[string]fleet.TrendSpace
}

// spaceKinds is each trend space with the DataID labels that say a DataID
// carries it and the expression whose samples say a candidate is the one.
func (spaces *trendSpaces) spaceKinds() []struct {
	key, isSystem, probe string
} {
	return []struct{ key, isSystem, probe string }{
		{fleet.TrendSpaceSelf, "false", "count(bkmonitor_alarmd_build_info)"},
		{fleet.TrendSpaceContainer, "true", fmt.Sprintf(`count(container_memory_working_set_bytes{namespace=%q,container=%q})`, spaces.namespace, trendContainer)},
	}
}

func (spaces *trendSpaces) get(ctx context.Context) map[string]fleet.TrendSpace {
	spaces.mu.Lock()
	defer spaces.mu.Unlock()
	at := spaces.now()
	if spaces.spaces != nil && at.Sub(spaces.at) < spaces.ttl {
		return spaces.spaces
	}
	derived := map[string]fleet.TrendSpace{}
	resolved := true
	items, err := spaces.list(ctx)
	for _, kind := range spaces.spaceKinds() {
		var space fleet.TrendSpace
		if err != nil {
			space = fleet.TrendSpace{State: fleet.TrendSpaceUnresolved, Reason: "the operator's DataID resources could not be read: " + err.Error()}
		} else {
			space = spaces.derive(ctx, items, kind.isSystem, kind.probe)
		}
		derived[kind.key] = space
		resolved = resolved && space.State == fleet.TrendSpaceResolved
	}
	spaces.spaces, spaces.at, spaces.ttl = derived, at, trendSpaceResolvedFor
	if !resolved {
		spaces.ttl = trendSpaceUnresolvedFor
	}
	return derived
}

// derive is one space: the businesses of the common metric DataIDs of the
// kind, in order, each asked for the probe's samples; the first that has
// them.
func (spaces *trendSpaces) derive(ctx context.Context, items []unstructured.Unstructured, isSystem, probe string) fleet.TrendSpace {
	businesses := map[string]bool{}
	for _, item := range items {
		labels := item.GetLabels()
		if labels["usage"] != "metric" || labels["isCommon"] != "true" || labels["isSystem"] != isSystem {
			continue
		}
		business, _, _ := unstructured.NestedString(item.Object, "spec", "labels", "bk_biz_id")
		if id, err := strconv.Atoi(business); err == nil && id > 0 {
			businesses[business] = true
		}
	}
	candidates := make([]string, 0, len(businesses))
	for business := range businesses {
		candidates = append(candidates, "bkcc__"+business)
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return fleet.TrendSpace{State: fleet.TrendSpaceUnresolved,
			Reason: fmt.Sprintf("no DataID with usage=metric, isCommon=true, isSystem=%s names a business", isSystem)}
	}
	var failures []string
	for _, candidate := range candidates {
		found, err := spaces.probe(ctx, candidate, probe)
		if err != nil {
			failures = append(failures, candidate+": "+err.Error())
			continue
		}
		if found {
			return fleet.TrendSpace{Space: candidate, State: fleet.TrendSpaceResolved, Candidates: candidates}
		}
	}
	reason := "no candidate space has samples of " + probe + " in the last 10 minutes"
	if len(failures) > 0 {
		reason += "; " + strings.Join(failures, "; ")
	}
	return fleet.TrendSpace{State: fleet.TrendSpaceUnresolved, Reason: reason, Candidates: candidates}
}

// trendRange adapts the query client the evaluation path uses.
type trendRange struct{ client *accessuq.Client }

func (provider trendRange) Range(ctx context.Context, space, promQL string, start, end time.Time, step time.Duration) (fleet.SeriesRange, error) {
	result, err := provider.client.Range(ctx, accessuq.RangeRequest{PromQL: promQL, SpaceUID: space, Start: start, End: end, Step: step})
	if err != nil {
		return fleet.SeriesRange{}, err
	}
	lines := make([]fleet.SeriesLine, 0, len(result.Series))
	for _, series := range result.Series {
		points := make([]fleet.SeriesPoint, 0, len(series.Points))
		for _, point := range series.Points {
			points = append(points, fleet.SeriesPoint{AtUnixMilli: point.AtUnixMilli, Value: point.Value})
		}
		lines = append(lines, fleet.SeriesLine{Labels: series.Labels, Points: points})
	}
	return fleet.SeriesRange{Lines: lines, Code: result.Code, Message: result.Message, Partial: result.Partial}, nil
}

// probeWith asks a space for an expression's samples over the probe window.
func probeWith(provider fleet.RangeProvider, now func() time.Time) spaceProbe {
	return func(ctx context.Context, space, promQL string) (bool, error) {
		at := now()
		result, err := provider.Range(ctx, space, promQL, at.Add(-trendProbeWindow), at, time.Minute)
		if err != nil {
			return false, err
		}
		if result.Code != "" {
			return false, nil
		}
		for _, line := range result.Lines {
			if len(line.Points) > 0 {
				return true, nil
			}
		}
		return false, nil
	}
}

// inClusterDataIDs lists the DataIDs with this process's ServiceAccount;
// out of a cluster every list says so.
func inClusterDataIDs() dataIDLister {
	var once sync.Once
	var client dynamic.Interface
	var setup error
	return func(ctx context.Context) ([]unstructured.Unstructured, error) {
		once.Do(func() {
			config, err := rest.InClusterConfig()
			if err != nil {
				setup = fmt.Errorf("not in a cluster: %w", err)
				return
			}
			client, setup = dynamic.NewForConfig(config)
		})
		if setup != nil {
			return nil, setup
		}
		list, err := client.Resource(dataIDResource).List(ctx, metav1.ListOptions{Limit: 500})
		if err != nil {
			return nil, err
		}
		return list.Items, nil
	}
}

// fleetTrends is the trends read, or nil without a query client: the route
// is then absent, which says there is nothing to read with.
func fleetTrends(client *accessuq.Client, namespace string, now func() time.Time) *fleet.Trends {
	if client == nil {
		return nil
	}
	provider := trendRange{client: client}
	spaces := &trendSpaces{list: inClusterDataIDs(), probe: probeWith(provider, now), namespace: namespace, now: now}
	return &fleet.Trends{Range: provider, Spaces: spaces.get, Scope: fleet.TrendScope{Namespace: namespace, Container: trendContainer}}
}
