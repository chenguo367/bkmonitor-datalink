// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"fmt"
	"sort"
	"strings"
)

// What a withheld reason means and what to do about it, decided here per
// reason and read by the page per group.
//
// The line for strategies this deployment cannot run used to carry one
// sentence for every reason under it -- "snapshot retention or completion
// offset too small, a deployment parameter" -- chosen by the line and not
// by the reason. On a deployment whose five strategies were withheld for a
// target model this build could not resolve, the page told the operator to
// change a deployment parameter; no parameter would have helped, and the
// facts under the sentence said so in a word the page had no words for. A
// reason word is decided where it is produced; the sentence that goes with
// it is decided here, once, and a reason this table does not know is said
// to be unknown rather than folded into whichever cause the line assumed.

// WithheldKind is who can act on a withheld reason. Closed: the page's
// wording table is held to this list.
type WithheldKind string

const (
	// WithheldDeploymentParameter: the strategy asks more than this
	// deployment is configured to keep or reserve; a deployment parameter,
	// raised to the value the strategy needs, admits it on the next round.
	WithheldDeploymentParameter WithheldKind = "DEPLOYMENT_PARAMETER"
	// WithheldBuildCapability: this build does not evaluate what the
	// strategy is written with; no parameter changes that, a build does.
	WithheldBuildCapability WithheldKind = "BUILD_CAPABILITY"
	// WithheldWriterAhead: the source wrote a document shape this build does
	// not read yet -- the writer went first.
	WithheldWriterAhead WithheldKind = "WRITER_AHEAD"
	// WithheldUnknownReason: a reason word this table does not know. The
	// page says so; it does not guess a cause.
	WithheldUnknownReason WithheldKind = "UNKNOWN_REASON"
)

// WithheldKinds is the closed list.
var WithheldKinds = []WithheldKind{WithheldDeploymentParameter, WithheldBuildCapability, WithheldWriterAhead, WithheldUnknownReason}

// WithheldReasonWords is one reason's meaning: who acts, what happened, and
// the next step, in the words the page shows.
type WithheldReasonWords struct {
	Kind WithheldKind `json:"kind"`
	What string       `json:"what"`
	Next string       `json:"next"`
}

// withheldReasonWords is the table, over the reasons the control plane
// attaches to the UNSUPPORTED_PHASE2_CAPABILITY disposition. A reason
// produced and not listed here reaches the page as unknown, which a test
// against the control plane's literals keeps from lasting a release.
var withheldReasonWords = map[string]WithheldReasonWords{
	"SNAPSHOT_RETENTION_INSUFFICIENT": {Kind: WithheldDeploymentParameter,
		What: "策略要的历史窗口超出本部署的快照保留期",
		Next: "把快照保留期调到策略要求的值（样本里有要求值），下一轮刷新自动接受"},
	"COMPLETION_OFFSET_BELOW_RESERVE": {Kind: WithheldDeploymentParameter,
		What: "策略要的完成偏移小于本部署的预留",
		Next: "把完成偏移预留调到策略要求的值，下一轮刷新自动接受"},
	"ALGORITHM_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "该检测算法还没迁到 Go 侧，本构建不评估它",
		Next: "等带该算法的构建；改部署参数没有用"},
	"EFFECTIVE_TIME_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "该生效时间条件还没迁到 Go 侧",
		Next: "等带该条件的构建；改部署参数没有用"},
	"QUERY_BK_DATA_LOCAL_TIME_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "该查询的本地时间字段还没迁到 Go 侧",
		Next: "等带该字段的构建；改部署参数没有用"},
	"UNSUPPORTED_PRIORITY_SEMANTICS": {Kind: WithheldBuildCapability,
		What: "策略的优先级语义本构建不支持",
		Next: "等支持该语义的构建；改部署参数没有用"},
	"UNSUPPORTED_TARGET_SCOPE": {Kind: WithheldBuildCapability,
		What: "策略的目标范围写法本构建不支持",
		Next: "等支持该目标范围的构建；改部署参数没有用"},
	"UNSUPPORTED_TARGET_SCOPE_UNRESOLVABLE": {Kind: WithheldBuildCapability,
		What: "策略的目标范围本构建解析不了",
		Next: "等能解析它的构建；改部署参数没有用"},
	"TARGET_PLAN_MODEL_REPRESENTATION_UNRESOLVED": {Kind: WithheldBuildCapability,
		What: "策略目标按模型实例（model_inst_id）给出而不带 model_match，本构建不会把它反查成主机身份，整条策略不进检测",
		Next: "等带主机模型反查的构建（读方缺的一支），策略与部署参数都不用改"},
	"UNSUPPORTED_TARGET_VALUE_SHAPE": {Kind: WithheldBuildCapability,
		What: "策略目标值的写法本构建读不出键",
		Next: "等能读该值形状的构建；改部署参数没有用"},
	"UNSUPPORTED_MULTI_ITEM_STRATEGY": {Kind: WithheldBuildCapability,
		What: "多 item 的策略本构建不支持",
		Next: "等支持多 item 的构建；改部署参数没有用"},
	"QUERY_SOURCE_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "该数据源的查询还没迁到 Go 侧",
		Next: "等带该数据源的构建；改部署参数没有用"},
	"QUERY_MIXED_PROMQL_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "混合 PromQL 的查询还没迁到 Go 侧",
		Next: "等带它的构建；改部署参数没有用"},
	"QUERY_CMDB_LEVEL_BYPASSES_UQ": {Kind: WithheldBuildCapability,
		What: "按 CMDB 层级聚合的查询绕过了统一查询，本构建不支持",
		Next: "等支持该聚合的构建；改部署参数没有用"},
	"EXPRESSION_FUNCTION_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "表达式里的函数还没迁到 Go 侧",
		Next: "等带该函数的构建；改部署参数没有用"},
	"QUERY_FUNCTION_NOT_MIGRATED": {Kind: WithheldBuildCapability,
		What: "查询里的函数还没迁到 Go 侧",
		Next: "等带该函数的构建；改部署参数没有用"},
	"UNSUPPORTED_TARGET_PLAN": {Kind: WithheldWriterAhead,
		What: "策略文档带了 target_plan 字段（field_path 指到它），本构建不解释这个字段，整条策略具名拒绝、不回退旧 target、不保留旧 Plan",
		Next: "写入方先于本构建上线了目标计划：升级到解释该字段的构建；在那之前这格应恒为 0"},
	"TARGET_PLAN_EMPTY": {Kind: WithheldWriterAhead,
		What: "写入方给的 target_plan 既没有静态目标也没有动态引用，永远匹配不到任何东西",
		Next: "写入方核这条策略的目标计划；alarmd 与部署参数都不用改"},
	"TARGET_PLAN_MISSING": {Kind: WithheldWriterAhead,
		What: "item 的目标是选择文档但没有配套的 target_plan",
		Next: "写入方补 target_plan；alarmd 与部署参数都不用改"},
	"DYNAMIC_GROUP_SOURCE_UNCONFIGURED": {Kind: WithheldDeploymentParameter,
		What: "策略的目标计划引用了动态分组，而本部署没有渲染动态分组缓存前缀",
		Next: "给部署配上动态分组缓存前缀，下一轮刷新自动接受"},
}

// WithheldWordsOf is the words for a reason, or the unknown entry naming
// the reason as it was written.
func WithheldWordsOf(reason string) WithheldReasonWords {
	if words, known := withheldReasonWords[reason]; known {
		return words
	}
	return WithheldReasonWords{Kind: WithheldUnknownReason,
		What: "原因 " + reason + "：本构建的页面还没有这个原因的说明",
		Next: "按原因词查该构建的变更说明；别按别的原因的处理办法改参数"}
}

// KnownWithheldReasons lists the reasons this table explains, sorted, for
// the test that holds it to what the control plane produces.
func KnownWithheldReasons() []string {
	reasons := make([]string, 0, len(withheldReasonWords))
	for reason := range withheldReasonWords {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	return reasons
}

// capabilityLine is the sentence for the line of strategies this
// deployment cannot run: how many, and by kind of cause -- not one cause
// for every reason. Kinds are named in the order of the closed list, and
// only the ones present.
func capabilityLine(strategies int, groups []CheckGroup) string {
	byKind := map[WithheldKind]int{}
	for _, group := range groups {
		if group.Words != nil {
			byKind[group.Words.Kind] += group.Strategies
		}
	}
	parts := make([]string, 0, len(WithheldKinds))
	for _, kind := range WithheldKinds {
		count := byKind[kind]
		if count == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d 条", withheldKindWords[kind], count))
	}
	line := fmt.Sprintf("%d 条策略这个部署跑不了（%d 种原因）", strategies, len(groups))
	if len(parts) > 0 {
		line += "——" + strings.Join(parts, "、") + "；处理办法按原因组看"
	}
	return line
}

// withheldKindWords is each kind in the words of the line.
var withheldKindWords = map[WithheldKind]string{
	WithheldDeploymentParameter: "部署参数不够",
	WithheldBuildCapability:     "本构建不支持",
	WithheldWriterAhead:         "写入方超前于本构建",
	WithheldUnknownReason:       "原因待查",
}
