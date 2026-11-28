// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

// The source kinds a realtime channel record names (its source_kind): the clean rules its result table's rows were written
// by. The set is closed and kept here once, for the reader of the channel records and the projection of their rows alike.
const (
	RealtimeSourceK8s          = "k8s"
	RealtimeSourceCustomReport = "custom_report"
	RealtimeSourcePluginFlat   = "plugin_flat"
	RealtimeSourcePluginNested = "plugin_nested"
	// RealtimeSourceUnresolved is a channel whose source the publisher has not resolved yet: a record may say it, and no
	// row of such a channel is projected.
	RealtimeSourceUnresolved = ""
)

// RealtimeSourceKinds is the closed set of the source kinds a channel's rows are projected by.
func RealtimeSourceKinds() []string {
	return []string{RealtimeSourceK8s, RealtimeSourceCustomReport, RealtimeSourcePluginFlat, RealtimeSourcePluginNested}
}
