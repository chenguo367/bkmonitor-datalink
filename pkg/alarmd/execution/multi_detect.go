// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

// MultiDetectContract is the query_output_config contract of an item
// detected over several inputs, each by its own thresholds, joined on the
// dimensions they declare in common.
const MultiDetectContract = "multi_detect/v1"

// The inputs one item detected over several may have: two at least, and at
// most as many as one event's detection snapshot names (four, as its named
// outputs).
const (
	MinMultiDetectInputs = 2
	MaxMultiDetectInputs = MaxNamedOutputs
)
