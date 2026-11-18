// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

// askedWindowEnd is the end of the window a query request asks for, as the
// query service reads its end_time: the provider takes a range's end in, so
// an aligned request of a metric time series - asked a second before its
// window's end (access/uq requestEnd) - ends its window a second after
// end_time; an unaligned one is asked as its window ends. The fakes that
// answer by the window place their points and pick their round by it.
func askedWindowEnd(endTime int64, notTimeAlign bool) int64 {
	if notTimeAlign {
		return endTime
	}
	return endTime + 1
}
