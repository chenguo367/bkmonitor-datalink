// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// How a physical query failed, closed: the overview's pull stage counts
// failures by these, and the query count and duration are taken at the same
// place (Config.ObserveQuery).
const (
	QueryFailureTimeout   = "timeout"
	QueryFailureRefused   = "refused"
	QueryFailureNotFound  = "not_found"
	QueryFailureTransport = "transport"
	QueryFailureDecode    = "decode"
	QueryFailureOther     = "other"
)

// QueryFailures is the closed list.
var QueryFailures = []string{QueryFailureTimeout, QueryFailureRefused, QueryFailureNotFound, QueryFailureTransport, QueryFailureDecode, QueryFailureOther}

// queryFailureOf is how a physical query failed, or "" when it answered,
// wholly or in part. The last failed attempt decides: a timeout wherever it
// was met (the transport, the gateway, the query's own budget); a refusal
// when the backend answered no (a 4xx, a throttle, a status code); not found
// when that status code names something that does not exist; transport when
// the connection or the body was cut or the gateway could not answer; decode
// when the answer broke the wire contract.
func queryFailureOf(completion execution.ProviderCompletion, err error) string {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return QueryFailureTimeout
		}
		return QueryFailureOther
	}
	if completion.Completeness != execution.CompletenessUnavailable {
		return ""
	}
	attempts := completion.RouteFacts.Attempts
	for index := len(attempts) - 1; index >= 0; index-- {
		attempt := attempts[index]
		if attempt.Result != execution.RouteAttemptFailed && attempt.ReasonCode == "" {
			continue
		}
		if attempt.ReasonCode == execution.ReasonCode(contract.ReasonQueryTimeout) {
			return QueryFailureTimeout
		}
		kind, value, _ := strings.Cut(attempt.Detail, "=")
		switch kind {
		case execution.RouteDetailKindTransport, execution.RouteDetailKindBody:
			if value == execution.TransportFailureTimeout {
				return QueryFailureTimeout
			}
			return QueryFailureTransport
		case execution.RouteDetailKindHTTPStatus:
			status, _ := strconv.Atoi(value)
			switch {
			case status == http.StatusGatewayTimeout:
				return QueryFailureTimeout
			case status >= 400 && status < 500:
				return QueryFailureRefused
			case status >= 500:
				return QueryFailureTransport
			}
			return QueryFailureOther
		case execution.RouteDetailKindResponse:
			if code, isStatus := strings.CutPrefix(value, execution.ResponseFailureStatusPrefix); isStatus {
				if strings.Contains(code, "not_exist") || strings.Contains(code, "not_found") {
					return QueryFailureNotFound
				}
				return QueryFailureRefused
			}
			return QueryFailureDecode
		}
		return QueryFailureOther
	}
	return QueryFailureOther
}
