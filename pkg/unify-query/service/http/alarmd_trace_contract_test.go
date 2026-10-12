// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/function"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
)

// The MCP/SaaS request and alarmd's compact request use the same native raw
// contract. Only metadata lookup and the ES HTTP responses are replaced here;
// HandlerQueryRaw, QueryTs parsing, routing expansion and ES DSL stay real.
func TestAlarmdTraceRawHandlerContract(t *testing.T) {
	mock.Init()

	const (
		esURL   = "http://alarmd-trace-es.example.invalid"
		tableID = "apm_global.shared_trace_example"
	)
	columns := []string{
		"trace_id", "span_id", "parent_span_id", "span_name", "start_time",
		"end_time", "elapsed_time", "kind", "resource.service.name", "status.code",
	}
	testCases := []struct {
		name          string
		body          string
		referenceName string
	}{
		{
			name:          "mcp_full",
			referenceName: "a",
			body: `{
				"query_list": [{
					"driver": "influxdb", "data_source": "bkapm",
					"table_id": "apm_global.shared_trace_example", "reference_name": "a",
					"field_name": "", "time_field": "start_time", "dimensions": [],
					"query_string": "*", "function": [], "time_aggregation": {}, "order_by": [],
					"conditions": {
						"field_list": [
							{"field_name": "bk_biz_id", "op": "eq", "value": ["42"]},
							{"field_name": "app_name", "op": "eq", "value": ["example"]},
							{"field_name": "trace_id", "op": "eq", "value": ["0123456789abcdef0123456789abcdef"]}
						],
						"condition_list": ["and", "and"]
					},
					"keep_columns": ["trace_id", "span_id", "parent_span_id", "span_name", "start_time",
						"end_time", "elapsed_time", "kind", "resource.service.name", "status.code"]
				}],
				"metric_merge": "a", "start_time": "1700000000", "end_time": "1700000060",
				"step": "60s", "space_uid": "bkcc__42", "timezone": "Asia/Shanghai",
				"order_by": ["start_time"], "limit": 2, "from": 0
			}`,
		},
		{
			name: "native_minimal",
			body: `{
				"query_list": [{
					"data_source": "bkapm", "table_id": "apm_global.shared_trace_example",
					"time_field": "start_time",
					"conditions": {
						"field_list": [
							{"field_name": "bk_biz_id", "op": "eq", "value": ["42"]},
							{"field_name": "app_name", "op": "eq", "value": ["example"]},
							{"field_name": "trace_id", "op": "eq", "value": ["0123456789abcdef0123456789abcdef"]}
						],
						"condition_list": ["and", "and"]
					},
					"keep_columns": ["trace_id", "span_id", "parent_span_id", "span_name", "start_time",
						"end_time", "elapsed_time", "kind", "resource.service.name", "status.code"]
				}],
				"start_time": "1700000000", "end_time": "1700000060",
				"space_uid": "bkcc__42", "order_by": ["start_time"], "limit": 2
			}`,
		},
	}
	aliasCase := testCases[1]
	aliasCase.name = "native_minimal_alias"
	aliasCase.body = strings.Replace(aliasCase.body, `"data_source": "bkapm"`, `"data_source": "bk_apm"`, 1)
	testCases = append(testCases, aliasCase)

	for caseIndex, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := metadata.InitHashID(context.Background())
			metadata.SetUser(ctx, &metadata.User{
				Key: "alarmd:sre", TenantID: "example", SpaceUID: "bkcc__42",
			})
			index := "alarmd_trace_contract_" + testCase.name
			storageID := fmt.Sprintf("99100%d", caseIndex)
			tsdb.SetStorage(storageID, &tsdb.Storage{
				Type: metadata.ElasticsearchStorageType, Address: esURL,
			})

			// tsdb_map is the existing structured-query metadata seam. Supplying
			// the resolved route avoids Redis while exercising its real consumer.
			// An omitted reference stays empty in the native minimal request.
			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(testCase.body), &body))
			route, err := json.Marshal(map[string]structured.TsDBs{
				testCase.referenceName: {{
					TableID: tableID, StorageID: storageID, DB: index,
					StorageType: metadata.ElasticsearchStorageType,
					TimeField: metadata.TimeField{
						Name: "start_time", Type: "long", Unit: function.Microsecond,
					},
				}},
			})
			require.NoError(t, err)
			body["tsdb_map"] = route
			requestBody, err := json.Marshal(body)
			require.NoError(t, err)

			mapping := fmt.Sprintf(`{"%s":{"mappings":{"properties":{
				"bk_biz_id":{"type":"long"},"app_name":{"type":"keyword"},
				"trace_id":{"type":"keyword"},"span_id":{"type":"keyword"},
				"parent_span_id":{"type":"keyword"},"span_name":{"type":"keyword"},
				"start_time":{"type":"long"},"end_time":{"type":"long"},
				"elapsed_time":{"type":"long"},"kind":{"type":"integer"},
				"resource":{"properties":{"service.name":{"type":"keyword"}}},
				"status":{"properties":{"code":{"type":"integer"}}}
			}}}}`, index)
			httpmock.RegisterResponder(http.MethodGet, esURL+"/"+index,
				httpmock.NewStringResponder(http.StatusOK, mapping))
			searchBodies := make(chan []byte, 2)
			httpmock.RegisterResponder(http.MethodPost, esURL+"/"+index+"/_search",
				func(request *http.Request) (*http.Response, error) {
					data, readErr := io.ReadAll(request.Body)
					if readErr != nil {
						return nil, readErr
					}
					searchBodies <- data
					// Out-of-order storage rows also exercise UQ's final merge sort.
					return httpmock.NewStringResponse(http.StatusOK, fmt.Sprintf(`{
						"hits":{"total":{"value":3,"relation":"eq"},"hits":[
							{"_index":"%s","_id":"child","_source":{
								"trace_id":"0123456789abcdef0123456789abcdef","span_id":"0000000000000002",
								"parent_span_id":"0000000000000001","span_name":"child",
								"start_time":1700000002000000,"end_time":1700000002100000,
								"elapsed_time":100000,"kind":2,"resource":{"service.name":"example-worker"},
								"status":{"code":1}}},
							{"_index":"%s","_id":"root","_source":{
								"trace_id":"0123456789abcdef0123456789abcdef","span_id":"0000000000000001",
								"parent_span_id":"","span_name":"root",
								"start_time":1700000001000000,"end_time":1700000001200000,
								"elapsed_time":200000,"kind":1,"resource":{"service.name":"example-api"},
								"status":{"code":1}}}
						]}}`, index, index)), nil
				})
			t.Cleanup(func() {
				httpmock.RegisterResponder(http.MethodGet, esURL+"/"+index, nil)
				httpmock.RegisterResponder(http.MethodPost, esURL+"/"+index+"/_search", nil)
			})

			recorder := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(recorder)
			ginContext.Request = httptest.NewRequest(http.MethodPost, "/query/ts/raw", bytes.NewReader(requestBody)).WithContext(ctx)
			ginContext.Request.Header.Set("Content-Type", "application/json")
			HandlerQueryRaw(ginContext)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Len(t, searchBodies, 1, "the native request must reach the real ES raw executor")

			// Assert the actual generated ES request, rather than a copied DTO.
			var search map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(<-searchBodies, &search))
			assert.Equal(t, "2", string(search["size"]))
			assert.Equal(t, "0", string(search["from"]))
			assert.JSONEq(t, `[{"start_time":{"order":"asc"}}]`, string(search["sort"]))
			var source map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(search["_source"], &source))
			var includes []string
			require.NoError(t, json.Unmarshal(source["includes"], &includes))
			assert.Equal(t, columns, includes)
			var queryBody, boolBody map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(search["query"], &queryBody))
			require.NoError(t, json.Unmarshal(queryBody["bool"], &boolBody))
			var filters []json.RawMessage
			require.NoError(t, json.Unmarshal(boolBody["filter"], &filters))
			require.GreaterOrEqual(t, len(filters), 2)
			assert.JSONEq(t, `{"bool":{"must":[
				{"match_phrase":{"bk_biz_id":{"query":"42"}}},
				{"match_phrase":{"app_name":{"query":"example"}}},
				{"match_phrase":{"trace_id":{"query":"0123456789abcdef0123456789abcdef"}}}
			]}}`, string(filters[0]))
			assert.JSONEq(t, `{"range":{"start_time":{
				"from":1700000000000000,"to":1700000060000000,
				"include_lower":true,"include_upper":true
			}}}`, string(filters[1]))

			queryReference := metadata.GetQueryReference(ctx)
			require.Len(t, queryReference[testCase.referenceName], 1)
			require.Len(t, queryReference[testCase.referenceName][0].QueryList, 1)
			parsedQuery := queryReference[testCase.referenceName][0].QueryList[0]
			assert.Equal(t, structured.BkApm, parsedQuery.DataSource)
			assert.Equal(t, tableID, parsedQuery.TableID)
			require.Len(t, parsedQuery.AllConditions, 1, "shared-table filters must form one AND group")
			require.Len(t, parsedQuery.AllConditions[0], 3)
			for _, condition := range parsedQuery.AllConditions[0] {
				assert.Equal(t, structured.ConditionEqual, condition.Operator)
			}
			params := metadata.GetQueryParams(ctx)
			assert.Equal(t, int64(1700000000), params.Start.Unix())
			assert.Equal(t, int64(1700000060), params.End.Unix())

			var response ListData
			decoder := json.NewDecoder(recorder.Body)
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&response))
			assert.Equal(t, int64(3), response.Total)
			require.Len(t, response.List, 2)
			assert.Equal(t, []string{tableID}, response.ResultTableID)
			assert.Nil(t, response.Status)
			assert.Equal(t, "0000000000000001", response.List[0]["span_id"])
			assert.Equal(t, "0000000000000002", response.List[1]["span_id"])
			assert.Equal(t, json.Number("1700000001000000"), response.List[0]["start_time"])
			assert.Equal(t, "example-api", response.List[0]["resource.service.name"])
			assert.Equal(t, json.Number("1"), response.List[0]["status.code"])
			assert.Equal(t, "0123456789abcdef0123456789abcdef", response.List[0]["trace_id"])
		})
	}
}
