// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

// Endpoint is one external system a replica reads from or writes to, as the
// replica resolved it: where, under what key space, and what it has seen of
// it lately.
//
// The page had no way to answer "which Redis is this", and the reader's next
// question -- "is it the one the platform writes to" -- is what a deployment
// with an empty Active Set turns on. The address is the deployment's; the
// credentials are never here. Where the replica reads something the platform
// wrote, Writer says what it found, because the address alone cannot tell a
// Redis nobody writes to from the right one.
type Endpoint struct {
	// Role is the closed name of what the replica uses it for; EndpointRoles
	// lists them.
	Role string `json:"role"`
	// Kind is redis, kafka or http.
	Kind string `json:"kind"`
	// Address is host:port, several joined with commas, or for a sentinel
	// deployment the master name followed by the sentinel addresses. Never a
	// username or password.
	Address string `json:"address"`
	Mode    string `json:"mode,omitempty"`
	// DB is the logical database, for Redis only.
	DB *int `json:"db,omitempty"`
	// Prefix is the key space or topic the role reads or writes under.
	Prefix string `json:"prefix,omitempty"`
	// SharedWith names the role whose connection this one reuses, when the
	// deployment resolved two roles to the same instance and database. The
	// health facts below then belong to that connection.
	SharedWith string `json:"shared_with,omitempty"`
	// Configured is false when the deployment renders no coordinates for the
	// role at all, which is a different answer from an address that does not
	// answer.
	Configured bool `json:"configured"`
	// LastSuccessAgeSeconds and LastFailureAgeSeconds are how long since this
	// process last completed a command against the connection, and last
	// failed one; LastFailure is what the failure said, sanitised and bounded.
	// Absent until the process has done either.
	LastSuccessAgeSeconds *float64 `json:"last_success_age_seconds,omitempty"`
	LastFailureAgeSeconds *float64 `json:"last_failure_age_seconds,omitempty"`
	LastFailure           string   `json:"last_failure,omitempty"`
	// Ready and Attempts are for a role the replica opens rather than calls:
	// whether it is open now, and how many attempts it has made, which for
	// an open one is how many it took. Absent for the roles read through a
	// connection, whose health is the command record above.
	Ready    *bool `json:"ready,omitempty"`
	Attempts *int  `json:"attempts,omitempty"`
	// ReadySinceAgeSeconds is how long an open role has been open. It is its
	// own field because it is not a success: the sink records opening, not
	// messages, and put in LastSuccessAgeSeconds it read on a live page as
	// "last succeeded sixteen minutes ago" on a producer that had been
	// sending every second since. Absent while not open.
	ReadySinceAgeSeconds *float64 `json:"ready_since_age_seconds,omitempty"`
	// Writer is what the replica found of the platform's writing under this
	// role, for the roles that read a platform cache.
	Writer *WriterEvidence `json:"writer,omitempty"`
	// ProtocolVersion is the protocol version this replica's client speaks to
	// the role, as configured, and HeadersSupported whether that version can
	// carry record headers -- which the standard raw event does. Present on
	// the output role only. A client told the broker is older than 0.11
	// refuses every event with a header before any byte leaves, and on a live
	// deployment that read for an afternoon as the broker being unavailable;
	// the version was in the configuration the whole time and on no screen.
	ProtocolVersion  string `json:"protocol_version,omitempty"`
	HeadersSupported *bool  `json:"headers_supported,omitempty"`
	// Checks is what this replica verified about the role at startup, each
	// by name with its verdict and, when it failed, why. A configuration
	// error has to be readable before the first message, not inferred from
	// the first message failing.
	Checks []EndpointCheck `json:"checks,omitempty"`
}

// EndpointCheck is one startup verification of a role: the name from
// EndpointCheckNames, whether it passed, and the sentence when it did not.
type EndpointCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// The checks a replica reports on the output role. Closed: a reader shows
// these words and no others.
const (
	// EndpointCheckBrokerVersion: the configured protocol version parses and
	// is one the client supports.
	EndpointCheckBrokerVersion = "broker_version"
	// EndpointCheckRecordHeaders: the configured protocol version can carry
	// the record headers the standard raw event needs.
	EndpointCheckRecordHeaders = "record_headers"
)

// EndpointCheckNames is every name an EndpointCheck can carry.
var EndpointCheckNames = []string{EndpointCheckBrokerVersion, EndpointCheckRecordHeaders}

// WriterEvidence is what a replica found of the platform's writing under a
// dependency it only reads: how much is there and how old it is. It is the
// evidence that the address is the one the platform writes to, and that the
// writer is alive; neither follows from the connection answering.
type WriterEvidence struct {
	// Present says the replica has read the writer's content at least once.
	Present bool `json:"present"`
	// Count is what there is: strategies listed, hosts indexed.
	Count int `json:"count"`
	// AgeSeconds is how old the writer's content is by its own marker, when it
	// carries one: the strategy cache's change marker, the host index's
	// source refresh time.
	AgeSeconds *float64 `json:"age_seconds,omitempty"`
	// State is the reader's own reading of the copy, in the reader's closed
	// words, when it keeps one: never_loaded, index_stale, index_empty for the
	// host cache; not_configured, authoritative and the like for the settings
	// copy. Empty where the reader keeps no such state.
	State string `json:"state,omitempty"`
}

// The endpoint roles, closed. The page's wording table is held to this list.
const (
	EndpointStateRedis    = "state_redis"
	EndpointStrategyCache = "strategy_cache"
	EndpointCMDBCache     = "cmdb_cache"
	EndpointDynamicConfig = "dynamic_config"
	EndpointOutputKafka   = "output_kafka"
	EndpointQueryBackend  = "query_backend"
	EndpointCompatOutput  = "compat_output_redis"
)

// EndpointRoles is the closed list, in the order the page shows them: the
// replica's own storage first, then what it reads of the platform's, then
// where its work goes and where its queries go.
var EndpointRoles = []string{
	EndpointStateRedis, EndpointStrategyCache, EndpointCMDBCache, EndpointDynamicConfig,
	EndpointQueryBackend, EndpointOutputKafka, EndpointCompatOutput,
}
