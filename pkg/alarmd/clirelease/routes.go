// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package clirelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// The public routes. All of them sit under /api/cli/ because that is all
// the admin console forwards to alarmd, so one set of paths works through
// the console and through alarmd's own ingress alike.
const (
	VersionPath  = "/api/cli/version"
	ManifestPath = "/api/cli/release"
	ReleasesPath = "/api/cli/releases/"
	InstallPath  = "/api/cli/install.sh"
)

// Error codes a client can tell apart.
const (
	CodeUnavailable = "cli_release_unavailable"
	CodeInvalid     = "cli_release_invalid"
	CodeNotFound    = "release_not_found"
)

// Register mounts the routes on a mux. They carry no credential and read no
// runtime state, so they are mounted outside the runtime's API handler: they
// answer before the runtime is open and when the public surface is
// restricted.
func (r *Release) Register(mux *http.ServeMux) {
	mux.Handle(VersionPath, r.route(r.serveVersion))
	mux.Handle(ManifestPath, r.route(r.serveManifest))
	mux.Handle(ReleasesPath, r.route(r.serveFile))
	mux.Handle(InstallPath, r.route(r.serveInstaller))
}

func (r *Release) route(serve func(http.ResponseWriter, *http.Request)) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			response.Header().Set("Allow", "GET, HEAD")
			refuse(response, http.StatusMethodNotAllowed, "method_not_allowed", "this route answers GET and HEAD")
			return
		}
		switch err := r.Problem(); {
		case err == nil:
			serve(response, request)
		case errors.Is(err, ErrUnavailable):
			refuse(response, http.StatusServiceUnavailable, CodeUnavailable, "this alarmd was built without an alarmd-cli release")
		default:
			refuse(response, http.StatusServiceUnavailable, CodeInvalid, "the alarmd-cli release this alarmd carries is invalid; see the alarmd log")
		}
	})
}

func (r *Release) serveVersion(response http.ResponseWriter, request *http.Request) {
	serveContent(response, request, "text/plain; charset=utf-8", []byte(r.manifest.Version+"\n"))
}

func (r *Release) serveManifest(response http.ResponseWriter, request *http.Request) {
	serveContent(response, request, "application/json", r.document)
}

func (r *Release) serveInstaller(response http.ResponseWriter, request *http.Request) {
	serveContent(response, request, "text/plain; charset=utf-8", r.installer)
}

// serveFile answers /api/cli/releases/<version>/<file> with a file the
// manifest lists for this release's version, and nothing else.
func (r *Release) serveFile(response http.ResponseWriter, request *http.Request) {
	version, name, ok := strings.Cut(strings.TrimPrefix(request.URL.Path, ReleasesPath), "/")
	content, listed := r.files[name]
	if !ok || version != r.manifest.Version || !listed {
		refuse(response, http.StatusNotFound, CodeNotFound, "this alarmd serves the files of release "+r.manifest.Version+" alone; read "+ManifestPath)
		return
	}
	contentType := "application/gzip"
	if strings.HasSuffix(name, ".sha256") {
		contentType = "text/plain; charset=utf-8"
	}
	serveContent(response, request, contentType, content)
}

func serveContent(response http.ResponseWriter, request *http.Request, contentType string, content []byte) {
	response.Header().Set("Content-Type", contentType)
	http.ServeContent(response, request, "", time.Time{}, bytes.NewReader(content))
}

func refuse(response http.ResponseWriter, status int, code, message string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]any{"status": "error", "error": map[string]string{"code": code, "message": message}})
}
