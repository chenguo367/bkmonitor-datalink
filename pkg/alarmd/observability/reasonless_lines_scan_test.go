// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// reasonlessLinesAllowed are the lines written today with a result that is
// not a success and no reason code, by file and stage: each normalizes to
// reason_not_reported, the word for a site that failed to report, and each
// is to be given its reason. The list only shrinks; a line not on it fails.
var reasonlessLinesAllowed = map[string]bool{
	"cmd/alarmd/runtime_phase_two.go|StageExecutionHung":                  true,
	"cmd/alarmd/runtime_phase_two.go|StageShutdown":                       true,
	"cmd/alarmd/runtime_phase_two.go|stage":                               true,
	"cmd/alarmd/runtime_phase_two_bundle.go|StageFleetSnapshotPublish":    true,
	"cmd/alarmd/runtime_phase_two_bundle.go|StageLegacyPodCache":          true,
	"cmd/alarmd/runtime_phase_two_bundle.go|StageObservationWindow":       true,
	"cmd/alarmd/runtime_phase_two_bundle.go|StageStartup":                 true,
	"cmd/alarmd/runtime_phase_two_bundle.go|StageViewSession":             true,
	"cmd/alarmd/runtime_phase_two_production.go|StageAssignmentIndexRead": true,
	"controlplane/object_catalog_store.go|StageObjectCatalog":             true,
	"controlplane/active_qg_set.go|StageActiveQGSet":                      true,
	"controlplane/redis_catalog.go|StageActiveQGSet":                      true,
	"controlplane/timeline_prune.go|StageScheduleCutover":                 true,
	"observability/observer.go|StageSlotWait":                             true,
	"scheduler/recovery_channels.go|StageQueryPermitWait":                 true,
	"scheduler/runtime.go|StageRunnerReturned":                            true,
}

// A line whose result is not a success names its reason. One built without
// a reason normalizes to reason_not_reported, which reads as a site that
// failed to report even where the cause is known - as a target resolution
// with members dropped did. This reads every Observation literal in alarmd:
// one with a Result that is not ResultSuccess or ResultStarted and no
// ReasonCode fails, unless it is a line already listed to be given one.
func TestEveryLineThatIsNotASuccessNamesItsReason(t *testing.T) {
	root := ".."
	found := map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		relative, _ := filepath.Rel(root, path)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok || !isObservationType(literal.Type) {
				return true
			}
			result, stage, reason := "", "", false
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, _ := pair.Key.(*ast.Ident)
				if key == nil {
					continue
				}
				switch key.Name {
				case "Result":
					result = lastName(pair.Value)
				case "Stage":
					stage = lastName(pair.Value)
				case "ReasonCode":
					reason = true
				}
			}
			if result == "" || reason || result == "ResultSuccess" || result == "ResultStarted" {
				return true
			}
			key := filepath.ToSlash(relative) + "|" + stage
			found[key] = true
			if !reasonlessLinesAllowed[key] {
				t.Errorf("%s: a %s line at stage %s has no ReasonCode; it will read as reason_not_reported",
					fset.Position(literal.Pos()), result, stage)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var stale []string
	for key := range reasonlessLinesAllowed {
		if !found[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	for _, key := range stale {
		t.Errorf("%s is listed as reasonless and now names its reason: take it off the list", key)
	}
}

func isObservationType(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.SelectorExpr:
		return typed.Sel.Name == "Observation"
	case *ast.Ident:
		return typed.Name == "Observation"
	}
	return false
}

// lastName is the identifier an expression ends in - Result(x) reads as x -
// or "expr" for anything else.
func lastName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.SelectorExpr:
		return typed.Sel.Name
	case *ast.Ident:
		return typed.Name
	case *ast.CallExpr:
		if len(typed.Args) == 1 {
			return lastName(typed.Args[0])
		}
	}
	return "expr"
}
