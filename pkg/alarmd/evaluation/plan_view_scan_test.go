// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package evaluation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// planViewForIsTheOnlyPlaceAllowedToChooseAPlanView names the one function that
// may pick which view of a Plan an evaluation sees.
const planViewForIsTheOnlyPlaceAllowedToChooseAPlanView = "planViewFor"

// Which levels a series is judged against is decided in one place.
//
// It is decided by choosing which view of the Plan the evaluation sees, and
// everything downstream - here, the detector, the trigger and the execution
// contract's validators - then reads the levels off the Plan it was handed, in
// fifteen places across four packages. A second place choosing a view would put
// a series in front of the wrong levels quietly, because every one of those
// fifteen would keep agreeing with whatever it was given.
//
// Tests are exempt: a test building a view is stating a fixture.
func TestOnlyPlanViewForChoosesWhichPlanAnEvaluationSees(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		checked++
		var enclosing string
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncDecl:
				enclosing = typed.Name.Name
			case *ast.CallExpr:
				selector, ok := typed.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "NoDataView" || len(typed.Args) != 0 {
					return true
				}
				if enclosing == planViewForIsTheOnlyPlaceAllowedToChooseAPlanView {
					return true
				}
				t.Errorf("%s:%d: %s chooses a Plan view; which levels a series is judged against is "+
					"decided in %s and nowhere else, because everything downstream reads it off the "+
					"Plan it was handed",
					name, fileSet.Position(typed.Pos()).Line, enclosing,
					planViewForIsTheOnlyPlaceAllowedToChooseAPlanView)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no non-test source was read; the scan would pass vacuously")
	}

	// And the one allowed call is actually there, so the rule is not being kept
	// by there being nothing to keep.
	source, err := os.ReadFile("plan_view.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "due.CompiledPlan.NoDataView()") {
		t.Fatal("planViewFor no longer chooses a view, so this scan is checking nothing")
	}
}
