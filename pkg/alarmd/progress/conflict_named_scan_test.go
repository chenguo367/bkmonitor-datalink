// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package progress

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// conflictNamers are the store's constructors of a conflict, each given the
// word that refused it.
var conflictNamers = map[string]bool{"commitConflict": true, "beginConflict": true, "rangeConflict": true}

// Every path of the store that returns a conflict names the fact that
// refused it: a result built with Status ProgressConflict carries its word
// (Conflict, or a pruned skip's Refusal) in the same literal, and a
// constructor of one is given a word, not an empty one. A path added later
// that leaves its word out fails here, before a reader meets an UNNAMED
// conflict on a counter.
func TestEveryConflictTheStoreReturnsIsNamed(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fileSet := token.NewFileSet()
	named, constructed := 0, 0
	for _, file := range files {
		name := file.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CompositeLit:
				conflict, word := false, false
				for _, element := range typed.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, _ := pair.Key.(*ast.Ident)
					if key == nil {
						continue
					}
					switch key.Name {
					case "Status":
						if selector, ok := pair.Value.(*ast.SelectorExpr); ok && selector.Sel.Name == "ProgressConflict" {
							conflict = true
						}
					case "Conflict", "Refusal":
						word = !isEmptyString(pair.Value)
					}
				}
				if conflict {
					named++
					if !word {
						t.Errorf("%s: a conflict built without the word that refused it", fileSet.Position(typed.Pos()))
					}
				}
			case *ast.CallExpr:
				function, ok := typed.Fun.(*ast.Ident)
				if !ok || !conflictNamers[function.Name] {
					return true
				}
				constructed++
				if len(typed.Args) == 0 || isEmptyString(typed.Args[0]) {
					t.Errorf("%s: %s given no word", fileSet.Position(typed.Pos()), function.Name)
				}
			}
			return true
		})
	}
	// The scan reads something: the store's three constructors and the
	// pruned skip's literals, and the nineteen paths that call a
	// constructor - four of a commit, seven of a BeginSlot, eight of a range.
	if named < 3 || constructed < 19 {
		t.Fatalf("the scan found %d conflict literals and %d constructed conflicts; it is reading nothing", named, constructed)
	}
	t.Logf("%d conflict literals, %d constructed conflicts, every one named", named, constructed)
}

func isEmptyString(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	return ok && literal.Kind == token.STRING && (literal.Value == `""` || literal.Value == "``")
}

// A conflict a path returned without a word is counted UNNAMED, not dropped
// and not refused as an invalid result.
func TestAConflictWithoutAWordIsUnnamedAndStillValid(t *testing.T) {
	if word := execution.ConflictWord(""); word != string(execution.ConflictUnnamed) {
		t.Fatalf("an unnamed conflict's word = %q, want UNNAMED", word)
	}
	if err := (execution.ProgressCommitResult{Status: execution.ProgressConflict}).Validate(); err != nil {
		t.Fatalf("an unnamed conflict is refused as invalid: %v", err)
	}
	if err := (execution.ProgressBeginResult{Status: execution.ProgressConflict}).Validate(); err != nil {
		t.Fatalf("an unnamed BeginSlot conflict is refused as invalid: %v", err)
	}
}
