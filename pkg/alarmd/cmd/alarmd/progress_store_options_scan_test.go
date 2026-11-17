// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const progressPackagePath = "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"

// progressStoreOptionsBuilders are the functions that may build a
// progress.StoreOptions, by file: the one production opens the owner's store
// with, whose hooks TestTheProductionProgressStoreCountsItsReadsAndSupersedesOnTheRecorder
// holds to the recorder, and the CLI's read-only diagnosis, which loads a
// record and writes none, so it has no read or supersede of an owner to count.
var progressStoreOptionsBuilders = map[string]string{
	"cmd/alarmd/runtime_roles_core.go":         "productionProgressStoreOptions",
	"cmd/alarmd/runtime_phase_two_cli_slot.go": "cliSlotReadHold",
}

// A Progress store is opened from options built in one of two places, every
// non-test source of the module scanned. The production store's counts are
// families every cell of which exists from the start: a second place that
// built its options and left a hook unset would read on /metrics as zeros,
// as if the owner never read its record and nothing were ever superseded,
// and no test of the first place would see it. Building counts a composite
// literal, a declaration of the type and new(StoreOptions), under whatever
// name the file imports the package.
func TestOnlyTheNamedFunctionsBuildAProgressStoresOptions(t *testing.T) {
	root := filepath.Join("..", "..")
	found := map[string]bool{}
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", relative, err)
		}
		scanned++
		local := ""
		for _, spec := range file.Imports {
			if importPath, _ := strconv.Unquote(spec.Path.Value); importPath == progressPackagePath {
				local = "progress"
				if spec.Name != nil {
					local = spec.Name.Name
				}
			}
		}
		if local == "" || local == "_" {
			return nil
		}
		isOptions := func(expression ast.Expr) bool {
			selector, ok := expression.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "StoreOptions" {
				return false
			}
			identifier, ok := selector.X.(*ast.Ident)
			return ok && identifier.Name == local
		}
		for _, declaration := range file.Decls {
			function, _ := declaration.(*ast.FuncDecl)
			enclosing := ""
			if function != nil {
				enclosing = function.Name.Name
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				built := false
				switch typed := node.(type) {
				case *ast.CompositeLit:
					built = isOptions(typed.Type)
				case *ast.ValueSpec:
					built = isOptions(typed.Type)
				case *ast.CallExpr:
					identifier, ok := typed.Fun.(*ast.Ident)
					built = ok && identifier.Name == "new" && len(typed.Args) == 1 && isOptions(typed.Args[0])
				}
				if !built {
					return true
				}
				if progressStoreOptionsBuilders[relative] == enclosing && enclosing != "" {
					found[relative] = true
					return true
				}
				t.Errorf("%s:%d: %s builds a progress.StoreOptions; production builds it in productionProgressStoreOptions, "+
					"whose hooks a test holds to the recorder, and a read-only one is named in progressStoreOptionsBuilders",
					relative, fileSet.Position(node.Pos()).Line, enclosing)
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("no source was read; the scan would pass vacuously")
	}
	// And each named builder still builds one, so the rule is not kept by
	// there being nothing to keep.
	for file, function := range progressStoreOptionsBuilders {
		if !found[file] {
			t.Errorf("%s no longer builds a progress.StoreOptions in %s; take it off the list", file, function)
		}
	}
}
