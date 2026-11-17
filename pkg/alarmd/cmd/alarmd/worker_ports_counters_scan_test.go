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
	"os"
	"strings"
	"testing"
)

// workerCounterPorts are the worker's counting ports production binds to the
// recorder. Each is optional to the worker - nil counts nothing - and its
// families have every cell from the start, so a port left unset reads on
// /metrics as zeros, as if nothing it counts ever happened.
var workerCounterPorts = []string{"SlotCounts", "ReadAheadWaits"}

// Every worker.Ports production builds binds each counting port to the
// recorder.
func TestProductionWorkerPortsBindEveryCountingPortToTheRecorder(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	built := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Ports" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "worker" {
				return true
			}
			built++
			bound := map[string]string{}
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, _ := pair.Key.(*ast.Ident)
				value, _ := pair.Value.(*ast.Ident)
				if key != nil && value != nil {
					bound[key.Name] = value.Name
				}
			}
			for _, port := range workerCounterPorts {
				if bound[port] != "recorder" {
					t.Errorf("%s:%d: worker.Ports binds %s to %q, want the recorder", name,
						fileSet.Position(literal.Pos()).Line, port, bound[port])
				}
			}
			return true
		})
	}
	if built == 0 {
		t.Fatal("no worker.Ports was found: the scan would pass vacuously")
	}
}
