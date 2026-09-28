// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// Every reason the control plane attaches to the capability disposition has
// words here, and every word here is a reason the control plane still
// produces. The literals are read from the source, not from a hand-kept
// list: the line that once said "a deployment parameter" for every reason
// was written against the reasons of its day and never learned the ones
// that came after -- one of which is what a live deployment's five
// strategies were withheld for.
func TestEveryCapabilityReasonHasWordsAndEveryWordIsAReason(t *testing.T) {
	produced := map[string]bool{}
	literal := regexp.MustCompile(`"([A-Z][A-Z0-9_]{5,})"`)
	// Reasons attached inline beside the disposition, and the ones handed
	// to the compilers' unsupported constructors and the target plan's
	// decoder, whose names are the only place they are spelled.
	sources := map[string]*regexp.Regexp{
		// The normalized disposition's reasons are named constants beside
		// the disposition; the words table carries them because the group
		// under CONFIG_NORMALIZED shows the reason's words the same way.
		"../controlplane": regexp.MustCompile(`DispositionUnsupported,?[^\n]*\n?[^\n]*Reason: "([A-Z_]+)"|queryUnsupported\("([A-Z_]+)"|return "(UNSUPPORTED_[A-Z_]+)"|Reason(?:EffectiveTimeRangeInvalid|PriorityIgnored|LevelTriggerBorrowed|AggIntervalDefaulted) = "([A-Z_]+)"`),
		"../targetplan":   regexp.MustCompile(`Reason[A-Za-z]* += "([A-Z_]+)"`),
		"../contract":     regexp.MustCompile(`Reason(?:SnapshotRetentionInsufficient|CompletionOffsetBelowReserve) += "([A-Z_]+)"`),
	}
	for dir, pattern := range sources {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			source, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, match := range pattern.FindAllStringSubmatch(string(source), -1) {
				for _, group := range match[1:] {
					if group != "" && literal.MatchString(`"`+group+`"`) {
						produced[group] = true
					}
				}
			}
		}
	}
	// The runtime compiler's terminals are a third producer, filed by
	// CompilerTerminalDisposition and spelled nowhere the regexes above
	// read: every reason constant of the contract goes through it, and the
	// ones it files under the capability disposition are produced.
	reasonConstant := regexp.MustCompile(`(?m)^\s*Reason[A-Za-z0-9]+\s*=\s*"([A-Z][A-Z0-9_]+)"`)
	entries, err := os.ReadDir("../contract")
	if err != nil {
		t.Fatal(err)
	}
	terminals := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(filepath.Join("../contract", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range reasonConstant.FindAllStringSubmatch(string(source), -1) {
			if disposition, known := controlplane.CompilerTerminalDisposition(match[1]); known && disposition == controlplane.DispositionUnsupported {
				produced[match[1]] = true
				terminals++
			}
		}
	}
	if terminals < 3 {
		t.Fatalf("read %d compiler terminals filed under the capability disposition, want at least the three the compiler produces", terminals)
	}
	// The dispositions the control plane builds itself, read from the syntax
	// rather than the text: a reason named by a constant is as produced as
	// one spelled in place, and the literal pattern above never saw it --
	// GLOBAL_STRATEGY_UNSUPPORTED withheld strategies with no words at all.
	for _, reason := range capabilityDispositionReasons(t) {
		produced[reason] = true
	}
	if len(produced) < 8 {
		t.Fatalf("read %d reasons from the source, too few to be the set: %v", len(produced), produced)
	}
	for reason := range produced {
		if _, known := withheldReasonWords[reason]; !known {
			t.Errorf("the control plane withholds under %q and the page has no words for it: it would read as unknown", reason)
		}
	}
	for _, reason := range KnownWithheldReasons() {
		if !produced[reason] {
			t.Errorf("words for %q, which nothing in the control plane produces", reason)
		}
		if words := withheldReasonWords[reason]; words.What == "" || words.Next == "" || words.Kind == "" || words.Kind == WithheldUnknownReason {
			t.Errorf("%q has incomplete words: %+v", reason, words)
		}
	}
	unknown := WithheldWordsOf("SOMETHING_NEW")
	if unknown.Kind != WithheldUnknownReason || !strings.Contains(unknown.What, "SOMETHING_NEW") || strings.Contains(unknown.Next, "部署参数") && !strings.Contains(unknown.Next, "别") {
		t.Errorf("an unknown reason = %+v, want named as unknown and not sent to a parameter", unknown)
	}
}

// The capability line counts strategies by kind of cause and names no cause
// the reasons under it do not carry: five strategies withheld for a target
// this build cannot resolve read as "this build does not support", not as a
// deployment parameter -- which is what the page used to say of every
// reason on this line, and what an operator was sent to change.
func TestTheCapabilityLineNamesTheKindsUnderItAndNotOneCauseForAll(t *testing.T) {
	facts := NewSourceFacts(now, map[string]int{"UNSUPPORTED_PHASE2_CAPABILITY": 7, "ACCEPTED": 40}, []WithheldObject{
		{StrategyID: "25", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"},
		{StrategyID: "351", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"},
		{StrategyID: "387", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"},
		{StrategyID: "472", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"},
		{StrategyID: "474", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "ALGORITHM_NOT_MIGRATED", FieldPath: "items[0].algorithms[0]"},
		{StrategyID: "600", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "SNAPSHOT_RETENTION_INSUFFICIENT"},
		{StrategyID: "601", Scope: "PLAN", Disposition: "UNSUPPORTED_PHASE2_CAPABILITY", Reason: "NEW_WORD_NOBODY_EXPLAINED"},
	})
	view := View{Source: facts, SourceReplica: "pod-a"}
	var line *CheckReport
	for _, report := range ReportChecks(nil, nil, &view, now) {
		if report.Code == CheckCapabilityUnsupported {
			copied := report
			line = &copied
		}
	}
	if line == nil {
		t.Fatal("no capability line")
	}
	if line.Strategies != 7 || len(line.Groups) != 3 {
		t.Fatalf("line = %+v, want 7 strategies in 3 groups", line)
	}
	if want := "7 条策略这个部署跑不了（3 种原因）——本构建不支持 5 条、策略定义超出护栏 1 条、原因待查 1 条；处理办法按原因组看"; line.Line != want {
		t.Errorf("line = %q\nwant %q", line.Line, want)
	}
	if strings.Contains(line.Line, "快照保留期") {
		t.Errorf("the line still states a cause for every reason: %q", line.Line)
	}
	words := map[string]*WithheldReasonWords{}
	for _, group := range line.Groups {
		words[group.Key] = group.Words
	}
	if algorithm := words["ALGORITHM_NOT_MIGRATED"]; algorithm == nil || algorithm.Kind != WithheldBuildCapability ||
		!strings.Contains(algorithm.Next, "改部署参数没有用") {
		t.Errorf("algorithm words = %+v, want this build's gap with nothing for the operator to change", algorithm)
	}
	// The compiler's terminals under the same disposition: a budget it
	// guards with is the strategy's to shrink first, not a parameter to
	// raise, and an algorithm it has no evaluator for is the build's.
	for reason, kind := range map[string]WithheldKind{"PLAN_BUDGET_EXCEEDED": WithheldStrategyDefinition,
		"LEVEL_BUDGET_EXCEEDED": WithheldStrategyDefinition, "ALGORITHM_UNSUPPORTED": WithheldBuildCapability} {
		if words := WithheldWordsOf(reason); words.Kind != kind || (kind == WithheldStrategyDefinition && !strings.Contains(words.Next, "先收")) {
			t.Errorf("%s words = %+v, want kind %s", reason, words, kind)
		}
	}
	// Past the state store's own ceiling the retention cannot be raised by
	// anyone but the strategy: the words send the reader to its cadence and
	// say that a deployment parameter will not help.
	if retention := words["SNAPSHOT_RETENTION_INSUFFICIENT"]; retention == nil || retention.Kind != WithheldStrategyDefinition ||
		!strings.Contains(retention.Next, "评估周期") || !strings.Contains(retention.Next, "调部署参数没有用") {
		t.Errorf("retention words = %+v, want the strategy's cadence and no deployment parameter", retention)
	}
	if unknown := words["NEW_WORD_NOBODY_EXPLAINED"]; unknown == nil || unknown.Kind != WithheldUnknownReason || !strings.Contains(unknown.What, "NEW_WORD_NOBODY_EXPLAINED") {
		t.Errorf("unknown reason words = %+v, want the reason named as unknown", unknown)
	}
}

// capabilityDispositionReasons is every reason the control plane sets beside
// Disposition: DispositionUnsupported in a composite literal, resolved to its
// word whether it is written as a literal or names a constant of the control
// plane or of a package it imports.
func capabilityDispositionReasons(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	packages := map[string]string{"controlplane": "../controlplane", "contract": "../contract", "strategy": "../strategy"}
	type constant struct {
		pkg  string
		expr ast.Expr
	}
	constants := map[string]constant{}
	files := map[string][]*ast.File{}
	for name, dir := range packages {
		parsed, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range parsed {
			for _, file := range pkg.Files {
				files[name] = append(files[name], file)
				for _, decl := range file.Decls {
					gen, ok := decl.(*ast.GenDecl)
					if !ok || gen.Tok != token.CONST {
						continue
					}
					for _, spec := range gen.Specs {
						value := spec.(*ast.ValueSpec)
						for index, ident := range value.Names {
							if index < len(value.Values) {
								constants[name+"."+ident.Name] = constant{pkg: name, expr: value.Values[index]}
							}
						}
					}
				}
			}
		}
	}
	var resolve func(pkg string, expr ast.Expr, depth int) (string, bool)
	resolve = func(pkg string, expr ast.Expr, depth int) (string, bool) {
		if depth > 8 {
			return "", false
		}
		key := ""
		switch expr := expr.(type) {
		case *ast.BasicLit:
			if expr.Kind != token.STRING {
				return "", false
			}
			word, err := strconv.Unquote(expr.Value)
			return word, err == nil
		case *ast.Ident:
			key = pkg + "." + expr.Name
		case *ast.SelectorExpr:
			qualifier, ok := expr.X.(*ast.Ident)
			if !ok {
				return "", false
			}
			key = qualifier.Name + "." + expr.Sel.Name
		default:
			return "", false
		}
		found, ok := constants[key]
		if !ok {
			return "", false
		}
		return resolve(found.pkg, found.expr, depth+1)
	}
	var reasons []string
	for _, file := range files["controlplane"] {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			var disposition, reason ast.Expr
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := pair.Key.(*ast.Ident); ok {
					switch key.Name {
					case "Disposition":
						disposition = pair.Value
					case "Reason":
						reason = pair.Value
					}
				}
			}
			if ident, ok := disposition.(*ast.Ident); !ok || ident.Name != "DispositionUnsupported" || reason == nil {
				return true
			}
			if word, ok := resolve("controlplane", reason, 0); ok {
				reasons = append(reasons, word)
			}
			return true
		})
	}
	return reasons
}
