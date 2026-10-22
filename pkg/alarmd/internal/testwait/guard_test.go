// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package testwait

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// justification marks a fixed wait that is meant: something must not happen
// within the duration, or the duration is the behaviour under test. It counts
// only in a line comment on the wait's own line and only with a reason after
// it: a marker on the line above carries over to whatever is written under it
// later, and an empty one says nothing.
const justification = "fixed-wait:"

// testwaitPath is this package. A wait bounded by what one of its functions
// returns is bounded by the test's deadline, not by a duration of its own.
const testwaitPath = "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"

// timers are the standard library's waits on a duration: they end when the
// duration does, not when the thing waited for happens.
var timers = map[string]bool{
	"time.Sleep":     true,
	"time.After":     true,
	"time.AfterFunc": true,
	"time.NewTimer":  true,
	"time.NewTicker": true,
	"time.Tick":      true,
}

// contextBounds end a context after a duration or at a time. The parent it
// derives from does not change that: the bound is the second argument.
var contextBounds = map[string]bool{
	"context.WithTimeout":       true,
	"context.WithDeadline":      true,
	"context.WithTimeoutCause":  true,
	"context.WithDeadlineCause": true,
}

// clockReads are how a loop watches the wall clock.
var clockReads = map[string]bool{
	"time.Now":   true,
	"time.Since": true,
	"time.Until": true,
}

// timeComparisons are the methods a time.Time is compared with.
var timeComparisons = map[string]bool{
	"Before":  true,
	"After":   true,
	"Equal":   true,
	"Compare": true,
}

// fixedWait is one wait the guard reports: where it starts and what it is.
type fixedWait struct {
	file string
	line int
	kind string
}

func (w fixedWait) String() string {
	return w.file + ":" + strconv.Itoa(w.line) + ": " + w.kind
}

// fixedWaits returns the waits in file that are bounded by a duration rather
// than by what they wait for and do not say why on their own line. file must
// be parsed with its comments and with the parser's identifier resolution,
// which is how a local variable called time is told from the package; the
// waits are reported under filename.
func fixedWaits(fset *token.FileSet, file *ast.File, filename string) []fixedWait {
	w := &waitWalk{
		fset:      fset,
		filename:  filename,
		imports:   map[string]string{},
		self:      file.Name.Name == "testwait",
		justified: map[int]bool{},
		called:    map[ast.Expr]bool{},
	}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch name {
		case "_":
		case ".":
			w.dotted = append(w.dotted, path)
		default:
			w.imports[name] = path
		}
	}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			text, line := strings.CutPrefix(comment.Text, "//")
			if !line {
				continue
			}
			reason, marked := strings.CutPrefix(strings.TrimSpace(text), justification)
			if marked && strings.TrimSpace(reason) != "" {
				w.justified[fset.Position(comment.Slash).Line] = true
			}
		}
	}
	var enclosing []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			enclosing = enclosing[:len(enclosing)-1]
			return true
		}
		switch n := n.(type) {
		case *ast.CallExpr:
			w.call(n)
		case *ast.SelectorExpr:
			// A function value handed on (sleep := time.Sleep) waits as
			// surely as the call it is made into later.
			if name := w.qualified(n); !w.called[n] && (timers[name] || contextBounds[name]) {
				w.report(n.Pos(), name+" as a value")
			}
		case *ast.ForStmt:
			w.loop(n.For, enclosing, []ast.Node{n.Init, n.Cond, n.Post}, n.Body)
		case *ast.RangeStmt:
			w.loop(n.For, enclosing, nil, n.Body)
		}
		enclosing = append(enclosing, n)
		return true
	})
	sort.SliceStable(w.found, func(i, j int) bool { return w.found[i].line < w.found[j].line })
	return w.found
}

// waitWalk is the state of fixedWaits over one file.
type waitWalk struct {
	fset     *token.FileSet
	filename string
	// imports maps the name a package goes by in this file to its path, so
	// stdtime.Sleep is time.Sleep and time.Sleep is nothing when "time" is
	// not imported under that name.
	imports map[string]string
	// dotted are the paths imported with a dot, whose functions are called
	// bare.
	dotted []string
	// self is set in package testwait, which calls its own functions bare.
	self bool
	// justified holds the lines carrying a "// fixed-wait: <reason>" comment.
	justified map[int]bool
	// called holds the function expressions already judged as calls, so the
	// selector inside a call is not reported a second time as a value.
	called map[ast.Expr]bool
	found  []fixedWait
}

func (w *waitWalk) report(at token.Pos, kind string) {
	line := w.fset.Position(at).Line
	if !w.justified[line] {
		w.found = append(w.found, fixedWait{file: w.filename, line: line, kind: kind})
	}
}

// qualified names the package-level function expr refers to as path.Name, or
// "" when it refers to none. The parser binds each identifier declared in the
// file to its declaration (Obj); a package name is never bound that way, so a
// bound identifier is a local variable or function, whatever it is called.
func (w *waitWalk) qualified(expr ast.Expr) string {
	switch e := ast.Unparen(expr).(type) {
	case *ast.SelectorExpr:
		x, ok := e.X.(*ast.Ident)
		if !ok || x.Obj != nil {
			return ""
		}
		if path, ok := w.imports[x.Name]; ok {
			return path + "." + e.Sel.Name
		}
	case *ast.Ident:
		if e.Obj == nil {
			for _, path := range w.dotted {
				name := path + "." + e.Name
				if timers[name] || contextBounds[name] || clockReads[name] || path == testwaitPath {
					return name
				}
			}
		}
		if w.self && ast.IsExported(e.Name) && (e.Obj == nil || e.Obj.Kind == ast.Fun) {
			return testwaitPath + "." + e.Name
		}
	}
	return ""
}

// call reports a timer, and a context bound whose bound is not the test's
// own deadline.
func (w *waitWalk) call(call *ast.CallExpr) {
	fun := ast.Unparen(call.Fun)
	name := w.qualified(fun)
	switch {
	case timers[name]:
		w.called[fun] = true
		w.report(call.Pos(), name)
	case contextBounds[name]:
		w.called[fun] = true
		// context.WithDeadline(ctx, testwait.Deadline(t)) ends with the test
		// by construction. time.Until(testwait.Deadline(t)) does not count:
		// whatever is wrapped around the call is a computation of its own.
		if len(call.Args) > 1 && w.intoTestwait(call.Args[1]) {
			return
		}
		parent := "nothing"
		if len(call.Args) > 0 {
			parent = w.parentKind(call.Args[0])
		}
		w.report(call.Pos(), name+" on "+parent)
	}
}

// intoTestwait reports whether expr is itself a call of a testwait function.
func (w *waitWalk) intoTestwait(expr ast.Expr) bool {
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	return ok && strings.HasPrefix(w.qualified(call.Fun), testwaitPath+".")
}

// parentKind says what a context bound derives from, for the report only.
func (w *waitWalk) parentKind(expr ast.Expr) string {
	switch e := ast.Unparen(expr).(type) {
	case *ast.CallExpr:
		name := w.qualified(e.Fun)
		switch {
		case name == "context.Background" || name == "context.TODO":
			return name + "()"
		case strings.HasPrefix(name, testwaitPath+"."):
			return "testwait." + strings.TrimPrefix(name, testwaitPath+".") + "(t)"
		}
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && name == "" && sel.Sel.Name == "Context" && len(e.Args) == 0 {
			return "t.Context()"
		}
		return "a call"
	case *ast.Ident, *ast.SelectorExpr:
		return "a variable"
	}
	return "an expression"
}

// loop reports a loop that waits on the wall clock: one whose init, condition
// or post reads the clock, or whose body tests it in an if or switch. Function
// literals in the body may never run, and a loop nested in the body is judged
// on its own, so neither is looked into. A loop is let be when every time it
// compares the clock with holds the test's deadline: it is then a wait on its
// condition, bounded by the test.
func (w *waitWalk) loop(at token.Pos, enclosing []ast.Node, header []ast.Node, body *ast.BlockStmt) {
	var compared []ast.Node
	for _, part := range header {
		if part != nil {
			compared = append(compared, w.clockComparisons(part)...)
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		var init ast.Stmt
		var tests []ast.Node
		switch n := n.(type) {
		case *ast.FuncLit, *ast.ForStmt, *ast.RangeStmt:
			return false
		case *ast.IfStmt:
			init, tests = n.Init, []ast.Node{n.Cond}
		case *ast.SwitchStmt:
			init = n.Init
			if n.Tag != nil {
				tests = append(tests, n.Tag)
			}
			for _, clause := range n.Body.List {
				for _, test := range clause.(*ast.CaseClause).List {
					tests = append(tests, test)
				}
			}
		default:
			return true
		}
		for _, test := range tests {
			compared = append(compared, w.clockComparisons(test)...)
		}
		compared = append(compared, w.clockTestedThrough(init, tests)...)
		return true
	})
	if len(compared) == 0 {
		return
	}
	var fn ast.Node
	for _, n := range enclosing {
		if _, ok := n.(*ast.FuncDecl); ok {
			fn = n
			break
		}
		if _, ok := n.(*ast.FuncLit); ok {
			fn = n
			break
		}
	}
	if fn != nil && w.allOnTestDeadline(compared, fn) {
		return
	}
	w.report(at, "clock loop")
}

// clockComparisons returns, for each clock read in n outside function
// literals, the comparison it takes part in: the nearest enclosing
// relational expression or time comparison method call, or the read itself
// when it is compared with nothing.
func (w *waitWalk) clockComparisons(n ast.Node) []ast.Node {
	var found []ast.Node
	var path []ast.Node
	ast.Inspect(n, func(n ast.Node) bool {
		if n == nil {
			path = path[:len(path)-1]
			return true
		}
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok && clockReads[w.qualified(call.Fun)] {
			comparison := ast.Node(call)
			for i := len(path) - 1; i >= 0; i-- {
				if w.isComparison(path[i]) {
					comparison = path[i]
					break
				}
			}
			found = append(found, comparison)
		}
		path = append(path, n)
		return true
	})
	return found
}

// clockTestedThrough returns the clock reads an if or switch tests through a
// variable its init statement sets, as in
// if left := time.Until(deadline); left <= 0. Only a variable set straight
// from the clock counts: time.Now() passed into some other call in the init
// is data for that call, not the clock being watched.
func (w *waitWalk) clockTestedThrough(init ast.Stmt, tests []ast.Node) []ast.Node {
	assign, ok := init.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) != len(assign.Rhs) {
		return nil
	}
	var found []ast.Node
	for i, value := range assign.Rhs {
		set, ok := assign.Lhs[i].(*ast.Ident)
		if !ok || set.Obj == nil || !w.readsClockStraight(value) {
			continue
		}
		tested := false
		for _, test := range tests {
			ast.Inspect(test, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Obj == set.Obj {
					tested = true
				}
				return !tested
			})
		}
		if tested {
			found = append(found, value)
		}
	}
	return found
}

// readsClockStraight reports whether expr's value comes from a clock read
// through arithmetic and method calls on it (time.Since(start) - d,
// time.Now().Sub(start)) rather than through a call it is an argument of.
func (w *waitWalk) readsClockStraight(expr ast.Expr) bool {
	switch e := ast.Unparen(expr).(type) {
	case *ast.CallExpr:
		if clockReads[w.qualified(e.Fun)] {
			return true
		}
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && w.qualified(sel) == "" {
			return w.readsClockStraight(sel.X)
		}
	case *ast.SelectorExpr:
		return w.readsClockStraight(e.X)
	case *ast.BinaryExpr:
		return w.readsClockStraight(e.X) || w.readsClockStraight(e.Y)
	case *ast.UnaryExpr:
		return w.readsClockStraight(e.X)
	}
	return false
}

func (w *waitWalk) isComparison(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.BinaryExpr:
		switch n.Op {
		case token.LSS, token.GTR, token.LEQ, token.GEQ, token.EQL, token.NEQ:
			return true
		}
	case *ast.CallExpr:
		sel, ok := n.Fun.(*ast.SelectorExpr)
		return ok && timeComparisons[sel.Sel.Name] && w.qualified(sel) == ""
	}
	return false
}

// allOnTestDeadline reports whether every comparison compares the clock only
// with variables of fn holding the test's deadline. A comparison with no such
// variable at all (time.Now() on its own, a constant duration) is not one.
func (w *waitWalk) allOnTestDeadline(comparisons []ast.Node, fn ast.Node) bool {
	for _, comparison := range comparisons {
		operands := w.operands(comparison)
		if len(operands) == 0 {
			return false
		}
		for _, operand := range operands {
			if !w.holdsTestDeadline(operand, fn) {
				return false
			}
		}
	}
	return true
}

// operands returns the identifiers a comparison is made of, leaving out the
// names of packages, of fields and methods, and the predeclared constants.
func (w *waitWalk) operands(comparison ast.Node) []*ast.Ident {
	var operands []*ast.Ident
	var visit func(ast.Node) bool
	visit = func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			ast.Inspect(n.X, visit)
			return false
		case *ast.Ident:
			if n.Obj == nil {
				if _, isPackage := w.imports[n.Name]; isPackage {
					return false
				}
				if n.Name == "true" || n.Name == "false" || n.Name == "nil" {
					return false
				}
			}
			operands = append(operands, n)
		}
		return true
	}
	ast.Inspect(comparison, visit)
	return operands
}

// holdsTestDeadline reports whether id is a variable declared in fn that is
// only ever set, in fn or the closures in it, straight from a
// testwait.Deadline call, and is never handed out by address to be set
// elsewhere. A parameter is set by the caller, and a variable outside fn by
// anyone, so neither does.
func (w *waitWalk) holdsTestDeadline(id *ast.Ident, fn ast.Node) bool {
	object := id.Obj
	if object == nil || object.Kind != ast.Var {
		return false
	}
	decl, ok := object.Decl.(ast.Node)
	if !ok || decl.Pos() < fn.Pos() || decl.End() > fn.End() {
		return false
	}
	sets, held := 0, true
	settle := func(lhs, rhs []ast.Expr, tok token.Token) {
		for i, target := range lhs {
			if target, ok := target.(*ast.Ident); !ok || target.Obj != object {
				continue
			}
			sets++
			if (tok != token.DEFINE && tok != token.ASSIGN) || len(rhs) != len(lhs) || !w.isDeadlineCall(rhs[i]) {
				held = false
			}
		}
	}
	switch decl := object.Decl.(type) {
	case *ast.AssignStmt:
		// A range variable's declaration is an assignment the parser makes
		// up, from the range expression; it is in no statement of fn.
		settle(decl.Lhs, decl.Rhs, decl.Tok)
	case *ast.ValueSpec:
		if len(decl.Values) > 0 {
			names := make([]ast.Expr, len(decl.Names))
			for i, name := range decl.Names {
				names[i] = name
			}
			settle(names, decl.Values, token.DEFINE)
		}
	default:
		return false
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if n != object.Decl {
				settle(n.Lhs, n.Rhs, n.Tok)
			}
		case *ast.UnaryExpr:
			if x, ok := ast.Unparen(n.X).(*ast.Ident); ok && n.Op == token.AND && x.Obj == object {
				held = false
			}
		}
		return true
	})
	return held && sets > 0
}

func (w *waitWalk) isDeadlineCall(expr ast.Expr) bool {
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	return ok && w.qualified(call.Fun) == testwaitPath+".Deadline"
}

// No test in this module waits a fixed time for something that should
// happen: on a loaded host the time runs out first and the test fails for
// nothing. Such a wait is on the condition (Until, Expired, Context). A fixed
// wait that is meant - nothing must happen within it, or a slow peer is what
// the test simulates - says why on its own line. The test helpers under
// internal/ are held to the same rule, since every test that calls them
// waits as they do.
func TestNoTestWaitsAFixedTimeWithoutSayingWhy(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s is not the module root: %v", root, err)
	}
	fset := token.NewFileSet()
	var found []string
	scanned := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		helper := strings.HasPrefix(relative, "internal/") && strings.HasSuffix(relative, ".go")
		if !strings.HasSuffix(relative, "_test.go") && !helper {
			return nil
		}
		scanned++
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			return err
		}
		lines := strings.Split(string(src), "\n")
		for _, wait := range fixedWaits(fset, file, relative) {
			found = append(found, wait.String()+": "+strings.TrimSpace(lines[wait.line-1]))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("no test files scanned; the walk is not reaching the module")
	}
	if len(found) > 0 {
		t.Fatalf("%d fixed waits without a %q reason on their own line - wait on the condition "+
			"(testwait.Until, Expired, Context, a loop on testwait.Deadline) or say why the duration is enough:\n%s",
			len(found), "// "+justification, strings.Join(found, "\n"))
	}
}
