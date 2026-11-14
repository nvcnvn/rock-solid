package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const module = "github.com/nvcnvn/rock-solid/"

var loopPackages = []string{
	"internal/loop",
	"internal/driver",
}

var dataPathPackages = []string{
	"internal/frontend",
}

var uncheckedPackages = []string{
	"internal/csi",
}

const noLogs = "record a trace event instead of a free-form log"

var bannedImports = map[string]string{
	"os":          "emit an Action for I/O",
	"os/exec":     "emit an Action for I/O",
	"io/fs":       "emit an Action for I/O",
	"net":         "emit an Action for I/O",
	"net/http":    "emit an Action for I/O",
	"syscall":     "emit an Action for I/O",
	"unsafe":      "keep state in plain Go values",
	"sync":        "a machine runs on one loop and needs no synchronization",
	"sync/atomic": "a machine runs on one loop and needs no synchronization",
	"log":         noLogs,
	"log/slog":    noLogs,
	"crypto/rand": "use a seeded core.Rand",
	"math/rand":   "use a seeded core.Rand",
	"context":     "cancellation and deadlines arrive as events",
	"runtime":     "the loop owns scheduling; emit an Action",
}

const sysPrefix = "golang.org/x/sys"

var bannedTimeFuncs = map[string]string{
	"Now":       "use ctx.Now",
	"Since":     "use ctx.Now",
	"Until":     "use ctx.Now",
	"Sleep":     "emit an Action with a TimerID",
	"After":     "emit an Action with a TimerID",
	"AfterFunc": "emit an Action with a TimerID",
	"NewTimer":  "emit an Action with a TimerID",
	"NewTicker": "emit an Action with a TimerID",
	"Tick":      "emit an Action with a TimerID",
}

var bannedLoopTimeFuncs = map[string]string{
	"Now":       "read the clocks through the kernel seam",
	"Since":     "read the clocks through the kernel seam",
	"Until":     "read the clocks through the kernel seam",
	"Sleep":     "block only in the kernel seam's Wait",
	"After":     "arm a timer in the loop's heap",
	"AfterFunc": "arm a timer in the loop's heap",
	"NewTimer":  "arm a timer in the loop's heap",
	"NewTicker": "arm a timer in the loop's heap",
	"Tick":      "arm a timer in the loop's heap",
}

var clockSyscalls = map[string]bool{
	"ClockGettime": true,
	"Gettimeofday": true,
	"Time":         true,
}

// realKernelFile is the one loop file that may read a clock: the real kernel behind the seam.
const realKernelFile = "epoll_linux.go"

var allowedRandFuncs = map[string]bool{
	"New":        true,
	"NewChaCha8": true,
	"NewPCG":     true,
	"NewZipf":    true,
}

var logFuncs = map[string]bool{
	"Print":    true,
	"Println":  true,
	"Printf":   true,
	"Fprint":   true,
	"Fprintln": true,
	"Fprintf":  true,
}

var loggingImports = map[string]bool{
	"log":      true,
	"log/slog": true,
}

// Analyzer rejects clocks, I/O, concurrency, global randomness and map iteration in sans-IO packages, clocks, Go timers, channels and map iteration in the loop and driver packages, and logs on the data path.
var Analyzer = &analysis.Analyzer{
	Name: "sansio",
	Doc:  "check sans-IO packages for nondeterminism, the loop and driver packages for waits outside the kernel seam, and the data path for free-form logs",
	Run:  run,
}

type packageKind int

const (
	unchecked packageKind = iota
	sansIO
	loopRules
	dataPath
)

func classify(path string) packageKind {
	if i := strings.Index(path, " ["); i >= 0 {
		path = path[:i]
	}
	if strings.HasSuffix(path, ".test") {
		return unchecked
	}
	rel, ok := strings.CutPrefix(path, module+"internal/")
	if !ok {
		return unchecked
	}
	rel = "internal/" + rel
	for _, p := range uncheckedPackages {
		if rel == p || strings.HasPrefix(rel, p+"/") {
			return unchecked
		}
	}
	for _, p := range loopPackages {
		if rel == p || strings.HasPrefix(rel, p+"/") {
			return loopRules
		}
	}
	for _, p := range dataPathPackages {
		if rel == p || strings.HasPrefix(rel, p+"/") {
			return dataPath
		}
	}
	return sansIO
}

func run(pass *analysis.Pass) (any, error) {
	kind := classify(pass.Pkg.Path())
	if kind == unchecked {
		return nil, nil
	}
	for _, file := range pass.Files {
		if strings.HasSuffix(pass.Fset.Position(file.Pos()).Filename, "_test.go") {
			continue
		}
		checkImports(pass, file, kind)
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				checkUse(pass, id, kind)
			}
			switch kind {
			case sansIO:
				checkSansIO(pass, n)
			case loopRules:
				checkLoop(pass, n)
			}
			return true
		})
	}
	return nil, nil
}

func checkSansIO(pass *analysis.Pass, n ast.Node) {
	switch n := n.(type) {
	case *ast.GoStmt:
		reportSansIO(pass, n, "uses a go statement", "emit an Action and let the loop schedule the work")
	case *ast.SelectStmt:
		reportSansIO(pass, n, "uses a select statement", "wait for an Event")
	case *ast.ChanType:
		reportSansIO(pass, n, "uses a channel", "exchange Events and Actions with the loop")
	case *ast.SendStmt:
		reportSansIO(pass, n, "sends on a channel", "emit an Action")
	case *ast.UnaryExpr:
		if n.Op == token.ARROW {
			reportSansIO(pass, n, "receives from a channel", "wait for an Event")
		}
	case *ast.RangeStmt:
		if rangesOverMap(pass, n) {
			reportSansIO(pass, n, "ranges over a map", "iteration order is random; iterate sorted keys")
		}
	}
}

const loopWaits = "a loop waits only in the kernel seam's Wait; hand work over through an inbox"

func checkLoop(pass *analysis.Pass, n ast.Node) {
	switch n := n.(type) {
	case *ast.SelectStmt:
		reportLoop(pass, n, "uses a select statement", loopWaits)
	case *ast.ChanType:
		reportLoop(pass, n, "uses a channel", loopWaits)
	case *ast.SendStmt:
		reportLoop(pass, n, "sends on a channel", loopWaits)
	case *ast.UnaryExpr:
		if n.Op == token.ARROW {
			reportLoop(pass, n, "receives from a channel", loopWaits)
		}
	case *ast.RangeStmt:
		if rangesOverMap(pass, n) {
			reportLoop(pass, n, "ranges over a map", "iteration order is random; iterate sorted keys")
		}
	}
}

func rangesOverMap(pass *analysis.Pass, n *ast.RangeStmt) bool {
	t := pass.TypesInfo.TypeOf(n.X)
	if t == nil {
		return false
	}
	_, ok := t.Underlying().(*types.Map)
	return ok
}

func checkImports(pass *analysis.Pass, file *ast.File, kind packageKind) {
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		switch kind {
		case sansIO:
			if path == sysPrefix || strings.HasPrefix(path, sysPrefix+"/") {
				pass.Reportf(spec.Pos(), "sans-IO package imports %q: emit an Action for I/O", path)
			} else if why, ok := bannedImports[path]; ok {
				pass.Reportf(spec.Pos(), "sans-IO package imports %q: %s", path, why)
			}
		case loopRules, dataPath:
			if loggingImports[path] {
				pass.Reportf(spec.Pos(), "data path package imports %q: the data path emits no free-form logs; record a trace event instead", path)
			}
		}
	}
}

func checkUse(pass *analysis.Pass, id *ast.Ident, kind packageKind) {
	if tn, ok := pass.TypesInfo.Uses[id].(*types.TypeName); ok && kind == loopRules && tn.Pkg() != nil && tn.Pkg().Path() == "time" && (tn.Name() == "Timer" || tn.Name() == "Ticker") {
		reportLoop(pass, id, "uses time."+tn.Name(), "arm a timer in the loop's heap")
		return
	}
	fn, ok := pass.TypesInfo.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Signature().Recv() != nil {
		return
	}
	name := fn.Pkg().Name() + "." + fn.Name()
	switch fn.Pkg().Path() {
	case "fmt":
		if !logFuncs[fn.Name()] {
			return
		}
		if kind != sansIO {
			pass.Reportf(id.Pos(), "data path package calls %s: the data path emits no free-form logs; record a trace event instead", name)
		} else {
			reportSansIO(pass, id, "calls "+name, noLogs)
		}
	case "time":
		if why, ok := bannedTimeFuncs[fn.Name()]; ok && kind == sansIO {
			reportSansIO(pass, id, "calls "+name, why)
		}
		if why, ok := bannedLoopTimeFuncs[fn.Name()]; ok && kind == loopRules {
			reportLoop(pass, id, "calls "+name, why)
		}
	case "golang.org/x/sys/unix", "syscall":
		if kind == loopRules && clockSyscalls[fn.Name()] && filepath.Base(pass.Fset.Position(id.Pos()).Filename) != realKernelFile {
			reportLoop(pass, id, "calls "+name, "read the clocks through the kernel seam")
		}
	case "math/rand/v2":
		if !allowedRandFuncs[fn.Name()] && kind == sansIO {
			reportSansIO(pass, id, "calls "+name, "it draws from the global source; use a seeded core.Rand")
		}
	}
}

func reportLoop(pass *analysis.Pass, n ast.Node, what, instead string) {
	pass.Reportf(n.Pos(), "loop package %s: %s", what, instead)
}

func reportSansIO(pass *analysis.Pass, n ast.Node, what, instead string) {
	pass.Reportf(n.Pos(), "sans-IO package %s: %s", what, instead)
}
