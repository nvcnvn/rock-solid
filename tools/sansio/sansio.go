package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const module = "github.com/nvcnvn/rock-solid/"

var dataPathPackages = []string{
	"internal/loop",
	"internal/driver",
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

// Analyzer rejects clocks, I/O, goroutines, channels, global randomness and map iteration
// in sans-IO packages, and free-form logs on the data path.
var Analyzer = &analysis.Analyzer{
	Name: "sansio",
	Doc:  "check sans-IO packages for nondeterminism and the data path for free-form logs",
	Run:  run,
}

type packageKind int

const (
	unchecked packageKind = iota
	sansIO
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
			if kind != sansIO {
				return true
			}
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
				if t := pass.TypesInfo.TypeOf(n.X); t != nil {
					if _, ok := t.Underlying().(*types.Map); ok {
						reportSansIO(pass, n, "ranges over a map", "iteration order is random; iterate sorted keys")
					}
				}
			}
			return true
		})
	}
	return nil, nil
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
		case dataPath:
			if loggingImports[path] {
				pass.Reportf(spec.Pos(), "data path package imports %q: the data path emits no free-form logs; record a trace event instead", path)
			}
		}
	}
}

func checkUse(pass *analysis.Pass, id *ast.Ident, kind packageKind) {
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
		if kind == dataPath {
			pass.Reportf(id.Pos(), "data path package calls %s: the data path emits no free-form logs; record a trace event instead", name)
		} else {
			reportSansIO(pass, id, "calls "+name, noLogs)
		}
	case "time":
		if why, ok := bannedTimeFuncs[fn.Name()]; ok && kind == sansIO {
			reportSansIO(pass, id, "calls "+name, why)
		}
	case "math/rand/v2":
		if !allowedRandFuncs[fn.Name()] && kind == sansIO {
			reportSansIO(pass, id, "calls "+name, "it draws from the global source; use a seeded core.Rand")
		}
	}
}

func reportSansIO(pass *analysis.Pass, n ast.Node, what, instead string) {
	pass.Reportf(n.Pos(), "sans-IO package %s: %s", what, instead)
}
