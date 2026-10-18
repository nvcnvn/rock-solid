// Command sansio is a go vet tool that checks sans-IO packages for nondeterminism and the
// data path for free-form logs.
package main

import "golang.org/x/tools/go/analysis/unitchecker"

func main() { unitchecker.Main(Analyzer) }
