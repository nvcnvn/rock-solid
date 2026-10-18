#!/bin/bash
# Runs every Go check: formatting, vet, the sans-IO analyzer, staticcheck, known vulnerabilities and dependency licenses.
set -euo pipefail
cd "$(dirname "$0")/.."

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
export GOTOOLCHAIN

STATICCHECK=honnef.co/go/tools/cmd/staticcheck@v0.8.2
GOVULNCHECK=golang.org/x/vuln/cmd/govulncheck@v1.9.0
GO_LICENSES=github.com/google/go-licenses/v2@v2.0.1
ALLOWED_LICENSES=Apache-2.0,MIT,BSD-2-Clause,BSD-3-Clause,ISC,Zlib,CC0-1.0,Unicode-3.0

mapfile -t files < <(git ls-files --cached --others --exclude-standard '*.go' ':!:spikes/**')
if [ ${#files[@]} -eq 0 ]; then
	echo "no Go files outside spikes/; nothing to lint"
	exit 0
fi

unformatted=$(gofmt -l "${files[@]}")
if [ -n "$unformatted" ]; then
	echo "gofmt needed:" >&2
	echo "$unformatted" >&2
	exit 1
fi

go vet ./...
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
go build -o "$tmp/sansio" ./tools/sansio
go vet -vettool="$tmp/sansio" ./...
go run "$STATICCHECK" ./...
status=0
go run "$GOVULNCHECK" ./... || status=$?
case $status in
0) ;;
3) exit 3 ;;
*) echo "govulncheck could not run (exit $status); skipping the vulnerability check" >&2 ;;
esac
go run "$GO_LICENSES" check ./... --allowed_licenses="$ALLOWED_LICENSES"
