#!/bin/bash
# Gates a branch or commit in a fresh worktree at its tip: the TLC ci runs of the models it touches,
# go test and seeds.sh over the packages it changes and every package that imports them (everything
# with --full or --merge), the same go test of the packages with Linux-only tests in Docker (skipped
# with RS_GATE_NO_LINUX=1), conform.sh when it touches a path the conform workflow watches, and lint.sh.
#   scripts/gate-branch.sh [--plan] [--full] [--merge] <branch-or-commit>
# The change is the diff from the tip's merge base with main, or from its parent for a commit already
# on main. --plan prints the steps and runs none. --merge fast-forwards the clean main checkout to the
# tip once every step passes, then lints main. Each step's log and summary.md land in
# $cache/gate/<branch>-<sha>/; a model whose tree a gate passed, recorded in $cache/gate-pass/, is
# skipped. Every step holds one machine-wide lock, a model step once per configuration; wrap any other
# heavy command the same way:
#   lockf -k "${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/heavy.lock" <command>
set -euo pipefail

usage="usage: $0 [--plan] [--full] [--merge] <branch-or-commit>"
plan= full= merge=
while [[ ${1:-} == --* ]]; do
	case $1 in
	--plan) plan=1 ;;
	--full) full=1 ;;
	--merge) merge=1 full=1 ;;
	*) echo "$usage" >&2; exit 2 ;;
	esac
	shift
done
ref=${1:?$usage}
repo=$(cd "$(dirname "$0")/.." && pwd)
cache=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid
lock=$cache/heavy.lock
tip=$(git -C "$repo" rev-parse --verify "$ref^{commit}")
short=$(git -C "$repo" rev-parse --short "$tip")
base=$(git -C "$repo" merge-base main "$tip")
[ "$base" != "$tip" ] || base=$(git -C "$repo" rev-parse "$tip^")
out=$cache/gate/${ref//\//-}-$short
wt=$cache/wt/$short-$$
mkdir -p "$cache/gate-pass" "$cache/wt" "$cache/linux-gocache"
git -C "$repo" worktree add -q --detach "$wt" "$tip"
trap 'git -C "$repo" worktree remove --force "$wt"' EXIT
cd "$wt"
GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
export GOTOOLCHAIN GOMEMLIMIT=4GiB WORKERS=4

changed=$(git diff --name-only "$base" "$tip")
grep -qxE 'go\.(mod|sum)' <<<"$changed" && full=1
eval "$(grep -E '^(protocol|exclude)=' scripts/model-gate.sh)"

# The rule map names a model's plants by package; internal/node and the raft fork have none yet.
models=$({
	sed -n 's|^models/\([^/]*\)/.*|\1|p' <<<"$changed"
	for f in $(grep -E "$protocol" <<<"$changed" | grep -Ev "$exclude" || true); do
		case $f in
		(internal/node/*) echo sessions ;;
		(third_party/raft/*) echo voters ;;
		(*) awk -F'\t' -v p="$(basename "$(dirname "$f")")." 'index($2, p) == 1 { sub("/.*", "", $1); print $1 }' scripts/model-rules.tsv ;;
		esac
	done
} | sort -u)

mod=$(go list -m)
pkgdirs=$(go list -f '{{.Dir}}' ./... | sed "s|^$PWD/||")
direct=$(for f in $changed; do
	d=$(dirname "$f")
	d=${d%%/testdata*}
	grep -qxF "$d" <<<"$pkgdirs" && echo "$mod/$d"
done | sort -u || true)
# affected [VAR=value...]: the changed packages and every package whose tests import one, built with
# the environment given.
affected() {
	echo "$direct"
	env "$@" go list -test -f '{{.ImportPath}} {{join .Deps " "}}' ./... | awk -v want="$(echo $direct)" '
		BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) W[w[i]] = 1 }
		$1 ~ /\.test$/ { for (i = 2; i <= NF; i++) if ($i in W) { print substr($1, 1, length($1) - 5); next } }'
}
tests='{{.ImportPath}} {{.GoFiles}} {{.TestGoFiles}} {{.XTestGoFiles}}'
linuxonly=$(comm -13 <(go list -f "$tests" ./... | sort) <(GOOS=linux go list -f "$tests" ./... | sort) | cut -d' ' -f1)
pkgs= linux=
if [ -n "$full" ]; then
	linux=$linuxonly
elif [ -n "$direct" ]; then
	pkgs=$(affected | sort -u)
	linux=$(affected GOOS=linux | sort -u | grep -xF "$linuxonly" || true)
fi
seeded=$(awk -v mod="$mod/" -v want="$(echo $pkgs)" '
	BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) W[w[i]] = 1 }
	/^internal\/[^ ]+ Test[^ ]+ [0-9]+$/ && (mod $1) in W { print $2 }' scripts/seeds.sh | paste -s -d ' ' -)
conform=
while read -r g; do
	while read -r f; do
		[[ -n $f && $f == $g ]] && conform=1
	done <<<"$changed"
done < <(sed -n '/^    paths:/,/^  [a-z]/s/^      - //p' .github/workflows/conform.yml)

steps=()
skips=()
for m in $models; do
	[ -d "models/$m" ] || continue
	pass=$cache/gate-pass/$m-$(git rev-parse "$tip:models/$m")
	if [ -f "$pass" ]; then
		skips+=("models-$m|$(head -1 "$pass")")
	else
		steps+=("models-$m|cs=\$(cd models/$m && ls ci*.cfg neg-*.cfg 2>/dev/null); [ -n \"\$cs\" ] || { echo 'FAIL $m: no configuration'; exit 1; }; f=0; for c in \$cs; do CI_LIMIT=${CI_LIMIT:-600} lockf -k $lock scripts/models.sh ci $m/\$c || f=1; done; [ \$f = 0 ] && echo 'passed in the gate of $ref at $short, $(date +%F)' >$pass")
	fi
done
if [ -n "$full" ]; then
	steps+=("go-test|go test -p 1 -count=1 -timeout 30m ./..." "seeds|scripts/seeds.sh")
elif [ -n "$pkgs" ]; then
	steps+=("go-test|go test -p 1 -count=1 -timeout 30m $(paste -s -d ' ' - <<<"$pkgs")")
	[ -z "$seeded" ] || steps+=("seeds|scripts/seeds.sh $seeded")
fi
if [ -n "$linux" ] && [ "${RS_GATE_NO_LINUX:-}" = 1 ]; then
	skips+=("linux-test|by operator (RS_GATE_NO_LINUX=1)")
elif [ -n "$linux" ]; then
	steps+=("linux-test|docker info >/dev/null 2>&1 || { echo 'Error: the Docker daemon is not reachable; start it, or set RS_GATE_NO_LINUX=1 to skip this step'; exit 1; }; \
docker run --rm --name rs-gate-$short-$$ --cpus 4 --memory 6g --security-opt seccomp=unconfined \
-v \"\$PWD\":/src:ro -v $(go env GOMODCACHE):/go/pkg/mod:ro -w /src -e GOTOOLCHAIN=local -e GOMEMLIMIT \
-v $cache/linux-gocache:/gocache -e GOFLAGS='-mod=mod -buildvcs=false' -e GOCACHE=/gocache golang:${GOTOOLCHAIN#go} \
go test -p 1 -count=1 -timeout 30m $(paste -s -d ' ' - <<<"$linux")")
fi
[ -z "$conform" ] || steps+=("conform|scripts/conform.sh")
steps+=("lint|MODEL_GATE_BASE=$base scripts/lint.sh")

if [ -n "$plan" ]; then
	echo "gate $ref at $short, change since ${base:0:7}"
	for s in ${skips[@]+"${skips[@]}"}; do echo "skip ${s%%|*}: ${s#*|}"; done
	for s in "${steps[@]}"; do echo "run  ${s%%|*}: ${s#*|}"; done
	exit 0
fi

mkdir -p "$out"
summary=$out/summary.md
{
	echo "# Gate of $ref at $short (change since ${base:0:7}${full:+, full})"
	echo
	echo "| step | verdict | lock wait | run | log |"
	echo "|---|---|---|---|---|"
	for s in ${skips[@]+"${skips[@]}"}; do echo "| ${s%%|*} | skipped: ${s#*|} | | | |"; done
} >"$summary"
failed=0
failures=
for s in "${steps[@]}"; do
	name=${s%%|*}
	log=$out/$name.log
	start=$(date +%s)
	echo "$(date +%T) $name"
	verdict=pass
	# The lock is shared with other sessions, so a model step takes it once per configuration.
	held=(lockf -k "$lock")
	[[ $name != models-* ]] || held=()
	if ! ${held[@]+"${held[@]}"} bash -c 'date +%s >"$0"; eval "$1"' "$out/$name.locked" "${s#*|}" >"$log" 2>&1; then
		verdict=FAIL
		failed=1
		failures+=$'\n'"## $name"$'\n\n```\n'"$(grep -E '^(--- FAIL|FAIL|panic:|(docker: )?Error)|(is|was) violated' "$log" | head -40)"$'\n```\n'
	fi
	end=$(date +%s)
	locked=$(cat "$out/$name.locked" 2>/dev/null || echo "$end")
	echo "| $name | $verdict | $((locked - start))s | $((end - locked))s | $log |" >>"$summary"
done
echo "$failures" >>"$summary"

if [ -n "$merge" ]; then
	main=$(git -C "$repo" worktree list --porcelain | awk '/^worktree /{w = $2} $0 == "branch refs/heads/main" {print w}')
	if [ $failed -ne 0 ]; then
		echo "merge: refused, a step failed" >>"$summary"
	elif ! git merge-base --is-ancestor main "$tip"; then
		echo "merge: refused, main is not an ancestor of $short" >>"$summary"
		failed=1
	elif [ -n "$(git -C "$main" status --porcelain)" ]; then
		echo "merge: refused, $main is not clean" >>"$summary"
		failed=1
	elif git -C "$main" merge -q --ff-only "$tip" && (cd "$main" && lockf -k "$lock" scripts/lint.sh) >"$out/main-lint.log" 2>&1; then
		echo "merge: main fast-forwarded to $short and lints, $out/main-lint.log" >>"$summary"
	else
		echo "merge: FAIL, see $out/main-lint.log" >>"$summary"
		failed=1
	fi
fi
cat "$summary"
echo "$summary"
exit $failed
