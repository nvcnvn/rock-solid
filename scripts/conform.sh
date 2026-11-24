#!/bin/bash
# Spec conformance: runs the simulator's conformance traces and has TLC 1.7.4 check each is a behavior
# of its model ("Spec conformance" in docs/test-scenarios.md): a trace named voters-* of models/voters/voters.tla,
# any other of models/epoch/epoch.tla. Not part of lint; the conform workflow runs it nightly and on a pull
# request that changes the epoch or voter protocol.
#   scripts/conform.sh                  every trace of TestConform*, or of the test RUN names, seeded
#                                       ones over COUNT seeds from FIRST (default 0): by default 10 of
#                                       the epoch model's, 50 of the voter model's
#   scripts/conform.sh file.trace ...   those traces alone
#   scripts/conform.sh plants           traces can fail: on each of COUNT seeds (default 10) from FIRST
#                                       (default 0) whose unplanted trace passes, each plant of PLANTS
#                                       (default all of conformPlants), one row each; a run stops at its
#                                       first broken invariant, or goes on with CONFORM_RUNON=1, failing
#                                       when a seed's baseline fails or a plant the check catches fires
#                                       on some seed and is rejected on none, or over seeds 0 to 39 fires
#                                       on none; then each voter plant's
#                                       trace and its unplanted baseline, failing when a baseline fails,
#                                       a trace gives no data or verdict, or a plant whose rule the check
#                                       catches passes
# Each trace is one light TLC process with its own directory: one worker, HEAP (default 2g), TLC_SECONDS
# (default 400). JOBS (default 1) of them run at once, and the lines print in order when all are done.
# KEEP=dir keeps the traces and data, and skips a check whose spec and data have not changed.
# A run ends with what its passing traces covered: "cover" rows of each action taken and the actions no
# trace took, and each trace's rows stay in its directory's cover file.
set -euo pipefail
cd "$(dirname "$0")/.."

version=1.7.4
jar=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/tla2tools-$version.jar
[ -f "$jar" ] || scripts/models.sh fetch

tmp=${KEEP:-$(mktemp -d)}
mkdir -p "$tmp/traces"
[ -n "${KEEP:-}" ] || trap 'rm -rf "$tmp"' EXIT
tmp=$(cd "$tmp" && pwd)

# model prints the model trace $1 is checked against.
model() { case $(basename "$1") in voters-*) echo voters ;; *) echo epoch ;; esac; }

# steps prints a data module's trace lines.
steps() { sed -n '/^Trace == <</,$p' "$1" | sed '1d;$d' | sed '$d'; }

# data writes trace $1's data module into its check directory.
data() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	tool=tracedata
	[ "$(model "$1")" = epoch ] || tool=votersdata
	if ! "$tmp/$tool" "$1" >"$dir/data.new"; then
		rm -f "$dir/data.new" "$dir/verdict"
		return 1
	fi
}

# check has TLC take the data module of trace $1, saving its verdict as $dir/verdict: "ok ...", the safety invariant a line broke, or the line no state could take. A check with no verdict is not kept, since a longer limit may give one.
check() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	m=$(model "$1")
	set -- "models/$m/$m.tla" "models/$m/trace_$m.tla" "models/$m/trace_$m.cfg"
	key=$(cat "$@" "$dir/data.new" | cksum)
	if [ -f "$dir/verdict" ] && [ "$(cat "$dir/key" 2>/dev/null)" = "$key" ]; then
		grep -q '^ok' "$dir/verdict"
		return
	fi
	rm -f "$dir/verdict"
	mv "$dir/data.new" "$dir/trace_data.tla"
	cp "$@" "$dir"
	total=$(steps "$dir/trace_data.tla" | wc -l | tr -d ' ')
	start=$SECONDS
	# TLC unpacks its standard modules into java.io.tmpdir and deletes them on exit, so a shared one fails a parse beside it.
	mkdir -p "$dir/tmp"
	(cd "$dir" && perl -e 'alarm shift; exec @ARGV' "${TLC_SECONDS:-400}" java -Xmx"${HEAP:-2g}" -Djava.io.tmpdir="$dir/tmp" -XX:+UseSerialGC -cp "$jar" tlc2.TLC -workers 1 -config "trace_$m.cfg" "trace_$m.tla") >"$dir/out" 2>&1 || true
	rm -rf "$dir/states" "$dir/tmp"
	secs=$((SECONDS - start))
	broken=$(sed -n 's/^Error: Invariant \([A-Za-z]*\) is violated.*/\1/p' "$dir/out" | head -1)
	depth=$(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' "$dir/out")
	if [ "$broken" = TraceIncomplete ]; then
		echo "ok $total lines, ${secs}s" >"$dir/verdict"
	elif [ -n "$broken" ]; then
		at=$(($(sed -n 's/^\/\\ line = \([0-9]*\)$/\1/p' "$dir/out" | tail -1) - 1))
		{
			echo "invariant $broken broken by line $at of $total, ${secs}s"
			steps "$dir/trace_data.tla" | sed -n "${at}p"
		} >"$dir/verdict"
	elif [ -n "$depth" ]; then
		{
			echo "no step for line $depth of $total, ${secs}s"
			steps "$dir/trace_data.tla" | sed -n "${depth}p"
		} >"$dir/verdict"
	else
		{
			echo "no verdict after ${secs}s"
			tail -20 "$dir/out"
		} >"$dir/verdict"
		rm -f "$dir/key"
		return 1
	fi
	echo "$key" >"$dir/key"
	grep -q '^ok' "$dir/verdict"
}

# cover leaves in $dir/cover a row for each spec change a passing trace's lines name: action, line kind, lines that took it, lines that stuttered. The behavior TLC prints for a pass is the one it found: a line stutters when no variable of the spec moves from the state before it.
cover() {
	dir=$tmp/check/$(basename "$1" .trace)
	m=$(model "$1")
	{
		[ "$m" = voters ] || sed -n '/^TPlan ==/,/^TWrites ==/p' "$dir/trace_data.tla" | sed -n '/^TWrites/d;s/^[^"]*"\([a-z]*\)".*/P \1/p'
		steps "$dir/trace_data.tla" | sed 's/^[^"]*"\([a-z]*\)".*/L \1/'
		cat "$dir/out"
	} | awk -v model="$m" '
		function step() {
			if (n > 0) {
				k = kind[n]
				if (k == "plan") k = k "." plan[++p]
				if (cur == prev) stutter[k]++; else took[k]++
				seen[k] = 1
			}
			prev = cur; cur = ""; n++
		}
		BEGIN {
			split("fold Fold renew Renew adopt Adopt send Send order Order logged Deliver ownlogged OwnLogged committed Answer maydeliver Deliver mfail MemberFail " \
				"gone GoneRefuses gonerecover GoneRefusesRecovery mayfail OwnFail restart Restart format Format recovered Recover recoveredlate Recover mayrecover Recover " \
				"reported ReportRecovered ask AskNext restartask RestartAsk peer Peer activate Activate plan.mark MarkDisk " \
				"plan.replace Replace plan.rotate Rotate plan.add Add plan.remove Remove plan.stay Stay plan.drop Drop plan.refill Refill " \
				"plan.minlive SetMinLive plan.edit Edit lost Lost", m)
			if (model == "voters")
				split("elect Elect transfer Transfer addlearner AddLearner swap Swap leave Leave replicate Replicate commit Commit " \
					"apply Apply removed Apply notice Notice delete Delete crash Crash restart Restart halt Halt die Die " \
					"fortify Fortify lapse Lapse askleader AskLeader handofffails HandOffFails", m)
			for (i = 1; i in m; i += 2) action[m[i]] = m[i + 1]
		}
		!on && $1 == "P" && NF == 2 { plan[++np] = $2; next }
		!on && $1 == "L" && NF == 2 { kind[++nl] = $2; next }
		/^State [0-9]+:/ { if (on) step(); on = 1; next }
		/^[0-9]+ states generated/ { if (on) step(); on = 0; next }
		!on { next }
		/^\/\\ (line|healed) / { skip = 1; next }
		/^\/\\ / { skip = 0 }
		!skip { cur = cur $0 "\n" }
		END { for (k in seen) print (k in action ? action[k] : "-"), k, took[k] + 0, stutter[k] + 0 }
	' | sort >"$dir/cover.new"
	mv "$dir/cover.new" "$dir/cover"
}

# coverage prints, summed over the passing traces of the arguments, each action taken with its real steps, and the actions of Next no trace took.
coverage() {
	for f in "$@"; do
		c=$tmp/check/$(basename "$f" .trace)/cover
		[ ! -f "$c" ] || sed "s/^/$(model "$f") /" "$c"
	done | awk '
		{ m[$1] = 1; $0 = substr($0, length($1) + 2) }
		{ took[$1 " " $2] += $3; stutter[$1 " " $2] += $4; if ($3) traces[$1 " " $2]++; if ($3) done[$1] = 1 }
		END {
			for (k in took) print "cover", k, took[k], "real,", stutter[k], "stutter,", traces[k] + 0, "traces"
			if ("voters" in m) {
				n = split("Replicate Fortify Commit Leave Apply Restart AskLeader AddLearner Swap Delete DeleteOnPlan Crash Halt Die " \
					"Notice Elect Transfer HandOffFails Lapse", a)
				never = ""
				for (i = 1; i <= n; i++) if (!(a[i] in done)) never = never " " a[i]
				print "voters never taken:" never
			}
			if (!("epoch" in m)) exit
			never = ""
			n = split("Fold Renew Adopt Peer Activate CounterOut AskNext RestartAsk ReportRecovered Persist Recover Send Order Refuse Deliver OwnLogged Answer " \
				"Fortify BootUp MarkDisk Replace Rotate Add Remove Drop Stay Refill Lost GiveUp SetMinLive Split Edit Die Isolate MemberFail GoneRefuses GoneRefusesRecovery OwnFail " \
				"OpenGrant OpenLapse Suspend Wake Freeze Thaw RateStep Restart Reboot Format NetFault Elect Transfer StepDown " \
				"Ask Grant GrantRefused SendHb RecvGrant RecvHb RoundTrip Beat EndWait Tick", a)
			for (i = 1; i <= n; i++) if (!(a[i] in done)) never = never " " a[i]
			print "never taken:" never
		}
	' | sort
}

# plain leaves trace $1's result line in its directory.
plain() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	if ! data "$1"; then
		echo "FAIL $name: no data"
	elif check "$1"; then
		cover "$1"
		echo "ok   $name: $(sed 's/^ok //' "$dir/verdict")"
	else
		echo "FAIL $name: $(cat "$dir/verdict")"
	fi >"$dir/line.new"
	mv "$dir/line.new" "$dir/line"
}

# planted leaves plant trace $1's table row in its directory, against its seed's baseline.
planted() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	base=$tmp/check/primaries-${name##*-}/base.lines
	stop=$(cut -c1-80 "${1%.trace}.stop" | head -1)
	(
		if ! data "$1"; then
			echo "$name: NO DATA, stop: $stop"
			exit 0
		fi
		steps "$dir/data.new" >"$dir/plant.lines"
		same=$(cmp "$base" "$dir/plant.lines" 2>&1 || true)
		case $same in
		*"EOF on "*base.lines*) first=$(($(wc -l <"$base") + 1)) ;;
		*" line "*) first=$(echo "$same" | sed 's/.* line \([0-9]*\).*/\1/') ;;
		*)
			echo "$name: not fired, $(wc -l <"$dir/plant.lines" | tr -d ' ') lines are a prefix of the baseline's, stop: $stop"
			exit 0
			;;
		esac
		if check "$1"; then
			echo "$name: PASSED, fired at line $first, $(cat "$dir/verdict"), stop: $stop"
		elif grep -q '^no verdict' "$dir/verdict"; then
			echo "$name: NO VERDICT, fired at line $first, $(head -1 "$dir/verdict"), stop: $stop"
		else
			echo "$name: rejected, fired at line $first, stop: $stop: $(cut -c1-200 "$dir/verdict" | tr '\n' ' ')"
		fi
	) >"$dir/line.new"
	mv "$dir/line.new" "$dir/line"
}

# each runs function $1 on each later argument, JOBS at a time, each in its own process.
each() {
	fn=$1
	shift
	[ $# -gt 0 ] || return 0
	export -f steps data check cover plain planted
	export -f model
	export tmp jar
	printf '%s\0' "$@" | xargs -0 -n 1 -P "${JOBS:-1}" "$BASH" -c 'set -euo pipefail; "$@"' each "$fn"
}

if [ "${1:-}" = plants ]; then
	rm -f "$tmp"/traces/*
	CONFORM_DIR=$tmp/traces CONFORM_PLANTS=${PLANTS:-} GOMEMLIMIT=4GiB go test -p 4 ./internal/chunkstore -run '^TestPlantsKeepTraces$' \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-10}"
	go build -o "$tmp/tracedata" ./tools/tracedata
	seeds=$(seq "${FIRST:-0}" $((${FIRST:-0} + ${COUNT:-10} - 1)))
	bases=()
	for seed in $seeds; do
		bases+=("$tmp/traces/primaries-$seed.trace")
	done
	each plain "${bases[@]}"
	rows=()
	for seed in $seeds; do
		dir=$tmp/check/primaries-$seed
		grep -q '^ok' "$dir/verdict" || continue
		steps "$dir/trace_data.tla" >"$dir/base.lines"
		for f in "$tmp"/traces/*-"$seed".trace; do
			[ "$(basename "$f")" = "primaries-$seed.trace" ] || rows+=("$f")
		done
	done
	each planted ${rows[@]+"${rows[@]}"}
	failed=0
	for seed in $seeds; do
		dir=$tmp/check/primaries-$seed
		if ! grep -q '^ok' "$dir/verdict"; then
			echo "FAIL seed $seed: baseline fails, no evidence: $(head -1 "$dir/verdict")"
			failed=1
			continue
		fi
		echo "seed $seed: baseline passes, $(cat "$dir/verdict")"
		for f in "$tmp"/traces/*-"$seed".trace; do
			name=$(basename "$f" .trace)
			[ "$name" = "primaries-$seed" ] || cat "$tmp/check/$name/line"
		done
	done
	# The epoch plants whose trace TLC rejects on some seed they fire on, each firing on some of seeds 0 to 39; most fire
	# on a few seeds in 40, so another window may have none, and some pass on a seed they fire on.
	ecaught="ackOnLoneCopy activateEarly mergeWhileLacking mergeOnDropped editKeepsInterval relistNever peerInOldInterval oldReport
		recoverFromListed listerVetoes listerOrders noDegradedHold ownInterval reportDecided removeOnActivation sendEpoch goneWhileUp"
	for plant in $ecaught; do
		[ -z "${PLANTS:-}" ] || case " $PLANTS " in *" $plant "*) ;; *) continue ;; esac
		fired=0 rejected=0
		for seed in $seeds; do
			line=$tmp/check/$plant-$seed/line
			grep -q '^ok' "$tmp/check/primaries-$seed/verdict" && [ -f "$line" ] || continue
			case $(head -1 "$line") in
			*": not fired,"* | *": NO DATA,"*) ;;
			*": rejected,"*) fired=$((fired + 1)) rejected=$((rejected + 1)) ;;
			*) fired=$((fired + 1)) ;;
			esac
		done
		if [ $fired -eq 0 ] && [ "${FIRST:-0}" -eq 0 ] && [ "${COUNT:-10}" -ge 40 ]; then
			echo "FAIL $plant: fired on none of seeds 0 to 39, where the record has it fire"
			failed=1
		elif [ $fired -eq 0 ]; then
			echo "$plant: no evidence, fired on none of these seeds"
		elif [ $rejected -eq 0 ]; then
			echo "FAIL $plant: fired on $fired seeds and rejected on none, a plant expected rejected"
			failed=1
		else
			echo "$plant: caught, rejected on $rejected of the $fired seeds it fired on"
		fi
	done
	rm -f "$tmp"/traces/*
	CONFORM_DIR=$tmp/traces GOMEMLIMIT=4GiB go test -count=1 ./internal/raftshard -run '^TestVoterPlantsKeepTraces$'
	go build -o "$tmp/votersdata" ./tools/votersdata
	each plain "$tmp"/traces/voters-*.trace
	# The voter plants whose trace TLC rejects at the planted step; the others may pass.
	caught=" campaign-unapplied restart-commit-at-base hard-state-old voter-first eager joint-one-half one-at-a-time delete-on-plan notice-any-index notice-non-member disloyal "
	for f in "$tmp"/traces/voters-*.trace; do
		name=$(basename "$f" .trace)
		dir=$tmp/check/$name
		if [ "${name#voters-plant-}" = "$name" ]; then
			cat "$dir/line"
			grep -q '^ok' "$dir/line" || failed=1
			continue
		fi
		plant=${name#voters-plant-}
		plant=${plant%-*}
		expect="not caught"
		case $caught in *" $plant "*) expect=caught ;; esac
		case $(head -1 "$dir/line") in
		"ok   "*) verdict=PASSED ;;
		*": no step for line "* | *": invariant "*" broken by line "*) verdict=rejected ;;
		*)
			echo "ERROR $(tr '\n' ' ' <"$dir/line")"
			failed=1
			continue
			;;
		esac
		basedir=$tmp/check/voters-base-${name#voters-plant-}
		if grep -q '^ok' "$basedir/line"; then
			steps "$basedir/trace_data.tla" >"$dir/base.lines"
			steps "$dir/trace_data.tla" >"$dir/plant.lines"
			same=$(cmp "$dir/base.lines" "$dir/plant.lines" 2>&1 || true)
			case $same in
			*"EOF on "*base.lines*) fired="fired at line $(($(wc -l <"$dir/base.lines") + 1))" ;;
			*" line "*) fired="fired at line $(echo "$same" | sed 's/.* line \([0-9]*\).*/\1/')" ;;
			*) fired="not fired, its lines are a prefix of the baseline's" ;;
			esac
		else
			fired="no baseline to compare"
		fi
		row="$fired: $(sed "s/^[a-zA-Z]* *$name: //" "$dir/line" | cut -c1-200 | tr '\n' ' ')"
		if [ "$verdict $expect" = "PASSED caught" ]; then
			echo "FAIL $name: PASSED, a plant expected rejected, $row"
			failed=1
		else
			echo "$name: $verdict, $expect, $row"
		fi
	done
	coverage "$tmp"/traces/voters-*.trace
	exit $failed
fi

if [ $# -eq 0 ]; then
	rm -f "$tmp"/traces/*
	CONFORM_DIR=$tmp/traces GOMEMLIMIT=4GiB go test -run "${RUN:-TestConform}" ./internal/chunkstore \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-10}"
	CONFORM_DIR=$tmp/traces GOMEMLIMIT=4GiB go test -run "${RUN:-TestConform}" ./internal/raftshard \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-50}"
	set -- "$tmp"/traces/*.trace
fi
go build -o "$tmp/tracedata" ./tools/tracedata
go build -o "$tmp/votersdata" ./tools/votersdata

each plain "$@"
failed=0
for f in "$@"; do
	line=$tmp/check/$(basename "$f" .trace)/line
	cat "$line"
	grep -q '^ok' "$line" || failed=1
done
coverage "$@"
exit $failed
