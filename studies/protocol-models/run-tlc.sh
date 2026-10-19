#!/usr/bin/env bash
# Checks CockroachDB's and etcd-raft's published TLA+ models with TLC and prints, per run, the
# distinct states, the depth reached and the wall time, as in results.txt.
# Usage: ./run-tlc.sh [dir] [workers] [cap-seconds]   (defaults ./work, 4, 900). Needs curl, java 11+, perl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
workers=${2:-4}
cap=${3:-900}
mkdir -p "$out/jars" "$out/specs"
cd "$out"

crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff
raft=f35a02212416d045c574e30aa59d4b07c4b8d732 # etcd-io/raft v3.6.0

jar() { # file url sha256
	[ -s "jars/$1" ] || curl -sSfL -o "jars/$1" "$2"
	echo "$3  jars/$1" | shasum -a 256 -c - >/dev/null
}
jar tla2tools-1.7.4.jar https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar \
	936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
# etcd-raft's model uses CommunityModules (FoldSeq), whose 2024 and 2025 jars need TLC 1.8, a
# pre-release whose jar is rebuilt in place; the checksum is the jar of 2026-10-15.
jar tla2tools-1.8.0.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar \
	15d103b7e5617780bcc904c0af12f1ad242e2b345c0301136c5be4588f17edac
jar CommunityModules-202505152026.jar \
	https://github.com/tlaplus/CommunityModules/releases/download/202505152026/CommunityModules-deps.jar \
	044e8ecdfbca92d51d7eb4469422c2a7da1fe25dc8ad39c4a90e6622d6da4d99

spec() { # repo commit path dir
	mkdir -p "specs/$4"
	local f="specs/$4/$(basename "$3")"
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}
spec cockroachdb/cockroach $crdb docs/tla-plus/ParallelCommits/ParallelCommits.tla pc
spec cockroachdb/cockroach $crdb docs/tla-plus/ParallelCommits/ParallelCommits.cfg pc
spec cockroachdb/cockroach $crdb docs/tla-plus/StoreLiveness/StoreLiveness.tla sl
spec cockroachdb/cockroach $crdb docs/tla-plus/StoreLiveness/StoreLiveness.cfg sl
for f in etcdraft.tla MCetcdraft.tla MCetcdraft.cfg; do spec etcd-io/raft $raft tla/$f raft; done

# ParallelCommits without its temporal properties: the safety invariants alone.
sed '/^PROPERTIES/,$d' specs/pc/ParallelCommits.cfg >specs/pc/Safety.cfg
# etcd-raft at 3 fixed servers, no reconfiguration, terms below 3, 2 client requests.
sed -e 's/^    Server      = .*/    Server      = {s1, s2, s3}/' \
	-e 's/ReconfigurationLimit    = 2/ReconfigurationLimit    = 0/' \
	-e 's/MaxTermLimit            = 10/MaxTermLimit            = 3/' \
	-e 's/RequestLimit            = 5/RequestLimit            = 2/' \
	specs/raft/MCetcdraft.cfg >specs/raft/Small.cfg

run() { # name dir classpath config spec [extra tlc flags]
	local name=$1 dir=$2 cp=$3 cfg=$4 tla=$5
	shift 5
	local log start end
	log=$(mktemp)
	start=$(date +%s)
	(cd "specs/$dir" && rm -rf states &&
		perl -e 'alarm shift; exec @ARGV' "$cap" java -XX:+UseParallelGC -cp "$cp" tlc2.TLC \
			-workers "$workers" -config "$cfg" "$tla" "$@" >"$log" 2>&1) 2>/dev/null || true
	end=$(date +%s)
	local states depth outcome
	if grep -q 'Model checking completed. No error' "$log"; then
		outcome=complete
		states=$(grep -E '^[0-9]+ states generated' "$log" | sed -E 's/.* ([0-9]+) distinct states found.*/\1/')
		depth=$(grep -E 'depth of the complete state graph' "$log" | sed -E 's/.* is ([0-9]+)\./\1/')
	elif grep -q -E 'is violated|Error:' "$log"; then
		outcome="error: $(grep -m1 -E 'is violated|Error:' "$log")"
		states=-; depth=-
	else
		outcome="stopped at the ${cap} s cap"
		states=$(grep -E '^Progress' "$log" | tail -1 | sed -E 's/.* ([0-9,]+) distinct states found.*/\1/' | tr -d ,)
		depth=$(grep -E '^Progress' "$log" | tail -1 | sed -E 's/^Progress\(([0-9]+)\).*/\1/')
		grep -q 'Checking .* temporal properties' "$log" && outcome="$outcome, in the liveness check"
	fi
	printf '%-34s workers=%s distinct=%s depth=%s wall=%ss %s\n' "$name" "$workers" "$states" "$depth" "$((end - start))" "$outcome"
	rm -f "$log"
}

J=$PWD/jars
run "crdb StoreLiveness (as shipped)" sl "$J/tla2tools-1.7.4.jar" StoreLiveness.cfg StoreLiveness.tla -deadlock
run "crdb ParallelCommits (safety)" pc "$J/tla2tools-1.7.4.jar" Safety.cfg ParallelCommits.tla
run "crdb ParallelCommits (as shipped)" pc "$J/tla2tools-1.7.4.jar" ParallelCommits.cfg ParallelCommits.tla
run "etcd-raft MCetcdraft (small)" raft "$J/tla2tools-1.8.0.jar:$J/CommunityModules-202505152026.jar" Small.cfg MCetcdraft.tla
run "etcd-raft MCetcdraft (as shipped)" raft "$J/tla2tools-1.8.0.jar:$J/CommunityModules-202505152026.jar" MCetcdraft.cfg MCetcdraft.tla
