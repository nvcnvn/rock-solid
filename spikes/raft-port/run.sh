#!/usr/bin/env bash
# Reproduces every results*.txt file. Run ./fetch.sh first.
#   ./run.sh            all tests
#   ./run.sh p2 p5-7    selected tests (p1 p2 p3 p4 p5-7 p8 p9)
set -uo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$PWD/target"
export PROTOC="$PWD/vendor/protoc/bin/protoc"
REPO=$(cd ../.. && pwd)
B=target/release/raft-port-bench

header() {
	echo "# macOS $(sw_vers -productVersion) (Darwin $(uname -r)), $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(($(sysctl -n hw.memsize) >> 30)) GiB, $(rustc --version | cut -d' ' -f1-2), etcd-raft v3.7.0 (b867cf13f6bc), raft-rs 0.7.0, rand_chacha 0.10.0, $(date +%F)"
	echo "# loadavg at start: $(sysctl -n vm.loadavg)"
}

build() {
	(cd eraft && cargo build --release -q --tests 2>&1 | grep -v '^$')
	(cd bench && cargo build --release -q 2>&1 | grep -v '^$')
}

# Code lines: non-blank lines that are not only a comment.
code_lines() { cat "$@" | grep -v '^[[:space:]]*$' | grep -cv '^[[:space:]]*//'; }
raw_lines() { cat "$@" | wc -l | tr -d ' '; }

p1() {
	header
	cd vendor/raft
	GO_CORE="raft.go rawnode.go log.go log_unstable.go storage.go read_only.go status.go util.go bootstrap.go types.go logger.go"
	GO_PKGS="tracker/inflights.go tracker/progress.go tracker/state.go tracker/tracker.go quorum/joint.go quorum/majority.go quorum/quorum.go quorum/voteresult_string.go confchange/confchange.go confchange/restore.go raftpb/confchange.go raftpb/confstate.go raftpb/util.go raftpb/alias.go"
	echo "P1 Go ported (non-test): $(raw_lines $GO_CORE $GO_PKGS) lines, $(code_lines $GO_CORE $GO_PKGS) code lines (excluding blank and comment-only lines)"
	echo "      Go node.go (only SoftState, Ready, IsEmpty*, Peer, confChangeToMsg ported): $(raw_lines node.go) lines"
	echo "      Go raftpb/raft.proto (message definitions ported as structs): $(raw_lines raftpb/raft.proto) lines"
	for f in $GO_CORE $GO_PKGS; do printf '      %-32s %5s lines %5s code\n' "$f" "$(raw_lines "$f")" "$(code_lines "$f")"; done
	echo "      Go left out: node.go Node/goroutines ($(raw_lines node.go) lines), raftpb/raft.pb.go generated codec ($(raw_lines raftpb/raft.pb.go)), state_trace.go TLA+ tracing behind build tag with_tla ($(raw_lines state_trace.go state_trace_nop.go)), doc.go ($(raw_lines doc.go), docs only)"
	echo "      Go test harness rafttest/ (non-test files): $(raw_lines rafttest/interaction_env*.go) lines"
	cd ../../eraft
	LIB=$(ls src/*.rs src/*/*.rs | grep -vE '_test\.rs$|testutil\.rs$')
	echo "P1 Rust library (eraft/src without tests): $(raw_lines $LIB) lines, $(code_lines $LIB) code lines"
	for f in $LIB; do printf '      %-32s %5s lines %5s code\n' "$f" "$(raw_lines "$f")" "$(code_lines "$f")"; done
	TESTS=$(ls src/*_test.rs src/testutil.rs tests/*.rs tests/*/*.rs)
	echo "P1 Rust tests and harness: $(raw_lines $TESTS) lines"
	for f in $TESTS; do printf '      %-32s %5s lines\n' "$f" "$(raw_lines "$f")"; done
	cd ..
}

p2() {
	header
	echo "# P2: etcd-raft testdata/*.txt through the ported rafttest harness, rewrite mode, byte comparison"
	./target/release/deps/$(ls -t target/release/deps | grep -E '^interaction-[0-9a-f]+$' | head -1) \
		vendor/raft/testdata target/interaction-out
	echo "# negative control: the same harness on two corrupted copies must fail"
	neg=target/p2-negative && rm -rf $neg && mkdir -p $neg
	cp vendor/raft/testdata/campaign.txt vendor/raft/testdata/prevote.txt $neg/
	sed -i '' 's/became leader at term 1/became LEADER at term 1/' $neg/campaign.txt
	sed -i '' '40,60d' $neg/prevote.txt
	./target/release/deps/$(ls -t target/release/deps | grep -E '^interaction-[0-9a-f]+$' | head -1) $neg $neg/out
}

p3() {
	header
	echo "# P3 and the other ported unit tests (cargo test --lib); upstream Go suite for reference"
	(cd vendor/raft && GOFLAGS=-mod=mod /opt/homebrew/bin/go version && GOFLAGS=-mod=mod /opt/homebrew/bin/go test -count=1 ./... 2>&1)
	(cd eraft && cargo test --release --lib 2>&1 | grep -E '^test |test result')
	for t in quorum_datadriven confchange_datadriven; do
		[ -f eraft/tests/$t.rs ] && (cd eraft && cargo test --release -q --test $t 2>&1 | grep -v '^$')
	done
}

p4() {
	header
	echo "# P4: virtual time, 30 groups x 3 nodes, seeded message shuffle, 10% drop, node 1 stops at tick 150; 10 separate processes per seed"
	for seed in 11 12; do
		h=$(for _ in $(seq 10); do $B sim-port --groups 30 --seed $seed | grep -o 'trace=[0-9a-f]*'; done | sort | uniq -c)
		n=$(echo "$h" | wc -l | tr -d ' ')
		tag="[FAIL]"; [ "$n" = 1 ] && tag="[OK ]"
		echo "$tag P4 seed=$seed: got=$n distinct traces in 10 runs want=1"
		echo "$h" | sed 's/^/      /'
	done
	a=$($B sim-port --groups 30 --seed 11 | grep -o 'trace=[0-9a-f]*')
	b=$($B sim-port --groups 30 --seed 12 | grep -o 'trace=[0-9a-f]*')
	c=$($B sim-port --groups 30 --seed 11 --rand-seed 99 | grep -o 'trace=[0-9a-f]*')
	tag="[FAIL]"; [ "$a" != "$b" ] && tag="[OK ]"
	echo "$tag P4 seed 11 vs 12: got=$a vs $b want=different"
	tag="[FAIL]"; [ "$a" != "$c" ] && tag="[OK ]"
	echo "$tag P4b same network seed 11, election rand_seed 11 vs 99: got=$a vs $c want=different (the injected RNG drives elections)"
	$B sim-port --groups 30 --seed 11 | cut -c1-160
}

p57() {
	header
	G=${GROUPS_N:-1000}
	DEFAULT="--hb 50 --elect 150-300"
	TUNED="--hb 100 --elect 1000-2000"
	echo "# P5/P6: $G groups x 3 replicas in one process, idle 10 s, check_quorum + pre_vote; two runs per library, alternating"
	for rep in 1 2; do
		for lib in raftrs port; do echo; $B $lib --groups "$G" $DEFAULT --idle 10; done
	done
	for rep in 1 2; do
		for lib in raftrs port; do echo; $B $lib --groups "$G" $TUNED --idle 10; done
	done
	echo; echo "# P7: 100 B proposals, 32 in flight per group, one thread, tuned timing, 5 s"
	for fs in 1000 0; do
		for rep in 1 2; do
			for lib in raftrs port; do echo; $B $lib --groups "$G" $TUNED --write 5 --inflight 32 --fsync-us $fs; done
		done
	done
}

p8() {
	header
	echo "# P8: dependencies of the library crate (normal edges, all targets)"
	(cd eraft && cargo tree -e normal --target all --prefix none)
	IO_DEPS='tokio|mio|libc|nix|rustix|io-uring|socket2|async-std|smol|getrandom'
	bad=$(cd eraft && cargo tree -e normal --target all --prefix none | sed 's/ .*//' | sort -u | grep -Ex "$IO_DEPS")
	tag="[OK ]"; [ -n "$bad" ] && tag="[FAIL]"
	echo "$tag P8 scripts/lint.sh I/O dependency check: got=${bad:-none} want=none"
	echo "# cargo-deny with the repo's deny.toml (licenses, advisories, bans, sources)"
	(cd eraft && cargo deny -L error --config "$REPO/deny.toml" check 2>&1 | tail -6; echo "cargo-deny exit status: ${PIPESTATUS[0]}")
	echo "# clippy with the repo's root clippy.toml (sans-IO bans), library only, -D warnings"
	(cd eraft && touch src/lib.rs && CLIPPY_CONF_DIR="$REPO" cargo clippy --lib -- -D warnings \
		-D clippy::disallowed_types -D clippy::disallowed_methods 2>&1 | tail -3)
	echo "# for information: the repo's full workspace lint set (pedantic, missing_docs, unwrap_used, ...) on the library, warnings by message"
	(cd eraft && touch src/lib.rs && CLIPPY_CONF_DIR="$REPO" cargo clippy --lib --message-format=short -- -W clippy::all -W clippy::pedantic \
		-W missing_docs -W unreachable_pub -W unused_qualifications -W clippy::unwrap_used -W clippy::string_slice \
		-A clippy::module_name_repetitions -A clippy::must_use_candidate -A clippy::missing_errors_doc \
		-A clippy::missing_panics_doc 2>&1 | grep -oE 'warning: .*' | grep -v 'generated [0-9]* warning' |
		sed -E 's/`[^`]*`/X/g; s/\([0-9]+\/[0-9]+\)//' | sort | uniq -c | sort -rn)
	echo "# unsafe code and banned items in the library source (excluding tests)"
	(cd eraft && echo "unsafe blocks/fns/impls: $(grep -rnE 'unsafe (\{|fn|impl)' src | grep -v _test | grep -vc testutil); HashMap/HashSet/Instant/SystemTime/std::thread/Mutex: $(grep -rnE 'HashMap|HashSet|Instant|SystemTime|std::thread|Mutex' src | grep -v _test | grep -vc testutil)")
}

p9() {
	header
	echo "# P9: 1,000 seeds x (3 and 5 nodes), 10,000 ticks each (election 10 ticks); drop 5%, dup 2%, delay 0-3 ticks, reorder, partitions, crash+restart, compaction, snapshots, conf changes"
	S=./target/release/deps/$(ls -t target/release/deps | grep -E '^sim-[0-9a-f]+$' | head -1)
	$S 1000 1 10000 0 2>&1 | grep -v '^$\|^thread\|^note'
	echo "# negative control: same runs with a disk that forgets the vote on crash; the checker must find violations"
	$S 1000 1 3000 1 2>&1 | grep -v '^$\|^thread\|^note'
	echo "# regression: seed 237 without the application's stale-apply guard (async storage writes)"
	$S 1 237 3000 0 0 2>&1 | grep -E '^\[' | head -2
	echo "# regression: regress/async_snapshot_overtakes_apply.txt, port harness (expect the panic at the last directive)"
	./target/release/deps/$(ls -t target/release/deps | grep -E '^interaction-[0-9a-f]+$' | head -1) regress target/regress-out 2>&1 | grep -v '^note'
	echo "# regression: the same file on upstream Go etcd-raft v3.7.0 (expect the same panic)"
	g=target/goraft && rm -rf $g && mkdir -p $g && (cd vendor/raft && tar cf - --exclude .git .) | (cd $g && tar xf -)
	rm $g/testdata/*.txt && cp regress/async_snapshot_overtakes_apply.txt $g/testdata/
	(cd $g && GOFLAGS=-mod=mod /opt/homebrew/bin/go test -count=1 -run TestInteraction . -rewrite 2>&1 | grep -E 'panic during|^panic:|^(ok|FAIL)' | head -4)
	echo "# cross-check: every directive before the panic, with expected output written by the port, passes unchanged on Go"
	python3 -c "import sys; s=open('regress/async_snapshot_overtakes_apply.txt').read(); open(sys.argv[1],'w').write(s[:s.index('# The stalled apply thread')])" target/regress-prefix.txt
	mkdir -p target/regress-prefix && mv target/regress-prefix.txt target/regress-prefix/async_snapshot_overtakes_apply.txt
	./target/release/deps/$(ls -t target/release/deps | grep -E '^interaction-[0-9a-f]+$' | head -1) target/regress-prefix target/regress-prefix/out >/dev/null 2>&1
	rm $g/testdata/*.txt && cp target/regress-prefix/out/async_snapshot_overtakes_apply.txt $g/testdata/
	(cd $g && GOFLAGS=-mod=mod /opt/homebrew/bin/go test -count=1 -run TestInteraction . 2>&1 | tail -1)
}

build
want=("$@")
[ ${#want[@]} -eq 0 ] && want=(p1 p2 p3 p4 p8 p9 p5-7)
for t in "${want[@]}"; do
	case $t in
	p1) p1 >results-p1.txt 2>&1 ;;
	p2) p2 >results-p2.txt 2>&1 ;;
	p3) p3 >results-p3.txt 2>&1 ;;
	p4) p4 >results-p4.txt 2>&1 ;;
	p5-7) p57 >results-p5-7.txt 2>&1 ;;
	p8) p8 >results-p8.txt 2>&1 ;;
	p9) p9 >results-p9.txt 2>&1 ;;
	*) echo "unknown test $t" >&2 ;;
	esac
done
