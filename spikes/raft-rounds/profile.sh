#!/bin/sh
# CPU profiles behind conclusion 4; ./profile.sh > results-cpuprofile.txt 2>&1
set -e
cd "$(dirname "$0")"
mkdir -p work
echo "# $(uname -sr), $(go version | cut -d' ' -f3), etcd-raft v3.7.0, $(nproc) vCPUs, $(date -u +%Y-%m-%dT%H:%MZ), loadavg $(cut -d' ' -f1-3 /proc/loadavg) (machine shared with other benchmarks)"
./bin/rounds -mode open -rate 30000 -flush-us 100 -loops 4 -warmup 0.3 -measure 1 -cpuprof work/cpu-light.pprof -label "P1 open 30,000/s, flush 0.1 ms, 4 loops"
go tool pprof -top -cum -nodecount=40 bin/rounds work/cpu-light.pprof 2>/dev/null | sed -n '1,46p'
go tool pprof -peek 'RawNode..HasReady$' bin/rounds work/cpu-light.pprof 2>/dev/null | sed -n '/flat/,/^-.*-$/p' | head -14
./bin/rounds -mode open -rate 60000 -warmup 0.3 -measure 1 -cpuprof work/cpu-60k.pprof -label "P2 open 60,000/s, flush 0.95 ms, 1 loop"
go tool pprof -top -cum -nodecount=40 bin/rounds work/cpu-60k.pprof 2>/dev/null | sed -n '1,46p'
