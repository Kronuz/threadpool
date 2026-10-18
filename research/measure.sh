#!/usr/bin/env bash
# Runs the research matrix: measure.sh BUILD_DIR OUT_DIR SUFFIX [TRIALS]
# Each workload writes OUT_DIR/<workload>-<config>-SUFFIX.txt. The caller wraps multi-core
# runs in a strict `quiet run` on the host; this script takes no holds itself.
set -euo pipefail
build="$1"; out="$2"; suffix="$3"; trials="${4:-3}"
W="$build/research/research_workloads"
mkdir -p "$out"

all="baseline,raw,steal,steal-spin,groups,groups-spin,prio,prio-r2,home,home-spin"
refs=""
if "$W" tiny pools=taskflow tasks=1000 trials=1 >/dev/null 2>&1; then refs=",taskflow"; fi
if "$W" tiny pools=moodycamel tasks=1000 trials=1 >/dev/null 2>&1; then refs="$refs,moodycamel"; fi

run() { local name="$1"; shift; echo "== $name: $*"; "$W" "$@" trials="$trials" | tee "$out/$name-$suffix.txt"; }

case "${WHAT:-all}" in
tiny|all)
	run tiny-ext-g1 tiny pools="$all$refs" tasks=500000 groups=1 producers=1
	run tiny-ext-g1000 tiny pools="$all$refs" tasks=500000 groups=1000 producers=1
	run tiny-ext-g10000 tiny pools="$all$refs" tasks=500000 groups=10000 producers=1
	run tiny-ext4-g1000 tiny pools="$all$refs" tasks=500000 groups=1000 producers=4
	run tiny-ext-1us-g1000 tiny pools="$all$refs" tasks=500000 groups=1000 producers=4 work_ns=1000
	run tiny-chain-g1000 tiny pools="steal,steal-lifo,groups,prio,home$refs" tasks=2000000 groups=1000 producers=0
	;;&
tail|all)
	run tail-hot1 tail pools="baseline,steal,groups,prio,prio-r2,home" groups=1 outstanding=64 merges=4 seconds=4
	run tail-busy1000 tail pools="baseline,steal,groups,prio,prio-r2,home" groups=1000 outstanding=1 merges=4 seconds=4
	run tail-idle10000 tail pools="baseline,steal,groups,prio,prio-r2,home" groups=10000 busy=100 outstanding=1 merges=4 seconds=4
	;;&
makespan|all)
	run makespan-g1000 makespan pools="baseline,steal,groups,prio,home" groups=1000 outstanding=1
	run makespan-g100 makespan pools="baseline,steal,groups,groups-q1ms,prio,home" groups=100 outstanding=1
	;;&
idle|all)
	run idle idle pools="$all$refs" seconds=3
	;;&
memory|all)
	run memory memory pools="baseline,steal,groups,prio,home" groups=10000
	;;
esac
