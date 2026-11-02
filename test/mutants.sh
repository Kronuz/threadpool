#!/bin/bash
# Mutation checks for WorkPool's counters: each mutant breaks one thing that
# the tests are meant to pin, builds the test that should notice, and must fail it. A surviving
# mutant means a missing test. Run from the repository root:
#
#   CXX=g++ EXTRA="-I/path/with/config.h" test/mutants.sh
#
# EXTRA carries the include directory of the generated config.h (the CMake build directory) and,
# on a toolchain without <format>, a shim directory. Needs python3.
set -u
CXX=${CXX:-c++}
EXTRA=${EXTRA:-}
ROOT=$(pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
survived=0

run() { # name file old new test
	local name=$1 file=$2 old=$3 new=$4 test=$5
	rm -rf "$WORK/src" && mkdir "$WORK/src"
	cp -R "$ROOT"/*.h "$ROOT"/*.hh "$ROOT"/*.cc "$ROOT"/test "$WORK/src/"
	if ! python3 - "$WORK/src/$file" "$old" "$new" <<'PY'
import sys
path, old, new = sys.argv[1:4]
text = open(path).read()
if old not in text:
	sys.exit(3)
open(path, "w").write(text.replace(old, new, 1))
PY
	then
		echo "PATTERN NOT FOUND: $name ($file)"
		survived=1
		return
	fi
	if ! (cd "$WORK/src" && $CXX -std=c++20 -O1 $EXTRA -I. -Itest "$test" thread.cc -o "$WORK/bin" -pthread 2>"$WORK/build.log"); then
		echo "DID NOT BUILD: $name"
		head -3 "$WORK/build.log"
		survived=1
		return
	fi
	if THREADPOOL_STRESS_SECONDS=1 timeout 120 "$WORK/bin" >"$WORK/out" 2>&1; then
		echo "SURVIVED: $name"
		survived=1
	else
		echo "caught: $name: $(grep -m1 'CHECK failed' "$WORK/out" || echo "exit $?")"
	fi
}

run "pending() counts only the query lane" work_pool.hh \
	"std::size_t n = _s->pending();" "std::size_t n = _s->lanes[0].pending;" test/work_counts.cc
run "running() reports the queued count" work_pool.hh \
	"std::size_t n = _s->running;" "std::size_t n = _s->pending();" test/work_counts.cc

if [ "$survived" -ne 0 ]; then
	echo "some mutants survived"
	exit 1
fi
echo "all mutants caught"
