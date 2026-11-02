#!/bin/bash
# Mutation checks for the pool's queue, its tasks and TaskGroup: each mutant breaks one thing that
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

run "a sleeper does not look at the queue again before it waits" pool_queue.h \
	"if (_head_pos.load(std::memory_order_relaxed) != _tail_pos.load(std::memory_order_seq_cst)) {" \
	"if (false) {" test/pool_stress.cc
run "a producer never wakes a sleeper" pool_queue.h \
	"	void wake(std::size_t n) {
		if (_idle.load(std::memory_order_seq_cst) != 0) {" "	void wake(std::size_t n) {
		if (false) {" test/pool_stress.cc
run "the running flag is never set" pool_queue.h \
	"claim->store(1, std::memory_order_relaxed);" "(void)claim;" test/pool_stress.cc
run "a producer wakes every sleeper instead of one" pool_queue.h \
	"if (k > n) {
				k = n;
			}" "" test/pool_stress.cc
run "the head index restarts at 1 in the next chunk" pool_queue.h \
	"_head_idx = 0;
		}
		return true;" "_head_idx = 1;
		}
		return true;" test/pool_stress.cc
run "size() counts the end() sentinels" pool_queue.h \
	"if (counted) {
			_tasks_pushed.store" "if (true) {
			_tasks_pushed.store" test/pool_diff.cc
run "a stale tail cache reports an empty queue as having an item" pool_queue.h \
	"if (pos == _tail_cache.load(std::memory_order_relaxed)) {" "if (false) {" test/pool_stress.cc
run "SmallTask relocation leaves the source alive" small_task.h \
	"get(src).~D();" "" test/small_task.cc
run "a boxed SmallTask is never freed" small_task.h \
	"static void destroy(void* p) noexcept { delete get(p); }" "static void destroy(void*) noexcept {}" test/small_task.cc
run "async drops the exception instead of setting it" threadpool.hh \
	"promise.set_exception(std::current_exception());" "(void)0;" test/pool_stress.cc
run "post drops its call" threadpool.hh \
	"std::apply(std::move(func), std::move(args));
		});
	}" "(void)func; (void)args;
		});
	}" test/pool_stress.cc
run "no consumer polls before it sleeps" pool_queue.h \
	"if (try_pop(out, claim) || spin(out, claim)) {" "if (try_pop(out, claim)) {" test/pool_spin.cc
run "a poller that gives up stays marked as polling" pool_queue.h \
	"_spinning.store(0, std::memory_order_seq_cst);
		const bool got" "if (found) _spinning.store(0, std::memory_order_seq_cst);
		const bool got" test/pool_spin.cc
run "a worker keeps its group's ticket whatever waits on the pool queue" task_group.hh \
	"return pool->size() > mine || pool->finished() || pool->ending();" "(void)mine; return false;" test/task_group.cc
run "a worker that enters a ticket does not count it off the pool queue" task_group.hh \
	"static void dispatch(const std::shared_ptr<State>& state) {
		state->in_pool.fetch_sub(1, std::memory_order_relaxed);" "static void dispatch(const std::shared_ptr<State>& state) {" test/task_group.cc
run "an enqueue posts a ticket while another is on its way to a worker" task_group.hh \
	"if (tickets == running && tickets < max_tickets && !queued.empty()) {" \
	"if (tickets < max_tickets && !queued.empty()) {" test/task_group.cc
run "the worker that takes a ticket does not list the next before its task" task_group.hh \
	"task = state->take(started);
			widen = state->list();
		}
		for (;;) {" "task = state->take(started);
			widen = false;
		}
		for (;;) {" test/task_group.cc
run "a ticket that retires after a burst stays counted" task_group.hh \
	"if (state->queued.empty()) {
					--state->tickets;
					if (state->running == 0) {" "if (state->queued.empty()) {
					if (state->running == 0) {" test/task_group.cc

if [ "$survived" -ne 0 ]; then
	echo "some mutants survived"
	exit 1
fi
echo "all mutants caught"
