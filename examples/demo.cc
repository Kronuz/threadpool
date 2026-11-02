// A runnable tour of threadpool.
//
// Build (when this repo is the top-level project):
//   cmake -B build && cmake --build build && ./build/threadpool_demo
//
// The one idea worth taking away: a WorkPool is a fixed set of long-lived, named worker
// threads that many owners share. Each owner posts to its own WorkGroup, the pool serves the
// groups in fair turns, and a group waits for, or cancels, its own work only. This demo
// spreads a batch across named workers, collects results through promises, reads the live
// counters, and cancels the queued tail of a group.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "thread.hh"       // get_thread_name() -> the running worker's name
#include "work_pool.hh"    // WorkPool, WorkGroup, WorkItem

using namespace std::chrono_literals;

static void rule(const char* title) {
	std::printf("\n\033[1m-- %s --\033[0m\n", title);
}

int main() {
	std::printf("threadpool demo  (hardware concurrency: %u)\n",
	            std::thread::hardware_concurrency());

	// --- 1. the work really runs on N named worker threads -------------------
	rule("concurrency: which named worker ran each task");
	// 4 workers named "worker-0".."worker-3"; the format is a std::format string
	// and {} gets the worker index. reserve = 0 lets every worker take any work
	// (by default one stays free for queries). Each task asks the library who it
	// is running as and we tally how many tasks each worker handled.
	{
		constexpr int N = 200;
		WorkPool pool({.threads = 4, .reserve = 0, .name_format = "worker-{}"});
		WorkGroup group(pool);

		std::mutex m;
		std::map<std::string, int> by_worker;

		for (int i = 0; i < N; ++i) {
			group.enqueue([&m, &by_worker] {
				// Tiny bit of work so the scheduler hands tasks around.
				std::this_thread::sleep_for(1ms);
				const std::string& who = get_thread_name();
				std::lock_guard<std::mutex> lk(m);
				by_worker[who]++;
			});
		}
		group.wait();  // waits for this group's tasks only

		std::printf("  %d tasks fanned out across %zu named workers:\n",
		            N, by_worker.size());
		for (auto& [name, count] : by_worker) {
			std::printf("    %-10s ran %3d tasks\n", name.c_str(), count);
		}
	}

	// --- 2. submit a batch, collect results through promises -----------------
	rule("a batch of results through promises");
	{
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup group(pool);

		std::vector<std::promise<long>> promises(8);
		std::vector<std::future<long>> results;
		for (auto& p : promises) results.push_back(p.get_future());
		for (int i = 1; i <= 8; ++i) {
			group.enqueue([&promises, i] {
				// A toy "expensive" job: sum 1..n with a little sleep so the work
				// actually overlaps across workers instead of finishing instantly.
				std::this_thread::sleep_for(20ms);
				long acc = 0;
				for (int k = 1; k <= i; ++k) acc += k;
				promises[i - 1].set_value(acc);
			});
		}

		std::fputs("  results : ", stdout);
		for (auto& f : results) std::printf("%ld ", f.get());
		std::putc('\n', stdout);
		std::puts("  (8 results, each produced on a worker; sum(1..n) for n=1..8)");
		group.wait();
	}

	// --- 3. live counters while work is in flight ----------------------------
	rule("pending() and running() while work is in flight");
	// pending() is what the group has queued and not started; running() is what
	// is executing now. Both read the group's state without involving the pool.
	{
		WorkPool pool({.threads = 3, .reserve = 0});
		WorkGroup group(pool);

		std::atomic<int> ran{0};
		for (int i = 0; i < 30; ++i) {
			group.enqueue([&ran] {
				std::this_thread::sleep_for(10ms);
				ran.fetch_add(1, std::memory_order_relaxed);
			});
		}

		// Peek once while the queue is still draining.
		std::this_thread::sleep_for(15ms);
		std::printf("  mid-flight : workers=%u  running=%zu  queued=%zu\n",
		            pool.threads(), group.running(), group.pending());

		group.wait();
		std::printf("  after wait : ran %d/30 tasks, queued=%zu, running=%zu\n",
		            ran.load(), group.pending(), group.running());
	}

	// --- 4. wait drains the queue, cancel_and_wait drops what has not started -
	rule("wait() runs the queue, cancel_and_wait() drops the tail");
	// Same batch, both ways, side by side.
	{
		auto run_one = [](bool drain) -> int {
			std::atomic<int> ran{0};
			WorkPool pool({.threads = 2, .reserve = 0});
			WorkGroup group(pool);
			for (int i = 0; i < 20; ++i) {
				group.enqueue([&ran] {
					std::this_thread::sleep_for(5ms);
					ran.fetch_add(1, std::memory_order_relaxed);
				});
			}
			if (drain) {
				group.wait();            // drain: all 20 run
			} else {
				std::this_thread::sleep_for(12ms);
				group.cancel_and_wait(); // queued tasks are dropped unrun
			}
			return ran.load();
		};

		std::printf("  wait()            ran %2d/20 tasks  (drained the queue)\n", run_one(true));
		std::printf("  cancel_and_wait() ran %2d/20 tasks  (dropped what had not started)\n", run_one(false));
	}

	std::puts("\ndone.");
	return 0;
}
