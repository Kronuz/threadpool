// A runnable tour of threadpool.
//
// Build (when this repo is the top-level project):
//   cmake -B build && cmake --build build && ./build/threadpool_demo
//
// The one idea worth taking away: a ThreadPool is a fixed set of long-lived,
// named worker threads pulling tasks off one shared queue. You hand it work
// three ways (fire-and-forget enqueue, bulk enqueue, or call-and-await async),
// the workers run it concurrently, and a two-phase shutdown (end drains the
// queue, finish stops asap) lets you finish cleanly. This demo submits a batch
// and collects futures, shows the same work landing on several workers by name,
// watches the live counters, and drains the pool gracefully.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "threadpool.hh"   // ThreadPool, TaskQueue, PackagedTask
#include "thread.hh"       // get_thread_name() -> the running worker's name

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
	// and {} gets the worker index. Each task asks the library who it is running
	// as (get_thread_name reads the OS thread name the pool gave this worker at
	// construction). We tally how many tasks each named worker handled, proving
	// the load spread across them. (This section runs first on purpose: workers
	// are detached, so once a thread id is registered a later pool reusing that
	// id keeps the earlier name, see get_thread_name's emplace.)
	{
		constexpr int N = 200;
		ThreadPool<> pool("worker-{}", 4);

		std::mutex m;
		std::map<std::string, int> by_worker;

		std::vector<std::future<void>> done;
		done.reserve(N);
		for (int i = 0; i < N; ++i) {
			done.push_back(pool.async([&m, &by_worker] {
				// Tiny bit of work so the scheduler hands tasks around.
				std::this_thread::sleep_for(1ms);
				const std::string& who = get_thread_name();
				std::lock_guard<std::mutex> lk(m);
				by_worker[who]++;
			}));
		}
		for (auto& f : done) f.get();

		std::printf("  %d tasks fanned out across %zu named workers:\n",
		            N, by_worker.size());
		for (auto& [name, count] : by_worker) {
			std::printf("    %-10s ran %3d tasks\n", name.c_str(), count);
		}

		pool.end();
		pool.join();
	}

	// --- 2. submit a batch, collect results as futures -----------------------
	rule("async(): submit a batch, await results as futures");
	// async() packages each call and hands back a std::future you await later.
	{
		ThreadPool<> pool("W{:02}", 4);

		std::vector<std::future<long>> results;
		for (int i = 1; i <= 8; ++i) {
			// A toy "expensive" job: sum 1..n with a little sleep so the work
			// actually overlaps across workers instead of finishing instantly.
			results.push_back(pool.async([](int n) {
				std::this_thread::sleep_for(20ms);
				long acc = 0;
				for (int k = 1; k <= n; ++k) acc += k;
				return acc;
			}, i));
		}

		std::fputs("  results : ", stdout);
		for (auto& f : results) std::printf("%ld ", f.get());
		std::putc('\n', stdout);
		std::puts("  (8 futures, each resolved off a worker; sum(1..n) for n=1..8)");

		pool.end();   // let workers drain the queue, then exit
		pool.join();  // wait for them
	}

	// --- 3. fire-and-forget enqueue + live state counters --------------------
	rule("enqueue() + live counters while work is in flight");
	// enqueue() is fire-and-forget: no future, just push and move on. While the
	// batch is running we read the lock-free counters the pool keeps so size()
	// (queued), running_size() (in a worker right now), and threadpool_workers()
	// (live workers) report state without taking any lock.
	{
		ThreadPool<> pool("bg-{}", 3);

		std::atomic<int> ran{0};
		for (int i = 0; i < 30; ++i) {
			pool.enqueue([&ran] {
				std::this_thread::sleep_for(10ms);
				ran.fetch_add(1, std::memory_order_relaxed);
			});
		}

		// Peek once while the queue is still draining.
		std::this_thread::sleep_for(15ms);
		std::printf("  mid-flight : workers=%zu  running=%zu  queued=%zu\n",
		            pool.threadpool_workers(), pool.running_size(), pool.size());

		pool.end();
		pool.join();
		std::printf("  after join : ran %d/30 tasks, queued=%zu, finished=%s\n",
		            ran.load(), pool.size(), pool.finished() ? "yes" : "no");
	}

	// --- 4. graceful drain vs. stop-asap -------------------------------------
	rule("end() drains the queue, finish() stops asap");
	// end() lets every already-queued task run before workers exit; finish()
	// tells them to stop as soon as the current task returns, so queued-but-not-
	// started tasks may never run. Same batch, both shutdowns, side by side.
	{
		auto run_one = [](bool drain) -> int {
			std::atomic<int> ran{0};
			ThreadPool<> pool("S{:02}", 2);
			for (int i = 0; i < 20; ++i) {
				pool.enqueue([&ran] {
					std::this_thread::sleep_for(5ms);
					ran.fetch_add(1, std::memory_order_relaxed);
				});
			}
			if (drain) {
				pool.end();    // drain: all 20 should run
			} else {
				pool.finish(); // asap: workers stop early, some tasks dropped
			}
			pool.join();
			return ran.load();
		};

		std::printf("  end()    ran %2d/20 tasks  (drained the queue)\n", run_one(true));
		std::printf("  finish() ran %2d/20 tasks  (stopped before draining)\n", run_one(false));
	}

	// --- 5. TaskQueue: callbacks you drain yourself --------------------------
	rule("TaskQueue: queue now, run later by hand");
	// Separate from the pool: a one-shot queue of packaged tasks you drain
	// yourself by calling them with arguments. Xapiand uses it for callbacks
	// parked until a condition (a database becoming ready) is met.
	{
		TaskQueue<std::packaged_task<int(int)>> callbacks;
		auto squared = callbacks.enqueue(std::packaged_task<int(int)>(
			[](int x) { return x * x; }));

		bool ran = callbacks.call(9);  // run the one queued callback with arg 9
		std::printf("  call(9) -> ran=%s, future=%d\n",
		            ran ? "true" : "false", squared.get());

		for (int i = 0; i < 5; ++i) {
			callbacks.enqueue(std::packaged_task<int(int)>([](int x) { return x; }));
		}
		std::printf("  clear() dropped %zu queued callbacks unrun\n", callbacks.clear());
	}

	std::puts("\ndone.");
	return 0;
}
