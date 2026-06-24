// Smoke test for the standalone threadpool library.
//
// Exercises the three pieces a consumer relies on:
//   1. ThreadPool<std::function<void()>>: submit N tasks, verify every one runs
//      and the accumulated result is correct.
//   2. ThreadPool::async: package a callable + args, get a std::future back, and
//      verify the returned value (the PackagedTask / future path).
//   3. TaskQueue<std::packaged_task<...>>: enqueue, call (drain), and clear.
//
// Build: c++ -std=c++20 -I.. test.cc thread.cc -o test && ./test
// or via CMake: cmake -B build && cmake --build build && ctest --test-dir build
#include <atomic>
#include <cassert>
#include <cstdio>
#include <functional>
#include <future>
#include <vector>

#include "threadpool.hh"

int main() {
	// ---- 1. Run N tasks on a pool and verify all of them executed. ----
	constexpr int N = 1000;
	{
		ThreadPool<std::function<void()>> pool("W{:02}", 4);

		std::atomic<int> ran{0};
		std::atomic<long> sum{0};
		for (int i = 0; i < N; ++i) {
			bool ok = pool.enqueue([i, &ran, &sum] {
				ran.fetch_add(1, std::memory_order_relaxed);
				sum.fetch_add(i, std::memory_order_relaxed);
			});
			assert(ok);
		}

		pool.end();      // let workers drain the queue, then exit
		pool.join();

		// expected = 0 + 1 + ... + (N-1)
		long expected = static_cast<long>(N) * (N - 1) / 2;

		assert(ran.load() == N);
		assert(sum.load() == expected);
		std::printf("threadpool OK: ran %d/%d tasks, sum=%ld (expected %ld)\n",
		            ran.load(), N, sum.load(), expected);
	}

	// ---- 2. async() returns a future with the correct result. ----
	{
		ThreadPool<std::function<void()>> pool("A{:02}", 2);

		std::vector<std::future<int>> futures;
		for (int i = 0; i < 50; ++i) {
			futures.push_back(pool.async([](int a, int b) { return a + b; }, i, i * 2));
		}

		int got = 0;
		for (int i = 0; i < 50; ++i) {
			int v = futures[i].get();
			assert(v == i + i * 2);
			got += v;
		}

		pool.finish();
		pool.join();

		std::printf("threadpool OK: async() resolved 50 futures, sum of results=%d\n", got);
	}

	// ---- 3. TaskQueue: enqueue / call / clear. ----
	{
		TaskQueue<std::packaged_task<int(int)>> tq;

		auto fut = tq.enqueue(std::packaged_task<int(int)>([](int x) { return x * x; }));
		bool called = tq.call(7);            // run the one queued task with arg 7
		assert(called);
		assert(fut.get() == 49);

		bool again = tq.call(1);             // queue is empty now
		assert(!again);

		// Enqueue a batch, then clear without calling them.
		for (int i = 0; i < 40; ++i) {
			tq.enqueue(std::packaged_task<int(int)>([](int x) { return x; }));
		}
		std::size_t cleared = tq.clear();
		assert(cleared == 40);

		std::printf("threadpool OK: TaskQueue call() ran a task (49), clear() dropped %zu tasks\n",
		            cleared);
	}

	std::printf("all threadpool tests passed\n");
	return 0;
}
