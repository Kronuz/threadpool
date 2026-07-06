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

#include "concurrent_queue.h"

// A free function and a small type with a method, used to exercise async() with
// every callable shape (ported from Xapiand's oldtests threadpool test_pool_func,
// which the smoke test's single-lambda async case did not cover).
static int square_free(int i) { return i * i; }

struct Multiplier {
	int factor;
	int scale(int i) const { return factor * i; }
};

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

	// ---- 4. ConcurrentQueue::try_dequeue_bulk writes every slot. ----
	// Regression test: before the fix, try_dequeue_bulk wrote *itemFirst (the
	// same destination) every iteration instead of *itemFirst++, so only the
	// last output slot received a value and the rest stayed default/garbage.
	// Enqueue Q distinct values, bulk-dequeue them into a vector of size Q, and
	// assert every distinct value lands in its own consecutive slot.
	{
		constexpr int Q = 32;
		ConcurrentQueue<int> cq;
		for (int i = 0; i < Q; ++i) {
			bool ok = cq.enqueue(1000 + i);   // distinct, non-default values
			assert(ok);
		}

		std::vector<int> out(Q, -1);          // sentinel so unwritten slots show
		std::size_t got = cq.try_dequeue_bulk(out.begin(), out.size());
		assert(got == static_cast<std::size_t>(Q));

		// Every dequeued item must land in its own slot, in FIFO order.
		for (int i = 0; i < Q; ++i) {
			assert(out[i] == 1000 + i);       // fails pre-fix: only out[Q-1] set
		}
		assert(cq.empty());

		std::printf("threadpool OK: try_dequeue_bulk wrote all %d distinct slots\n", Q);
	}

	// ---- 5. async() accepts every callable shape, not just a plain lambda. ----
	// Ported from Xapiand's oldtests test_pool_func: a no-arg lambda, a lambda
	// with arguments, a free function, a member function (object + pointer), and
	// a lambda capturing an object all package correctly and resolve their future.
	{
		ThreadPool<std::function<void()>> pool("C{:02}", 3);
		Multiplier mul{10};
		int captured = 3;

		auto f_nullary = pool.async([] { return 42; });
		auto f_lambda  = pool.async([](int a, int b) { return a * b; }, 6, 7);
		auto f_free    = pool.async(&square_free, 9);
		auto f_member  = pool.async(&Multiplier::scale, &mul, 5);
		auto f_capture = pool.async([&captured](int i) { return captured + i; }, 4);

		assert(f_nullary.get() == 42);
		assert(f_lambda.get()  == 42);
		assert(f_free.get()    == 81);
		assert(f_member.get()  == 50);
		assert(f_capture.get() == 7);

		pool.finish();
		pool.join();
		std::printf("threadpool OK: async() dispatched nullary/lambda/free/member/captured callables\n");
	}

	std::printf("all threadpool tests passed\n");
	return 0;
}
