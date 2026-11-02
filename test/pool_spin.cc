// The consumer that polls the queue before it sleeps (PoolQueue::spin), with a long poll so the
// checks do not depend on the host's speed:
//
//   - a trickle of tasks a few microseconds apart is served without a wakeup per task
//   - tasks queued while a consumer polls (the one it waits for goes without a signal) wake the
//     sleepers they need: three blocking tasks start at once, none waits for a sleeper's timeout
//   - once a poll gives up, a task is signaled again: none is found by a sleeper's timeout
//
// Uses CHECK, not assert: a Release build must verify too.
#define THREADPOOL_QUEUE_STATS 1
#define THREADPOOL_SPIN_NS 50000000  // 50 ms: hot after any prompt wake, and polls that long

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <thread>

#include "threadpool.hh"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

void pause_for(std::chrono::nanoseconds d) {
	const auto until = Clock::now() + d;
	while (Clock::now() < until) {
	}
}

// One producer, a task every 5 us, two workers: the worker that ran the last task polls for the
// next one, so the producer signals (almost) nobody. Without the poll every task wakes a worker.
void trickle() {
	ThreadPool<> pool("SP{:02}", 2);
	std::this_thread::sleep_for(20ms);  // both workers asleep
	constexpr int n = 2000;
	std::atomic<int> ran{0};
	pool.enqueue([&ran] { ran.fetch_add(1); });  // the first wake is prompt: the queue turns hot
	while (ran.load() < 1) {
	}
	const auto before = pool.wakeups();
	for (int i = 0; i < n; ++i) {
		pool.enqueue([&ran] { ran.fetch_add(1); });
		pause_for(5us);
	}
	while (ran.load() < n + 1) {
		std::this_thread::yield();
	}
	const auto woken = pool.wakeups() - before;
	CHECK(woken <= static_cast<std::size_t>(n / 2));
	CHECK(pool.lost_wakeups() == 0);
	std::printf("pool_spin OK: %d tasks 5 us apart took %zu wakeups\n", n, woken);
	pool.end();
	pool.join();
}

// While one worker polls, three tasks that block until all three run are queued at once: the
// poller takes one and wakes two sleepers for the others, where a lost signal would leave them
// queued behind a blocked worker until a sleeper's 100 ms timeout.
void burst_while_polling() {
	ThreadPool<> pool("SB{:02}", 3);
	std::this_thread::sleep_for(20ms);
	std::atomic<int> ran{0};
	for (int i = 0; i < 20; ++i) {  // turn the queue hot: one worker now polls, two sleep
		pool.enqueue([&ran] { ran.fetch_add(1); });
		while (ran.load() < i + 1) {
		}
	}
	std::this_thread::sleep_for(1ms);
	std::atomic<int> started{0};
	const auto t0 = Clock::now();
	const auto until = t0 + 2s;
	for (int i = 0; i < 3; ++i) {
		pool.enqueue([&started, until] {
			started.fetch_add(1);
			while (started.load() < 3 && Clock::now() < until) {
				std::this_thread::yield();
			}
		});
	}
	while (started.load() < 3 && Clock::now() < until) {
		std::this_thread::yield();
	}
	const auto took = Clock::now() - t0;
	CHECK(started.load() == 3);
	CHECK(took < 50ms);
	std::printf("pool_spin OK: three blocking tasks queued during a poll all started in %lld us\n",
		static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(took).count()));
	pool.end();
	pool.join();
}

// A trickle makes a consumer poll; after it gives up (50 ms with nothing) and every worker
// sleeps, single tasks must be signaled again, not left for a sleeper's 100 ms timeout.
void cold_after_poll() {
	ThreadPool<> pool("SC{:02}", 2);
	std::atomic<int> ran{0};
	for (int i = 0; i < 200; ++i) {
		pool.enqueue([&ran] { ran.fetch_add(1); });
		pause_for(5us);
	}
	while (ran.load() < 200) {
		std::this_thread::yield();
	}
	std::this_thread::sleep_for(120ms);
	const auto lost = pool.lost_wakeups();
	for (int i = 0; i < 3; ++i) {
		const auto t0 = Clock::now();
		pool.enqueue([&ran] { ran.fetch_add(1); });
		while (ran.load() < 201 + i) {
			std::this_thread::yield();
		}
		CHECK(Clock::now() - t0 < 50ms);
		std::this_thread::sleep_for(20ms);
	}
	CHECK(pool.lost_wakeups() == lost);
	std::printf("pool_spin OK: after the poll gave up, three single tasks were signaled\n");
	pool.end();
	pool.join();
}

}  // namespace

int main() {
	trickle();
	burst_while_polling();
	cold_after_poll();
	std::printf("all pool_spin tests passed\n");
	return 0;
}
