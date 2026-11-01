// Stress for TaskGroup on a 16-worker pool: bursts of tiny tasks, where a worker keeps its ticket
// while nothing else waits and a group lists one ticket at a time. Meant for TSan and ASan as well
// as a plain run (THREADPOOL_STRESS_SECONDS lengthens it).
//
//   - bursts from one producer and from several: every task runs exactly once, wait() returns
//   - a group of max_concurrency 1 runs its tasks in FIFO order and never two at once
//   - groups that share the pool with plain tasks and with each other: all accounted for
//   - cancel() during bursts: every task runs or is dropped, never both, never neither
//   - tasks that enqueue to their own group, and groups destroyed with tickets on the queue
//
// Uses CHECK, not assert: a Release build must verify too.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "task_group.hh"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

using namespace std::chrono_literals;
using Pool = ThreadPool<std::function<void()>>;

namespace {

constexpr int workers = 16;

// What the test is doing, for the watchdog: a hang must fail with its location, not stall a CI job.
std::atomic<const char*> stage{"start"};

void watchdog(double limit_seconds) {
	std::thread([limit_seconds] {
		auto until = std::chrono::steady_clock::now() + std::chrono::duration<double>(limit_seconds);
		while (std::chrono::steady_clock::now() < until) {
			std::this_thread::sleep_for(100ms);
		}
		std::fprintf(stderr, "task_group_stress: still running after %.0f s, in stage \"%s\"\n", limit_seconds, stage.load());
		std::abort();
	}).detach();
}

double stress_seconds() {
	const char* s = std::getenv("THREADPOOL_STRESS_SECONDS");
	return s ? std::atof(s) : 1.0;
}

std::chrono::steady_clock::time_point deadline(double seconds) {
	return std::chrono::steady_clock::now() +
		std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(seconds));
}

// At least two rounds, then until the deadline.
bool more(std::chrono::steady_clock::time_point until, int round) {
	return round < 2 || std::chrono::steady_clock::now() < until;
}

// A pool that counts the tickets groups put on it.
struct CountingPool {
	Pool& pool;
	std::atomic<std::size_t> posts{0};

	template <typename F>
	bool enqueue(F&& f) {
		posts.fetch_add(1, std::memory_order_relaxed);
		return pool.enqueue(std::forward<F>(f));
	}
	std::size_t threadpool_size() { return pool.threadpool_size(); }
	std::size_t size() { return pool.size(); }
	bool finished() { return pool.finished(); }
	bool ending() { return pool.ending(); }
};

// One producer, one group: bursts of n tiny tasks, each burst waited for.
void one_producer(Pool& pool, double seconds, int n) {
	stage = "one producer";
	CountingPool counting{pool};
	TaskGroup<CountingPool> group(counting);
	std::size_t tasks = 0;
	const auto until = deadline(seconds);
	for (int round = 0; more(until, round); ++round) {
		std::atomic<int> ran{0};
		for (int i = 0; i < n; ++i) {
			CHECK(group.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); }));
		}
		group.wait();
		CHECK(ran.load() == n);
		tasks += static_cast<std::size_t>(n);
	}
	std::printf("task_group_stress OK: one producer, %zu tasks in bursts of %d, %.3f tickets per task\n", tasks, n,
		static_cast<double>(counting.posts.load()) / static_cast<double>(tasks));
}

// Several producers into one group, and a group of max_concurrency 1 in FIFO order beside it.
void many_producers(Pool& pool, double seconds, int n) {
	stage = "many producers";
	constexpr int producers = 4;
	TaskGroup wide(pool);
	TaskGroup serial(pool, 1);
	std::size_t tasks = 0;
	const auto until = deadline(seconds);
	for (int round = 0; more(until, round); ++round) {
		std::atomic<int> ran{0};
		std::atomic<int> active{0};
		std::atomic<int> next{0};
		std::atomic<bool> in_order{true};
		std::vector<std::thread> threads;
		for (int p = 0; p < producers; ++p) {
			threads.emplace_back([&, p] {
				for (int i = 0; i < n; ++i) {
					CHECK(wide.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); }));
				}
				if (p == 0) {
					for (int i = 0; i < n; ++i) {
						CHECK(serial.enqueue([&, i] {
							if (active.fetch_add(1) != 0 || next.load(std::memory_order_relaxed) != i) {
								in_order = false;
							}
							next.store(i + 1, std::memory_order_relaxed);
							active.fetch_sub(1);
						}));
					}
				}
			});
		}
		for (auto& t : threads) {
			t.join();
		}
		wide.wait();
		serial.wait();
		CHECK(ran.load() == producers * n);
		CHECK(next.load() == n);
		CHECK(in_order.load());
		tasks += static_cast<std::size_t>((producers + 1) * n);
	}
	std::printf("task_group_stress OK: %d producers and a serial group, %zu tasks, FIFO kept\n", producers, tasks);
}

// Groups that share the pool with plain tasks and each other, with cancels in the middle.
void shared_and_cancelled(Pool& pool, double seconds, int n) {
	stage = "shared and cancelled";
	constexpr int groups = 3;
	std::size_t tasks = 0;
	const auto until = deadline(seconds);
	for (int round = 0; more(until, round); ++round) {
		std::vector<std::unique_ptr<TaskGroup<Pool>>> group(groups);
		std::vector<std::atomic<int>> ran(groups);
		std::vector<std::atomic<std::size_t>> dropped(groups);
		std::atomic<int> plain{0};
		for (int g = 0; g < groups; ++g) {
			group[g] = std::make_unique<TaskGroup<Pool>>(pool, g == 2 ? 3 : 0);
		}
		std::vector<std::thread> threads;
		for (int g = 0; g < groups; ++g) {
			threads.emplace_back([&, g] {
				for (int i = 0; i < n; ++i) {
					CHECK(group[g]->enqueue([&ran, g] { ran[g].fetch_add(1, std::memory_order_relaxed); }));
					if (g == 1 && i % 7 == 0) {
						pool.enqueue([&plain] { plain.fetch_add(1, std::memory_order_release); });
					}
					if (g == 0 && i % 500 == 0) {
						dropped[g].fetch_add(group[g]->cancel());
					}
				}
			});
		}
		for (auto& t : threads) {
			t.join();
		}
		// Group 0 was cancelled along the way and is cancelled once more; the others run every task.
		dropped[0].fetch_add(group[0]->cancel_and_wait());
		CHECK(ran[0].load() + dropped[0].load() == static_cast<std::size_t>(n));
		for (int g = 1; g < groups; ++g) {
			group[g]->wait();
			CHECK(ran[g].load() == n);
		}
		// The last plain task's increment is released to this acquire before `plain` goes away.
		const int plains = (n + 6) / 7;
		while (plain.load(std::memory_order_acquire) < plains) {
			std::this_thread::yield();
		}
		tasks += static_cast<std::size_t>(groups * n + plains);
	}
	std::printf("task_group_stress OK: groups beside plain tasks, with cancels, %zu tasks accounted for\n", tasks);
}

// Tasks that enqueue to their own group, and groups destroyed while their tickets are queued.
void chains_and_teardown(Pool& pool, double seconds, int n) {
	stage = "chains and teardown";
	std::size_t rounds = 0;
	const auto until = deadline(seconds);
	for (int round = 0; more(until, round); ++round) {
		auto count = std::make_shared<std::atomic<int>>(0);
		{
			TaskGroup group(pool);
			// Fan out: every task of the first n posts one more, from inside the group.
			std::function<void(int)> step = [&group, count, &step](int depth) {
				count->fetch_add(1, std::memory_order_relaxed);
				if (depth == 0) {
					CHECK(group.enqueue([&step] { step(1); }));
				}
			};
			for (int i = 0; i < n; ++i) {
				CHECK(group.enqueue([&step] { step(0); }));
			}
			group.wait();
			CHECK(count->load() == 2 * n);
			// Destroyed with a burst still queued: whatever has not started is dropped.
			for (int i = 0; i < n; ++i) {
				group.enqueue([count] { count->fetch_add(1, std::memory_order_relaxed); });
			}
		}
		CHECK(count->load() >= 2 * n && count->load() <= 3 * n);
		++rounds;
	}
	std::printf("task_group_stress OK: %zu rounds of self-posting chains and teardown under load\n", rounds);
}

}  // namespace

int main() {
	const double seconds = stress_seconds();
	watchdog(120 + 60 * seconds);
	// Bursts large enough that a group's tickets meet on the pool queue, small enough for TSan.
	const int n = 5000;
	{
		Pool pool("TS{:02}", workers);
		one_producer(pool, seconds, 4 * n);
		many_producers(pool, seconds, n);
		shared_and_cancelled(pool, seconds, n);
		chains_and_teardown(pool, seconds, n / 5);
		stage = "pool end";
		pool.end();
		CHECK(pool.join());
	}
	std::printf("all task_group_stress tests passed\n");
	return 0;
}
