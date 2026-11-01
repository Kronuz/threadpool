// Tests for TaskGroup: cancel, wait, close, the destructor, stop tokens,
// posting from a task, the concurrency cap, round-robin across groups, and how
// many tickets a burst puts on the pool queue.
// Every ordering is forced with promises or a single-thread pool; no sleeps.
//
// Build: c++ -std=c++20 -I.. task_group.cc ../thread.cc -o task_group && ./task_group
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "task_group.hh"

using Pool = ThreadPool<std::function<void()>>;

// Checked in every build type, unlike assert.
#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

// A pool that counts the tickets a group puts on it: TaskGroup takes any pool of this shape.
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

// Occupies every worker of the pool with a task that waits for the returned promise.
std::promise<void> occupy(Pool& pool, int workers) {
	std::promise<void> gate;
	auto gate_future = gate.get_future().share();
	auto busy = std::make_shared<std::atomic<int>>(0);
	for (int i = 0; i < workers; ++i) {
		pool.enqueue([gate_future, busy] {
			busy->fetch_add(1);
			gate_future.wait();
		});
	}
	while (busy->load() < workers) {
		std::this_thread::yield();
	}
	return gate;
}

int main() {
	// ---- 1. cancel() drops exactly the tasks that have not started. ----
	{
		Pool pool("TG{:02}", 4);
		TaskGroup group(pool, 1);  // one at a time: the rest stay queued
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		std::atomic<bool> first_done{false};
		std::atomic<int> ran{0};
		auto probe = std::make_shared<int>(0);

		bool ok = group.enqueue([&started, release_future, &first_done] {
			started.set_value();
			release_future.wait();
			first_done = true;
		});
		assert(ok);
		for (int i = 0; i < 10; ++i) {
			group.enqueue([probe, &ran] { ran.fetch_add(1); });
		}
		started_future.wait();

		std::size_t dropped = group.cancel();
		assert(dropped == 10);
		// The dropped tasks' captures were destroyed inside cancel().
		assert(probe.use_count() == 1);

		release.set_value();
		group.wait();
		assert(first_done);
		assert(ran.load() == 0);

		// Tasks queued after cancel() run normally.
		group.enqueue([&ran] { ran.fetch_add(1); });
		group.wait();
		assert(ran.load() == 1);
		std::printf("task_group OK: cancel() dropped %zu queued tasks, the running one finished\n", dropped);
	}

	// ---- 2. wait() returns only after the running tasks finish. ----
	{
		Pool pool("TW{:02}", 2);
		TaskGroup group(pool);
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		std::atomic<bool> done{false};

		group.enqueue([&started, release_future, &done] {
			started.set_value();
			release_future.wait();
			done = true;
		});
		started_future.wait();
		std::thread releaser([&release] { release.set_value(); });
		group.wait();
		assert(done);
		releaser.join();
		std::printf("task_group OK: wait() returned after the running task finished\n");
	}

	// ---- 3. The destructor closes, cancels, stops, and waits for a running task. ----
	{
		Pool pool("TD{:02}", 2);
		std::promise<void> started;
		auto started_future = started.get_future();
		std::atomic<bool> done{false};
		std::atomic<int> late{0};
		{
			TaskGroup group(pool, 1);
			group.enqueue([&started, &done](const TaskGroupStopToken& stop) {
				started.set_value();
				while (!stop.stop_requested()) {
					std::this_thread::yield();
				}
				done = true;
			});
			for (int i = 0; i < 5; ++i) {
				group.enqueue([&late] { late.fetch_add(1); });
			}
			started_future.wait();
		}
		assert(done);
		assert(late.load() == 0);
		std::printf("task_group OK: destructor stopped and waited for the running task\n");
	}

	// ---- 4. A running task observes cancel(); later tasks are not stopped. ----
	{
		Pool pool("TS{:02}", 2);
		TaskGroup group(pool);
		std::promise<void> started;
		auto started_future = started.get_future();
		std::atomic<bool> observed{false};
		std::atomic<bool> fresh{false};

		group.enqueue([&started, &observed](const TaskGroupStopToken& stop) {
			assert(!stop.stop_requested());
			started.set_value();
			while (!stop.stop_requested()) {
				std::this_thread::yield();
			}
			observed = true;
		});
		started_future.wait();
		group.cancel();
		group.wait();
		assert(observed);

		group.enqueue([&fresh](const TaskGroupStopToken& stop) {
			fresh = !stop.stop_requested();
		});
		group.wait();
		assert(fresh);
		std::printf("task_group OK: a running task observed cancel() through its stop token\n");
	}

	// ---- 5. A task posts to its own group; wait() covers the whole chain. ----
	{
		Pool pool("TP{:02}", 3);
		TaskGroup group(pool);
		constexpr int depth = 100;
		std::atomic<int> count{0};
		std::function<void()> step;
		step = [&group, &count, &step] {
			if (count.fetch_add(1) + 1 < depth) {
				bool ok = group.enqueue(step);
				assert(ok);
				(void)ok;
			}
		};
		group.enqueue(step);
		group.wait();
		assert(count.load() == depth);
		std::printf("task_group OK: a %d-task chain posted from within its own tasks\n", depth);
	}

	// ---- 6. close() rejects new tasks; queued ones still run. ----
	{
		Pool pool("TC{:02}", 2);
		TaskGroup group(pool);
		std::atomic<int> ran{0};
		for (int i = 0; i < 10; ++i) {
			group.enqueue([&ran] { ran.fetch_add(1); });
		}
		group.close();
		assert(group.closed());
		bool ok = group.enqueue([&ran] { ran.fetch_add(100); });
		assert(!ok);
		group.wait();
		assert(ran.load() == 10);
		std::printf("task_group OK: close() rejected a late task, queued tasks ran\n");
	}

	// ---- 7. Round-robin: one queued task is not starved by a thousand. ----
	{
		std::promise<void> gate;
		auto gate_future = gate.get_future().share();
		Pool pool("TR{:02}", 1);
		TaskGroup big(pool);
		TaskGroup lone(pool);
		// Hold the only worker so both groups queue up before anything runs.
		pool.enqueue([gate_future] { gate_future.wait(); });

		std::atomic<int> seq{0};
		std::atomic<int> lone_pos{-1};
		for (int i = 0; i < 1000; ++i) {
			big.enqueue([&seq] { seq.fetch_add(1); });
		}
		lone.enqueue([&seq, &lone_pos] { lone_pos = seq.fetch_add(1); });
		gate.set_value();
		big.wait();
		lone.wait();
		assert(seq.load() == 1001);
		assert(lone_pos.load() == 1);
		std::printf("task_group OK: the lone task ran at position %d of 1001\n", lone_pos.load());
	}

	// ---- 8. Round-robin: two busy groups alternate strictly, FIFO within each. ----
	{
		std::promise<void> gate;
		auto gate_future = gate.get_future().share();
		Pool pool("TA{:02}", 1);
		TaskGroup a(pool);
		TaskGroup b(pool);
		pool.enqueue([gate_future] { gate_future.wait(); });

		constexpr int n = 100;
		std::vector<int> order;  // written by the single worker only
		order.reserve(2 * n);
		for (int i = 0; i < n; ++i) {
			a.enqueue([&order, i] { order.push_back(i); });
			b.enqueue([&order, i] { order.push_back(1000 + i); });
		}
		gate.set_value();
		a.wait();
		b.wait();
		assert(order.size() == static_cast<std::size_t>(2 * n));
		for (int i = 0; i < n; ++i) {
			assert(order[2 * i] == i);
			assert(order[2 * i + 1] == 1000 + i);
		}
		std::printf("task_group OK: two groups alternated over %d tasks\n", 2 * n);
	}

	// ---- 9. max_concurrency caps the group's parallelism. ----
	{
		Pool pool("TM{:02}", 4);
		TaskGroup group(pool, 1);
		std::atomic<int> active{0};
		std::atomic<int> peak{0};
		for (int i = 0; i < 200; ++i) {
			group.enqueue([&active, &peak] {
				int now = active.fetch_add(1) + 1;
				int seen = peak.load();
				while (now > seen && !peak.compare_exchange_weak(seen, now)) {}
				active.fetch_sub(1);
			});
		}
		group.wait();
		assert(peak.load() == 1);
		std::printf("task_group OK: max_concurrency 1 kept the group serial\n");
	}

	// ---- 10. Concurrent producers and cancels: every task runs or is dropped. ----
	{
		Pool pool("TX{:02}", 4);
		constexpr int groups = 3;
		constexpr int per_group = 2000;
		std::vector<std::unique_ptr<TaskGroup<Pool>>> group(groups);
		std::vector<std::atomic<int>> ran(groups);
		std::vector<std::atomic<std::size_t>> dropped(groups);
		for (int g = 0; g < groups; ++g) {
			group[g] = std::make_unique<TaskGroup<Pool>>(pool);
		}
		std::vector<std::thread> producers;
		for (int g = 0; g < groups; ++g) {
			producers.emplace_back([&, g] {
				for (int i = 0; i < per_group; ++i) {
					group[g]->enqueue([&ran, g] { ran[g].fetch_add(1); });
					if (g == 0 && i % 250 == 0) {
						dropped[g].fetch_add(group[g]->cancel());
					}
				}
			});
		}
		for (auto& producer : producers) {
			producer.join();
		}
		for (int g = 0; g < groups; ++g) {
			dropped[g].fetch_add(group[g]->cancel_and_wait());
			assert(ran[g].load() + dropped[g].load() == static_cast<std::size_t>(per_group));
		}
		group.clear();
		std::printf("task_group OK: %d groups x %d tasks under concurrent cancels all accounted for\n",
		            groups, per_group);
	}

	// ---- 11. A throwing task is contained; the group keeps going. ----
	{
		Pool pool("TT{:02}", 1);
		TaskGroup group(pool);
		std::atomic<bool> after{false};
		group.enqueue([] { throw std::runtime_error("task failure"); });
		group.enqueue([&after] { after = true; });
		group.wait();
		assert(after);
		std::printf("task_group OK: a throwing task did not stall its group\n");
	}

	// ---- 12. A burst puts one ticket per worker on the pool queue, not one per task. ----
	{
		constexpr int workers = 4;
		constexpr int n = 1000;
		Pool pool("TB{:02}", workers);
		CountingPool counting{pool};
		TaskGroup<CountingPool> group(counting);
		auto gate = occupy(pool, workers);
		std::atomic<int> ran{0};
		for (int i = 0; i < n; ++i) {
			group.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
		}
		// No worker has taken the first ticket, and it reaches every queued task: one is enough.
		CHECK(counting.posts.load() == 1);
		gate.set_value();
		group.wait();
		CHECK(ran.load() == n);
		// Each worker that takes a ticket lists one more (up to the pool's size) and runs the
		// group's tasks while nothing else waits; a trip through the pool per task would be n.
		const auto posts = counting.posts.load();
		CHECK(posts < static_cast<std::size_t>(n / 10));
		// The tickets retired with the burst: the next one lists a ticket again, one.
		gate = occupy(pool, workers);
		for (int i = 0; i < n; ++i) {
			group.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
		}
		CHECK(counting.posts.load() == posts + 1);
		gate.set_value();
		group.wait();
		CHECK(ran.load() == 2 * n);
		std::printf("task_group OK: %d tasks on %d workers took %zu tickets\n", n, workers, posts);
	}

	// ---- 13. A group widens to its cap one hand-off at a time, even from busy workers. ----
	{
		constexpr int workers = 4;
		Pool pool("TN{:02}", workers);
		for (int cap : {workers, 2}) {
			TaskGroup group(pool, cap == workers ? 0 : cap);
			auto gate = occupy(pool, workers);
			// Each task waits until `cap` of them run at once (for 10 s at most, in all), so a group
			// that does not widen as the workers come free fails here instead of hanging.
			std::atomic<int> active{0};
			std::atomic<int> peak{0};
			std::atomic<int> started{0};
			const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
			for (int i = 0; i < 3 * cap; ++i) {
				group.enqueue([&, cap] {
					const int now = active.fetch_add(1) + 1;
					int seen = peak.load();
					while (now > seen && !peak.compare_exchange_weak(seen, now)) {}
					started.fetch_add(1);
					while (peak.load() < cap && std::chrono::steady_clock::now() < until) {
						std::this_thread::yield();
					}
					active.fetch_sub(1);
				});
			}
			gate.set_value();
			group.wait();
			CHECK(started.load() == 3 * cap);
			CHECK(peak.load() == cap);
		}
		std::printf("task_group OK: a group of waiting tasks widened to the pool and to its cap\n");
	}

	// ---- 14. A worker keeps its ticket only while nothing else waits on the pool queue. ----
	{
		// One worker, one group with many tasks and plain pool tasks posted from inside the
		// group's tasks: each plain task runs right after the group task that posted it.
		Pool pool("TY{:02}", 1);
		TaskGroup group(pool);
		constexpr int n = 50;
		std::vector<int> order;  // written by the single worker only
		order.reserve(2 * n);
		auto gate = occupy(pool, 1);
		for (int i = 0; i < n; ++i) {
			group.enqueue([&order, &pool, i] {
				order.push_back(i);
				if (i % 5 == 0) {
					pool.enqueue([&order, i] { order.push_back(1000 + i); });
				}
			});
		}
		gate.set_value();
		group.wait();
		pool.end();
		pool.join();
		std::vector<int> expected;
		for (int i = 0; i < n; ++i) {
			expected.push_back(i);
			if (i % 5 == 0) {
				expected.push_back(1000 + i);
			}
		}
		CHECK(order == expected);
		std::printf("task_group OK: plain pool tasks interleaved with a draining group in FIFO order\n");
	}

	std::printf("all task_group tests passed\n");
	return 0;
}
