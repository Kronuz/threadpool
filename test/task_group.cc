// Tests for TaskGroup: cancel, wait, close, the destructor, stop tokens,
// posting from a task, the concurrency cap, and round-robin across groups.
// Every ordering is forced with promises or a single-thread pool; no sleeps.
//
// Build: c++ -std=c++20 -I.. task_group.cc ../thread.cc -o task_group && ./task_group
#include <atomic>
#include <cassert>
#include <cstdio>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "task_group.hh"

using Pool = ThreadPool<std::function<void()>>;

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

	std::printf("all task_group tests passed\n");
	return 0;
}
