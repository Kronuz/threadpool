// Tests for WorkPool: every item runs once, posts from workers and producers, help_until
// on a worker and off it, the draining destructor, idle CPU, and a ping-pong that hangs
// on a lost wakeup.
//
// Build: c++ -std=c++20 -I.. work_pool.cc ../thread.cc -o work_pool && ./work_pool
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "work_pool.hh"

namespace {

struct Counted : WorkItem {
	std::atomic<std::uint64_t>* count = nullptr;
};

void count_one(WorkItem* w) noexcept {
	static_cast<Counted*>(w)->count->fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t cpu_ns() {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	auto ns = [](const timeval& t) { return std::uint64_t(t.tv_sec) * 1'000'000'000u + std::uint64_t(t.tv_usec) * 1'000u; };
	return ns(ru.ru_utime) + ns(ru.ru_stime);
}

}  // namespace

int main() {
	// ---- 1. Every item from four producers runs exactly once. ----
	{
		constexpr unsigned producers = 4;
		constexpr unsigned per = 50'000;
		std::atomic<std::uint64_t> count{0};
		std::vector<Counted> items(producers * per);
		{
			WorkPool pool({.threads = 4});
			assert(pool.threads() == 4);
			assert(!pool.on_worker());
			std::vector<std::thread> threads;
			for (unsigned p = 0; p < producers; ++p) {
				threads.emplace_back([&, p] {
					for (unsigned i = 0; i < per; ++i) {
						auto& item = items[p * per + i];
						item.run = count_one;
						item.count = &count;
						pool.post(item);
					}
				});
			}
			for (auto& t : threads) {
				t.join();
			}
			pool.help_until([&] { return count.load() == producers * per; });
		}
		assert(count.load() == producers * per);
		std::printf("work_pool OK: %u items from %u producers ran once each\n", producers * per, producers);
	}

	// ---- 2. An item reposts itself from a worker; on_worker() holds there. ----
	{
		struct Chain : WorkItem {
			WorkPool* pool;
			std::atomic<unsigned> left{10'000};
			std::atomic<bool> on_worker{true};
			std::atomic<bool> done{false};
		};
		WorkPool pool({.threads = 3});
		Chain chain;
		chain.pool = &pool;
		chain.run = [](WorkItem* w) noexcept {
			auto* c = static_cast<Chain*>(w);
			if (!c->pool->on_worker()) {
				c->on_worker = false;
			}
			if (c->left.fetch_sub(1) > 1) {
				c->pool->post(*c);
			} else {
				c->done.store(true, std::memory_order_release);
			}
		};
		pool.post(chain);
		// Waits without helping, so every link runs on a worker.
		while (!chain.done.load(std::memory_order_acquire)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		assert(chain.on_worker.load());
		std::printf("work_pool OK: a chain of 10000 reposts ran on the workers\n");
	}

	// ---- 3. help_until inside an item on a one-worker pool runs the children itself. ----
	{
		struct Parent : WorkItem {
			WorkPool* pool;
			std::atomic<std::uint64_t> children{0};
			std::vector<Counted> kids;
			std::atomic<bool> done{false};
		};
		WorkPool pool({.threads = 1});
		Parent parent;
		parent.pool = &pool;
		parent.kids.resize(100);
		parent.run = [](WorkItem* w) noexcept {
			auto* p = static_cast<Parent*>(w);
			for (auto& k : p->kids) {
				k.run = count_one;
				k.count = &p->children;
				p->pool->post(k);
			}
			// The only worker is this one: without helping, this would deadlock.
			p->pool->help_until([p] { return p->children.load() == p->kids.size(); });
			p->done.store(true, std::memory_order_release);
		};
		pool.post(parent);
		while (!parent.done.load(std::memory_order_acquire)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::printf("work_pool OK: a nested wait on a one-worker pool helped instead of blocking\n");
	}

	// ---- 4. The destructor runs everything queued before it. ----
	{
		std::atomic<std::uint64_t> count{0};
		std::vector<Counted> items(20'000);
		{
			WorkPool pool({.threads = 2});
			for (auto& item : items) {
				item.run = count_one;
				item.count = &count;
				pool.post(item);
			}
		}
		assert(count.load() == items.size());
		std::printf("work_pool OK: the destructor drained %zu queued items\n", items.size());
	}

	// ---- 5. Ping-pong: one item at a time, waited on from outside; a lost wakeup hangs. ----
	{
		struct Ping : WorkItem {
			std::atomic<std::uint32_t> done{0};
		};
		WorkPool pool({.threads = 4});
		Ping ping;
		ping.run = [](WorkItem* w) noexcept {
			auto* p = static_cast<Ping*>(w);
			p->done.fetch_add(1, std::memory_order_release);
			p->done.notify_one();
		};
		constexpr std::uint32_t rounds = 20'000;
		for (std::uint32_t i = 0; i < rounds; ++i) {
			pool.post(ping);
			ping.done.wait(i, std::memory_order_acquire);
		}
		assert(ping.done.load() == rounds);
		std::printf("work_pool OK: %u ping-pong rounds, no lost wakeup\n", rounds);
	}

	// ---- 6. Idle workers sleep: a spinning pool of 4 would burn about 2 s of CPU here. ----
	{
		WorkPool pool({.threads = 4});
		std::atomic<std::uint64_t> count{0};
		std::vector<Counted> items(1'000);
		for (auto& item : items) {
			item.run = count_one;
			item.count = &count;
			pool.post(item);
		}
		pool.help_until([&] { return count.load() == items.size(); });
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		auto c0 = cpu_ns();
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		auto used = cpu_ns() - c0;
		std::printf("work_pool OK: 4 idle workers used %.3f ms of CPU in 500 ms\n", double(used) / 1e6);
		assert(used < 50'000'000);
	}

	// ---- 7. threads = 0 means the hardware's concurrency. ----
	{
		WorkPool pool;
		auto hw = std::thread::hardware_concurrency();
		assert(pool.threads() == (hw ? hw : 1));
	}

	std::printf("all work_pool tests passed\n");
	return 0;
}
