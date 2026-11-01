// User, kernel time, and switches per op, today's pool (bare, TaskGroup) against WorkPool
// (bare, WorkGroup), interleaved: a spun-on handoff, a burst of tiny items, an idle pool,
// and a 1 ms timed sleep (condvar wait_until against WorkPool::sleep_until).
// Built with -DTHREADPOOL_BUILD_BENCH=ON; run as kernel_time [trials] [threads] [rows], where
// threads defaults to the hardware's and rows picks some of handoff,burst,idle,sleep (all by
// default). The TaskGroup burst on many workers: kernel_time 5 16 burst (and 5 4 burst).
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "task_group.hh"
#include "threadpool.hh"
#include "work_pool.hh"

using namespace std::chrono_literals;

namespace {

struct Usage {
	double user_us = 0;
	double sys_us = 0;
	double switches = 0;
};

Usage usage_now() {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	auto us = [](const timeval& t) { return double(t.tv_sec) * 1e6 + double(t.tv_usec); };
	return {us(ru.ru_utime), us(ru.ru_stime), double(ru.ru_nvcsw + ru.ru_nivcsw)};
}

void report(const char* what, const char* pool, const Usage& a, const Usage& b, double ops, double wall_s) {
	std::printf("%-8s pool=%-10s ops=%-8.0f user_us_per_op=%-9.3f sys_us_per_op=%-9.3f switches_per_op=%-7.3f wall_s=%.3f\n",
		what, pool, ops, (b.user_us - a.user_us) / ops, (b.sys_us - a.sys_us) / ops, (b.switches - a.switches) / ops, wall_s);
	std::fflush(stdout);
}

// Posts one item and spins until it ran, `rounds` times; spinning keeps the producer off
// the kernel, so the counters are the pool's handoff.
template <typename Post>
void handoff(const char* pool, unsigned rounds, Post&& post) {
	std::atomic<std::uint32_t> done{0};
	auto a = usage_now();
	auto t0 = std::chrono::steady_clock::now();
	for (std::uint32_t i = 0; i < rounds; ++i) {
		post(done);
		while (done.load(std::memory_order_acquire) == i) {
			threadpool_detail::cpu_relax();
		}
		// A gap without clock reads (a VM's hpet makes them kernel time), so the worker parks
		// again and the next round is a real wake.
		for (int k = 0; k < 20'000; ++k) {
			threadpool_detail::cpu_relax();
		}
	}
	auto t1 = std::chrono::steady_clock::now();
	report("handoff", pool, a, usage_now(), rounds, std::chrono::duration<double>(t1 - t0).count());
}

template <typename Post, typename Drain>
void burst(const char* pool, unsigned items, Post&& post, Drain&& drain) {
	std::atomic<std::uint64_t> done{0};
	auto a = usage_now();
	auto t0 = std::chrono::steady_clock::now();
	for (unsigned i = 0; i < items; ++i) {
		post(done);
	}
	drain(done, items);
	auto t1 = std::chrono::steady_clock::now();
	report("burst", pool, a, usage_now(), items, std::chrono::duration<double>(t1 - t0).count());
}

void idle(const char* pool, double seconds) {
	auto a = usage_now();
	std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
	report("idle", pool, a, usage_now(), seconds, seconds);
}

struct Spin : WorkItem {
	std::atomic<std::uint32_t>* done = nullptr;
};

struct Count : WorkItem {
	std::atomic<std::uint64_t>* done = nullptr;
};

}  // namespace

int main(int argc, char** argv) {
	unsigned trials = argc > 1 ? unsigned(std::atoi(argv[1])) : 5;
	unsigned threads = argc > 2 && std::atoi(argv[2]) > 0 ? unsigned(std::atoi(argv[2])) : std::thread::hardware_concurrency();
	const char* rows = argc > 3 ? argv[3] : "handoff,burst,idle,sleep";
	auto want = [rows](const char* row) { return std::strstr(rows, row) != nullptr; };
	const unsigned rounds = 5'000;
	const unsigned items = 200'000;
	const unsigned sleeps = 2'000;
	std::printf("kernel_time: trials=%u threads=%u handoff=%u burst=%u sleeps=%u rows=%s\n", trials, threads, rounds, items, sleeps, rows);

	// Spins: a sleeping drain would add its own timed sleeps to the kernel time.
	auto spin_drain = [](std::atomic<std::uint64_t>& done, unsigned n) {
		while (done.load(std::memory_order_acquire) < n) {
			threadpool_detail::cpu_relax();
		}
	};

	for (unsigned trial = 0; trial < trials; ++trial) {
		{
			ThreadPool<> pool("T{:02}", threads);
			std::this_thread::sleep_for(50ms);
			if (want("handoff")) {
				handoff("today", rounds, [&](std::atomic<std::uint32_t>& d) { pool.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); });
			}
			if (want("burst")) {
				burst("today", items, [&](std::atomic<std::uint64_t>& d) { pool.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); }, spin_drain);
			}
			if (want("idle")) {
				idle("today", 2.0);
			}
			pool.finish();
			pool.join();
		}
		{
			ThreadPool<> pool("G{:02}", threads);
			TaskGroup group(pool);
			std::this_thread::sleep_for(50ms);
			if (want("handoff")) {
				handoff("today-tg", rounds, [&](std::atomic<std::uint32_t>& d) { group.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); });
			}
			if (want("burst")) {
				burst("today-tg", items, [&](std::atomic<std::uint64_t>& d) { group.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); }, spin_drain);
			}
			if (want("idle")) {
				idle("today-tg", 2.0);
			}
		}
		{
			WorkPool pool({.threads = threads});
			std::this_thread::sleep_for(50ms);
			Spin s;
			s.run = [](WorkItem* w) noexcept { static_cast<Spin*>(w)->done->fetch_add(1, std::memory_order_release); };
			if (want("handoff")) {
				handoff("workpool", rounds, [&](std::atomic<std::uint32_t>& d) { s.done = &d; pool.post(s); });
			}
			std::vector<Count> cs(items);
			unsigned next = 0;
			if (want("burst")) {
				burst("workpool", items, [&](std::atomic<std::uint64_t>& d) {
					auto& c = cs[next++];
					c.done = &d;
					c.run = [](WorkItem* w) noexcept { static_cast<Count*>(w)->done->fetch_add(1, std::memory_order_release); };
					pool.post(c);
				}, spin_drain);
			}
			if (want("idle")) {
				idle("workpool", 2.0);
			}
		}
		{
			WorkPool pool({.threads = threads});
			WorkGroup group(pool);
			std::this_thread::sleep_for(50ms);
			if (want("handoff")) {
				handoff("workgroup", rounds, [&](std::atomic<std::uint32_t>& d) { group.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); });
			}
			if (want("burst")) {
				burst("workgroup", items, [&](std::atomic<std::uint64_t>& d) { group.enqueue([&d] { d.fetch_add(1, std::memory_order_release); }); }, spin_drain);
			}
			if (want("idle")) {
				idle("workgroup", 2.0);
			}
		}
		if (!want("sleep")) {
			continue;
		}
		// The timed sleep a merge's rate limiter takes: today's pool_executor::sleep_until is
		// a condvar wait_until; WorkPool::sleep_until is one timed futex wait.
		{
			std::mutex mu;
			std::condition_variable cv;
			auto a = usage_now();
			auto t0 = std::chrono::steady_clock::now();
			for (unsigned i = 0; i < sleeps; ++i) {
				std::unique_lock<std::mutex> lk(mu);
				cv.wait_until(lk, std::chrono::steady_clock::now() + 1ms, [] { return false; });
			}
			auto t1 = std::chrono::steady_clock::now();
			report("sleep", "condvar", a, usage_now(), sleeps, std::chrono::duration<double>(t1 - t0).count());
		}
		{
			WorkPool pool({.threads = 1});
			WorkGroup group(pool);
			auto a = usage_now();
			auto t0 = std::chrono::steady_clock::now();
			for (unsigned i = 0; i < sleeps; ++i) {
				pool.sleep_until(std::chrono::steady_clock::now() + 1ms, group);
			}
			auto t1 = std::chrono::steady_clock::now();
			report("sleep", "workpool", a, usage_now(), sleeps, std::chrono::duration<double>(t1 - t0).count());
		}
	}
	return 0;
}
