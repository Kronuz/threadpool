// The bound: what this hardware and OS charge for the primitives a scheduler is made of.
// Single-threaded costs (clock, mutex, atomics, queue ops), cross-core line transfer, and
// the wake latency of a sleeping thread through a condition variable, C++20 atomic wait,
// and the raw futex. Run under a light hold (two threads at most).
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <sched.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif

#include "common.hh"
#include "futex.hh"
#include "queues.hh"

using namespace research;

namespace {

struct Item { Item* next = nullptr; };

template <typename F>
double per_op_ns(std::uint64_t iterations, F&& f) {
	auto t0 = now_ns();
	for (std::uint64_t i = 0; i < iterations; ++i) { f(); }
	auto t1 = now_ns();
	return double(t1 - t0) / double(iterations);
}

void report(const char* what, double ns) {
	std::printf("bound %-48s %10.1f ns\n", what, ns);
}

// Two threads alternate ownership of one line: one-way transfer is half a round trip.
void line_pingpong(std::uint64_t rounds) {
	alignas(64) std::atomic<std::uint32_t> flag{0};
	std::thread peer([&] {
		for (std::uint64_t i = 0; i < rounds; ++i) {
			while (flag.load(std::memory_order_acquire) != 2 * i + 1) { cpu_relax(); }
			flag.store(2 * i + 2, std::memory_order_release);
		}
	});
	auto t0 = now_ns();
	for (std::uint64_t i = 0; i < rounds; ++i) {
		flag.store(2 * i + 1, std::memory_order_release);
		while (flag.load(std::memory_order_acquire) != 2 * i + 2) { cpu_relax(); }
	}
	auto t1 = now_ns();
	peer.join();
	report("cross-core line round trip (spin, 2 threads)", double(t1 - t0) / double(rounds));
}

// Wake latency: the waiter publishes that it is about to block; the waker waits until it
// surely sleeps (100 µs), stamps the time, wakes it; the waiter stamps its own wake.
enum class Wake { condvar, atomic_wait, futex };

const char* wake_name(Wake w) {
	switch (w) {
		case Wake::condvar: return "std::condition_variable notify_one";
		case Wake::atomic_wait: return "std::atomic<uint32_t>::wait/notify_one";
		case Wake::futex: return RESEARCH_FUTEX " wake";
	}
	return "?";
}

void wake_latency(Wake kind, unsigned rounds) {
	alignas(64) std::atomic<std::uint32_t> word{0};
	alignas(64) std::atomic<std::uint32_t> about_to_sleep{0};
	alignas(64) std::atomic<std::uint64_t> stamp{0};
	std::mutex mu;
	std::condition_variable cv;
	std::vector<std::uint64_t> lat;
	lat.reserve(rounds);
	std::thread waiter([&] {
		for (unsigned i = 1; i <= rounds; ++i) {
			about_to_sleep.store(i, std::memory_order_release);
			switch (kind) {
				case Wake::condvar: {
					std::unique_lock<std::mutex> lk(mu);
					cv.wait(lk, [&] { return word.load(std::memory_order_acquire) >= i; });
					break;
				}
				case Wake::atomic_wait:
					while (word.load(std::memory_order_acquire) < i) { word.wait(i - 1, std::memory_order_acquire); }
					break;
				case Wake::futex:
					while (word.load(std::memory_order_acquire) < i) { futex_wait(&word, i - 1); }
					break;
			}
			auto t1 = now_ns();
			lat.push_back(t1 - stamp.load(std::memory_order_acquire));
		}
	});
	for (unsigned i = 1; i <= rounds; ++i) {
		while (about_to_sleep.load(std::memory_order_acquire) < i) { cpu_relax(); }
		burn_ns_clock(100'000);  // let it reach the kernel
		stamp.store(now_ns(), std::memory_order_release);
		switch (kind) {
			case Wake::condvar: {
				{ std::lock_guard<std::mutex> lk(mu); word.store(i, std::memory_order_release); }
				cv.notify_one();
				break;
			}
			case Wake::atomic_wait:
				word.store(i, std::memory_order_release);
				word.notify_one();
				break;
			case Wake::futex:
				word.store(i, std::memory_order_release);
				futex_wake_one(&word);
				break;
		}
	}
	waiter.join();
	char label[96];
	std::snprintf(label, sizeof label, "wake latency: %s", wake_name(kind));
	print_percentiles(label, lat);
}

// Both threads sleep between turns: the cost of a wake, a schedule, and a wake back.
void futex_roundtrip(unsigned rounds) {
	alignas(64) std::atomic<std::uint32_t> a{0};
	alignas(64) std::atomic<std::uint32_t> b{0};
	std::thread peer([&] {
		for (unsigned i = 1; i <= rounds; ++i) {
			while (a.load(std::memory_order_acquire) < i) { futex_wait(&a, i - 1); }
			b.store(i, std::memory_order_release);
			futex_wake_one(&b);
		}
	});
	auto t0 = now_ns();
	for (unsigned i = 1; i <= rounds; ++i) {
		a.store(i, std::memory_order_release);
		futex_wake_one(&a);
		while (b.load(std::memory_order_acquire) < i) { futex_wait(&b, i - 1); }
	}
	auto t1 = now_ns();
	peer.join();
	report("futex ping-pong round trip (both sleep)", double(t1 - t0) / double(rounds));
}

// One producer, one consumer, through each queue; the consumer spins. Throughput per item.
void spsc_handoff(std::uint64_t items) {
	std::vector<Item> pool(items);
	{
		std::mutex mu;
		std::deque<Item*> q;
		std::thread consumer([&] {
			std::uint64_t got = 0;
			while (got < items) {
				Item* x = nullptr;
				{
					std::lock_guard<std::mutex> lk(mu);
					if (!q.empty()) { x = q.front(); q.pop_front(); }
				}
				if (x) { ++got; } else { cpu_relax(); }
			}
		});
		auto t0 = now_ns();
		for (std::uint64_t i = 0; i < items; ++i) {
			std::lock_guard<std::mutex> lk(mu);
			q.push_back(&pool[i]);
		}
		consumer.join();
		auto t1 = now_ns();
		report("SPSC handoff: mutex + std::deque (today's queue)", double(t1 - t0) / double(items));
	}
	{
		auto q = std::make_unique<Mpmc<Item, 1 << 16>>();
		std::thread consumer([&] {
			std::uint64_t got = 0;
			while (got < items) {
				if (q->pop()) { ++got; } else { cpu_relax(); }
			}
		});
		auto t0 = now_ns();
		for (std::uint64_t i = 0; i < items; ++i) {
			while (!q->push(&pool[i])) { cpu_relax(); }
		}
		consumer.join();
		auto t1 = now_ns();
		report("SPSC handoff: bounded MPMC ring (Vyukov)", double(t1 - t0) / double(items));
	}
	{
		auto q = std::make_unique<ChaseLev<Item, 1 << 16>>();
		std::thread thief([&] {
			std::uint64_t got = 0;
			while (got < items) {
				if (q->steal()) { ++got; } else { cpu_relax(); }
			}
		});
		auto t0 = now_ns();
		for (std::uint64_t i = 0; i < items; ++i) {
			while (!q->push(&pool[i])) { cpu_relax(); }
		}
		thief.join();
		auto t1 = now_ns();
		report("SPSC handoff: Chase-Lev push / steal", double(t1 - t0) / double(items));
	}
}

// Four producers, four consumers: contention on one shared queue.
void mpmc_contended(std::uint64_t items_per_producer, unsigned producers) {
	std::vector<Item> pool(items_per_producer * producers);
	const std::uint64_t total = items_per_producer * producers;
	{
		std::mutex mu;
		std::deque<Item*> q;
		std::atomic<std::uint64_t> got{0};
		std::vector<std::thread> ts;
		auto t0 = now_ns();
		for (unsigned p = 0; p < producers; ++p) {
			ts.emplace_back([&, p] {
				for (std::uint64_t i = 0; i < items_per_producer; ++i) {
					std::lock_guard<std::mutex> lk(mu);
					q.push_back(&pool[p * items_per_producer + i]);
				}
			});
		}
		for (unsigned c = 0; c < producers; ++c) {
			ts.emplace_back([&] {
				for (;;) {
					Item* x = nullptr;
					{
						std::lock_guard<std::mutex> lk(mu);
						if (!q.empty()) { x = q.front(); q.pop_front(); }
					}
					if (x) {
						if (got.fetch_add(1, std::memory_order_relaxed) + 1 >= total) { return; }
					} else if (got.load(std::memory_order_relaxed) >= total) {
						return;
					} else {
						cpu_relax();
					}
				}
			});
		}
		for (auto& t : ts) { t.join(); }
		auto t1 = now_ns();
		report("MPMC 4x4 contended: mutex + std::deque", double(t1 - t0) / double(total));
	}
	{
		auto q = std::make_unique<Mpmc<Item, 1 << 16>>();
		std::atomic<std::uint64_t> got{0};
		std::vector<std::thread> ts;
		auto t0 = now_ns();
		for (unsigned p = 0; p < producers; ++p) {
			ts.emplace_back([&, p] {
				for (std::uint64_t i = 0; i < items_per_producer; ++i) {
					while (!q->push(&pool[p * items_per_producer + i])) { cpu_relax(); }
				}
			});
		}
		for (unsigned c = 0; c < producers; ++c) {
			ts.emplace_back([&] {
				for (;;) {
					if (q->pop()) {
						if (got.fetch_add(1, std::memory_order_relaxed) + 1 >= total) { return; }
					} else if (got.load(std::memory_order_relaxed) >= total) {
						return;
					} else {
						cpu_relax();
					}
				}
			});
		}
		for (auto& t : ts) { t.join(); }
		auto t1 = now_ns();
		report("MPMC 4x4 contended: bounded MPMC ring (Vyukov)", double(t1 - t0) / double(total));
	}
}

}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 5'000'000;
	const unsigned wake_rounds = env_unsigned("WAKE_ROUNDS", 3000);
	std::printf("bound: %u hardware threads, futex = %s, %llu iterations\n", hardware_threads(), RESEARCH_FUTEX, static_cast<unsigned long long>(n));
	std::printf("bound: churn rate %.3f iterations/ns\n", churn_per_ns());

	report("steady_clock::now()", per_op_ns(n, [] { sink.fetch_add(now_ns() & 1, std::memory_order_relaxed); }));
	{
		std::mutex mu;
		report("std::mutex lock + unlock, uncontended", per_op_ns(n, [&] { mu.lock(); mu.unlock(); }));
	}
	{
		alignas(64) std::atomic<std::uint64_t> a{0};
		report("atomic fetch_add relaxed, one thread", per_op_ns(n, [&] { a.fetch_add(1, std::memory_order_relaxed); }));
		report("atomic fetch_add seq_cst, one thread", per_op_ns(n, [&] { a.fetch_add(1, std::memory_order_seq_cst); }));
		std::uint64_t e = 0;
		report("atomic CAS (success), one thread", per_op_ns(n, [&] { a.compare_exchange_strong(e, e + 1, std::memory_order_acq_rel); e = a.load(std::memory_order_relaxed); }));
		report("atomic_thread_fence(seq_cst)", per_op_ns(n, [] { std::atomic_thread_fence(std::memory_order_seq_cst); }));
	}
	{
		std::vector<Item> pool(1 << 16);
		std::mutex mu;
		std::deque<Item*> q;
		report("mutex + std::deque push_back + pop_front", per_op_ns(n, [&] {
			{ std::lock_guard<std::mutex> lk(mu); q.push_back(&pool[0]); }
			{ std::lock_guard<std::mutex> lk(mu); q.pop_front(); }
		}));
		auto cl = std::make_unique<ChaseLev<Item, 1 << 16>>();
		report("Chase-Lev push + pop (owner, LIFO)", per_op_ns(n, [&] { cl->push(&pool[0]); sink.fetch_add(cl->pop() != nullptr, std::memory_order_relaxed); }));
		report("Chase-Lev push + steal (same thread, FIFO)", per_op_ns(n, [&] { cl->push(&pool[0]); sink.fetch_add(cl->steal() != nullptr, std::memory_order_relaxed); }));
		auto mq = std::make_unique<Mpmc<Item, 1 << 16>>();
		report("bounded MPMC ring push + pop, one thread", per_op_ns(n, [&] { mq->push(&pool[0]); sink.fetch_add(mq->pop() != nullptr, std::memory_order_relaxed); }));
	}
	{
		std::function<void()> f;
		report("std::function assign (24-byte capture) + call", per_op_ns(n, [&] {
			std::uint64_t a = 1, b = 2, c = 3;
			f = [a, b, c] { sink.fetch_add(a + b + c, std::memory_order_relaxed); };
			f();
		}));
		report("make_unique<std::function> (TaskGroup's box)", per_op_ns(n, [&] {
			auto p = std::make_unique<std::function<void()>>([] { sink.fetch_add(1, std::memory_order_relaxed); });
			(*p)();
		}));
	}
	report("sched_yield()", per_op_ns(n / 10, [] { sched_yield(); }));
#if defined(__linux__)
	report("syscall floor (getppid)", per_op_ns(n / 10, [] { syscall(SYS_getppid); }));
#else
	report("syscall floor (getppid)", per_op_ns(n / 10, [] { sink.fetch_add(getppid() & 1, std::memory_order_relaxed); }));
#endif

	line_pingpong(n / 5);
	futex_roundtrip(wake_rounds);
	wake_latency(Wake::condvar, wake_rounds);
	wake_latency(Wake::atomic_wait, wake_rounds);
	wake_latency(Wake::futex, wake_rounds);
	spsc_handoff(n);
	mpmc_contended(n / 4, 4);
	return 0;
}
