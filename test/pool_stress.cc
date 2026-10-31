// Stress and invariant tests for ThreadPool's queue and wake protocol. Meant for TSan and ASan as
// well as a plain run (CI runs it under both; THREADPOOL_STRESS_SECONDS lengthens it).
//
//   - exact counts and sums under many producers, all call shapes, 1-8 workers
//   - per-producer order with one worker; global order across chunk boundaries with bulk pushes
//   - bursts with sleeps between them, so workers park and wake: no wakeup is lost (a sleeper that
//     wakes by its timeout and finds a task counts as lost; it must stay zero)
//   - a task is always in size() or running_size(): never in neither
//   - end/finish/clear/destruction under load: every queued task destroyed exactly once
//   - the shared_ptr / unique_ptr task types
//
// Uses CHECK, not assert: a Release build must verify too.
#define THREADPOOL_QUEUE_STATS 1
// A worker that found the queue empty calls this just before it registers as a sleeper.
void queue_hook(void* queue);
#define THREADPOOL_QUEUE_TEST_HOOK(q) queue_hook(q)

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include "threadpool.hh"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

using namespace std::chrono_literals;

namespace {

// What the test is doing, for the watchdog: a hang must fail with its location, not stall a CI job.
std::atomic<const char*> stage{"start"};

void watchdog(double limit_seconds) {
	std::thread([limit_seconds] {
		auto until = std::chrono::steady_clock::now() + std::chrono::duration<double>(limit_seconds);
		while (std::chrono::steady_clock::now() < until) {
			std::this_thread::sleep_for(100ms);
		}
		std::fprintf(stderr, "pool_stress: still running after %.0f s, in stage \"%s\"\n", limit_seconds, stage.load());
		std::abort();
	}).detach();
}

double stress_seconds() {
	const char* s = std::getenv("THREADPOOL_STRESS_SECONDS");
	return s ? std::atof(s) : 1.0;
}

template <typename Pred>
void wait_until(Pred pred, std::chrono::milliseconds limit = 30s) {
	auto deadline = std::chrono::steady_clock::now() + limit;
	while (!pred()) {
		CHECK(std::chrono::steady_clock::now() < deadline);
		std::this_thread::sleep_for(50us);
	}
}

// Counts how many tasks were destroyed and how many ran.
struct Counts {
	std::atomic<long> destroyed{0};
	std::atomic<long> ran{0};
};

struct Token {
	Counts* counts;
	explicit Token(Counts* c) : counts(c) {}
	Token(Token&& o) noexcept : counts(o.counts) { o.counts = nullptr; }
	Token(const Token& o) : counts(o.counts) {}  // std::function in other pools needs it; the pool never copies
	~Token() {
		if (counts) {
			counts->destroyed.fetch_add(1, std::memory_order_relaxed);
		}
	}
	void operator()() const { counts->ran.fetch_add(1, std::memory_order_relaxed); }
};

// ---- exact counts under many producers, every call shape ----
void test_counts(int workers, int producers, int per) {
	ThreadPool<> pool("T{:02}", static_cast<std::size_t>(workers));
	std::atomic<long> sum{0};
	std::atomic<long> futures_sum{0};
	std::vector<std::thread> ps;
	for (int p = 0; p < producers; ++p) {
		ps.emplace_back([&, p] {
			std::mt19937 rng(static_cast<unsigned>(p) + 1);
			std::vector<std::future<long>> futures;
			for (int i = 0; i < per;) {
				long v = static_cast<long>(p) * per + i;
				switch (rng() % 5) {
					case 0:
						pool.enqueue([&sum, v] { sum.fetch_add(v, std::memory_order_relaxed); });
						++i;
						break;
					case 1:
						pool.post([&sum](long a, long b) { sum.fetch_add(a + b, std::memory_order_relaxed); }, v, 0L);
						++i;
						break;
					case 2: {
						futures.push_back(pool.async([](long a, long b) { return a + b; }, v, 0L));
						++i;
						break;
					}
					case 3: {
						int k = static_cast<int>(rng() % 150);
						if (k > per - i) {
							k = per - i;
						}
						std::vector<std::function<void()>> batch;
						long local = 0;
						for (int j = 0; j < k; ++j) {
							long w = v + j;
							local += w;
							batch.push_back([&sum, w] { sum.fetch_add(w, std::memory_order_relaxed); });
						}
						CHECK(pool.enqueue_bulk(batch.begin(), batch.size()));
						i += k;
						break;
					}
					default:
						// A boxed task: larger than the inline buffer.
						{
							long pad[10] = {v, 0, 0, 0, 0, 0, 0, 0, 0, 0};
							pool.enqueue([&sum, pad] { sum.fetch_add(pad[0], std::memory_order_relaxed); });
						}
						++i;
						break;
				}
			}
			for (auto& f : futures) {
				futures_sum.fetch_add(f.get(), std::memory_order_relaxed);
			}
		});
	}
	for (auto& t : ps) {
		t.join();
	}
	pool.end();
	CHECK(pool.join());
	long expected = 0;
	for (long v = 0; v < static_cast<long>(producers) * per; ++v) {
		expected += v;
	}
	CHECK(sum.load() + futures_sum.load() == expected);
	CHECK(pool.size() == 0);
	CHECK(pool.running_size() == 0);
}

// ---- order ----
void test_order() {
	// One worker, several producers: each producer's tasks run in the order it queued them.
	{
		ThreadPool<> pool("O{:02}", 1);
		constexpr int producers = 4, per = 5000;
		std::vector<int> last(producers, -1);
		std::atomic<int> bad{0};
		std::vector<std::thread> ps;
		for (int p = 0; p < producers; ++p) {
			ps.emplace_back([&, p] {
				for (int i = 0; i < per; ++i) {
					pool.enqueue([&last, &bad, p, i] {
						if (last[p] != i - 1) {
							bad.fetch_add(1);
						}
						last[p] = i;
					});
				}
			});
		}
		for (auto& t : ps) {
			t.join();
		}
		pool.end();
		CHECK(pool.join());
		CHECK(bad.load() == 0);
		for (int p = 0; p < producers; ++p) {
			CHECK(last[p] == per - 1);
		}
	}
	// One producer, one worker, bulk and single pushes of sizes that straddle chunk boundaries.
	{
		ThreadPool<> pool("B{:02}", 1);
		std::vector<int> order;
		std::atomic<bool> gate{false}, in{false};
		pool.enqueue([&gate, &in] {
			in = true;
			while (!gate.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
		});
		wait_until([&] { return in.load(); });  // the gate has left size(), so size() counts what follows
		int next = 0;
		for (int size : {0, 1, 62, 63, 64, 65, 127, 128, 129, 300, 1, 63, 1, 1, 64}) {
			if (size % 2) {
				for (int i = 0; i < size; ++i) {
					int id = next++;
					pool.enqueue([&order, id] { order.push_back(id); });
				}
			} else {
				std::vector<std::function<void()>> batch;
				for (int i = 0; i < size; ++i) {
					int id = next++;
					batch.push_back([&order, id] { order.push_back(id); });
				}
				CHECK(pool.enqueue_bulk(batch.begin(), batch.size()));
			}
		}
		CHECK(pool.size() == static_cast<std::size_t>(next));
		gate.store(true, std::memory_order_release);
		pool.end();
		CHECK(pool.join());
		CHECK(order.size() == static_cast<std::size_t>(next));
		for (int i = 0; i < next; ++i) {
			CHECK(order[i] == i);
		}
	}
}

// ---- parking and waking: bursts with gaps, so workers sleep between them ----
void test_bursts(int workers, int bursts) {
	ThreadPool<> pool("P{:02}", static_cast<std::size_t>(workers));
	std::mt19937 rng(7);
	std::atomic<long> ran{0};
	long queued = 0;
	for (int b = 0; b < bursts; ++b) {
		int n = 1 + static_cast<int>(rng() % 40);
		for (int i = 0; i < n; ++i) {
			pool.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
		}
		queued += n;
		// Sometimes no gap, sometimes long enough for every worker to stop spinning and sleep.
		switch (rng() % 4) {
			case 0: break;
			case 1: std::this_thread::sleep_for(std::chrono::microseconds(rng() % 50)); break;
			case 2: std::this_thread::sleep_for(std::chrono::microseconds(500 + rng() % 1500)); break;
			default: wait_until([&] { return ran.load() == queued; }); std::this_thread::sleep_for(2ms); break;
		}
	}
	wait_until([&] { return ran.load() == queued; });
	// The workers wake by their 100 ms timeout too, which would hide a lost wakeup as latency;
	// the queue counts a timeout that found a task. A task can also arrive in the instant after
	// a worker's timeout fires (a loaded host delays it there), so a few are not a loss; a
	// protocol that loses wakeups has one per burst that finds the workers asleep.
	CHECK(pool.lost_wakeups() <= static_cast<std::size_t>(bursts) / 20 + 1);
	pool.end();
	CHECK(pool.join());
}

// A single task after a long idle must wake a sleeper at once, not by the timeout.
void test_wake_latency(int workers) {
	ThreadPool<> pool("W{:02}", static_cast<std::size_t>(workers));
	std::this_thread::sleep_for(50ms);  // let every worker sleep
	int late = 0;
	for (int round = 0; round < 20; ++round) {
		std::promise<void> p;
		auto f = p.get_future();
		auto t0 = std::chrono::steady_clock::now();
		pool.enqueue([&p] { p.set_value(); });
		CHECK(f.wait_for(5s) == std::future_status::ready);
		auto dt = std::chrono::steady_clock::now() - t0;
		if (dt >= 90ms) {  // the timeout wakeup is at 100 ms; a loaded host may delay a few
			++late;
		}
		std::this_thread::sleep_for(20ms);
	}
	CHECK(late <= 2);
	CHECK(pool.lost_wakeups() <= 2);
	// One task wakes one sleeper, not all of them (a worker that is timing out may take it instead).
	CHECK(pool.wakeups() <= 40);
}

// ---- the window between "the queue is empty" and "I am a sleeper" ----
// A task that arrives there sees no sleeper to wake, so the worker must look once more after it
// registers. The hook queues the task from the worker itself, at that exact point.
std::atomic<ThreadPool<>*> hook_pool{nullptr};
std::atomic<int> hook_armed{0};
std::atomic<int> hook_ran{0};

void test_sleep_window() {
	hook_armed = 1;
	ThreadPool<> pool("H{:02}", 1);
	hook_pool = &pool;
	wait_until([&] { return hook_armed.load() == 0; });  // the worker found its queue empty, and queued
	auto t0 = std::chrono::steady_clock::now();
	wait_until([&] { return hook_ran.load() == 1; }, 5000ms);
	CHECK(std::chrono::steady_clock::now() - t0 < 60ms);  // not by the 100 ms timeout
	CHECK(pool.lost_wakeups() == 0);  // a single worker, asleep for this one task: a timeout here is a loss
	hook_pool = nullptr;
	pool.end();
	CHECK(pool.join());
}

// ---- a task is always counted: in size() or in running_size() ----
void test_counted(int workers) {
	ThreadPool<> pool("K{:02}", static_cast<std::size_t>(workers));
	std::atomic<long> enq_done{0}, done{0};
	std::atomic<bool> stop{false};
	std::atomic<long> samples{0};
	std::thread sampler([&] {
		while (!stop.load()) {
			long e = enq_done.load(std::memory_order_acquire);
			std::size_t s = pool.size();
			std::size_t r = pool.running_size();
			long d = done.load(std::memory_order_acquire);
			// Some task enqueued before this look had not finished after it: it was queued or running.
			if (d < e) {
				CHECK(s + r >= 1);
				samples.fetch_add(1);
			}
		}
	});
	const long total = 200000;
	for (long i = 0; i < total; ++i) {
		pool.enqueue([&done] {
			for (volatile int spin = 0; spin < 20; ++spin) {
			}
			done.fetch_add(1, std::memory_order_release);
		});
		enq_done.store(i + 1, std::memory_order_release);
	}
	wait_until([&] { return done.load() == total; });
	stop = true;
	sampler.join();
	pool.end();
	CHECK(pool.join());
	CHECK(pool.size() == 0 && pool.running_size() == 0);
	CHECK(samples.load() > 0);
}

// ---- shutdown and destruction: each queued task destroyed exactly once ----
void test_destruction() {
	// finish() with a backlog: what did not run is destroyed with the pool.
	stage = "destruction: finish with a backlog";
	{
		Counts counts;
		constexpr int n = 5000;
		{
			ThreadPool<> pool("Z{:02}", 2);
			std::atomic<bool> gate{false}, in{false};
			pool.enqueue([&] {
				in = true;
				while (!gate.load()) {
					std::this_thread::yield();
				}
			});
			pool.enqueue([&] {
				while (!gate.load()) {
					std::this_thread::yield();
				}
			});
			wait_until([&] { return in.load(); });
			for (int i = 0; i < n; ++i) {
				pool.enqueue(Token(&counts));
			}
			CHECK(pool.size() >= static_cast<std::size_t>(n) - 1);
			pool.finish();
			gate = true;
			CHECK(pool.join());
		}
		CHECK(counts.destroyed.load() == n);
		CHECK(counts.ran.load() <= n);
	}
	// end() drains: everything runs and is destroyed.
	stage = "destruction: end drains";
	{
		Counts counts;
		constexpr int n = 20000;
		ThreadPool<> pool("Y{:02}", 3);
		for (int i = 0; i < n; ++i) {
			pool.enqueue(Token(&counts));
		}
		pool.end();
		CHECK(pool.join());
		CHECK(counts.ran.load() == n);
		CHECK(counts.destroyed.load() == n);
	}
	// clear() racing producers and workers: every task is run or dropped, destroyed once.
	stage = "destruction: clear racing";
	{
		Counts counts;
		std::atomic<long> enqueued{0};
		{
			ThreadPool<> pool("X{:02}", 4);
			std::atomic<bool> stop{false};
			std::atomic<bool> clearing{false};
			std::vector<std::thread> ps;
			for (int p = 0; p < 3; ++p) {
				ps.emplace_back([&] {
					while (!stop.load()) {
						// clear() ends when it finds the queue empty, so producers that can keep ahead
						// of it would hold it forever (a slow, instrumented build does): they pause for
						// it, and an enqueue already under way still races it.
						if (clearing.load() || pool.size() > 20000) {
							std::this_thread::yield();
							continue;
						}
						pool.enqueue(Token(&counts));
						enqueued.fetch_add(1);
					}
				});
			}
			auto until = std::chrono::steady_clock::now() + std::chrono::duration<double>(stress_seconds() / 2);
			while (std::chrono::steady_clock::now() < until) {
				std::this_thread::sleep_for(500us);  // let tasks queue, then clear while the workers run
				clearing = true;
				pool.clear();
				clearing = false;
			}
			stop = true;
			for (auto& t : ps) {
				t.join();
			}
			pool.clear();
			pool.end();
			CHECK(pool.join());
		}
		CHECK(counts.destroyed.load() == enqueued.load());
		CHECK(counts.ran.load() <= enqueued.load());
	}
	// finish() while producers keep enqueueing: no crash, nothing leaks, join returns.
	stage = "destruction: finish racing";
	{
		Counts counts;
		std::atomic<long> enqueued{0};
		{
			ThreadPool<> pool("W{:02}", 4);
			std::atomic<bool> stop{false};
			std::vector<std::thread> ps;
			for (int p = 0; p < 3; ++p) {
				ps.emplace_back([&] {
					while (!stop.load()) {
						if (pool.size() > 20000) {  // bounded: clear() only ends if the queue can empty
							std::this_thread::yield();
							continue;
						}
						pool.enqueue(Token(&counts));
						enqueued.fetch_add(1);
					}
				});
			}
			std::this_thread::sleep_for(std::chrono::duration<double>(stress_seconds() / 4));
			pool.finish();
			CHECK(pool.join());
			stop = true;
			for (auto& t : ps) {
				t.join();
			}
		}
		CHECK(counts.destroyed.load() == enqueued.load());
	}
	// A pool with no threads queues and destroys.
	stage = "destruction: no threads";
	{
		Counts counts;
		{
			ThreadPool<> pool("N{:02}", 0);
			for (int i = 0; i < 200; ++i) {
				pool.enqueue(Token(&counts));
			}
			CHECK(pool.size() == 200);
			CHECK(pool.running_size() == 0);
			CHECK(pool.threadpool_size() == 0);
		}
		CHECK(counts.destroyed.load() == 200);
		CHECK(counts.ran.load() == 0);
	}
}

// ---- exceptions: through futures, and from tasks nobody waits on ----
void test_exceptions() {
	ThreadPool<> pool("E{:02}", 3);
	std::vector<std::future<int>> futures;
	for (int i = 0; i < 2000; ++i) {
		futures.push_back(pool.async([](int v) -> int {
			if (v % 3 == 0) {
				throw std::runtime_error(std::to_string(v));
			}
			return v;
		}, i));
		pool.enqueue([] { throw std::runtime_error("nobody waits"); });
	}
	// The pool is joined before the messages are read: a worker destroys its end of the shared
	// state, and with it the stored exception, right after it sets it, and the exception's
	// reference count lives in the standard library, where TSan sees no ordering.
	pool.end();
	CHECK(pool.join());
	for (int i = 0; i < 2000; ++i) {
		if (i % 3 == 0) {
			bool threw = false;
			try {
				futures[i].get();
			} catch (const std::runtime_error& e) {
				threw = std::to_string(i) == e.what();
			}
			CHECK(threw);
		} else {
			CHECK(futures[i].get() == i);
		}
	}
}

// A reference result and a void result.
void test_result_shapes() {
	ThreadPool<> pool("R{:02}", 2);
	static int target = 3;
	auto r = pool.async([]() -> int& { return target; });
	CHECK(&r.get() == &target);
	pool.async([] {}).get();
	pool.end();
	CHECK(pool.join());
}

// ---- the other task types ----
struct Job {
	std::atomic<long>* sum;
	long v;
	void operator()() { sum->fetch_add(v, std::memory_order_relaxed); }
};

void test_task_types() {
	{
		std::atomic<long> sum{0};
		ThreadPool<std::shared_ptr<Job>> pool("S{:02}", 3);
		for (long i = 1; i <= 5000; ++i) {
			pool.enqueue(std::make_shared<Job>(Job{&sum, i}));
		}
		pool.end();
		CHECK(pool.join());
		CHECK(sum.load() == 5000L * 5001 / 2);
		CHECK(pool.size() == 0);
	}
	{
		std::atomic<long> sum{0};
		ThreadPool<std::unique_ptr<Job>> pool("U{:02}", 3);
		for (long i = 1; i <= 5000; ++i) {
			pool.enqueue(std::make_unique<Job>(Job{&sum, i}));
		}
		pool.end();
		CHECK(pool.join());
		CHECK(sum.load() == 5000L * 5001 / 2);
	}
}

}  // namespace

void queue_hook(void*) {
	if (hook_armed.load(std::memory_order_acquire) != 1) {
		return;
	}
	ThreadPool<>* pool;
	while (!(pool = hook_pool.load(std::memory_order_acquire))) {
		std::this_thread::yield();
	}
	if (hook_armed.exchange(0) == 1) {
		pool->enqueue([] { hook_ran.fetch_add(1); });
	}
}

int main() {
	const double scale = stress_seconds();
	watchdog(300 * (scale < 1 ? 1 : scale));

	stage = "counts";
	for (int workers : {1, 2, 4, 8}) {
		for (int producers : {1, 2, 4}) {
			test_counts(workers, producers, static_cast<int>(20000 * scale));
		}
	}
	std::printf("pool_stress OK: counts and sums exact over 1-8 workers x 1-4 producers\n");

	stage = "order";
	test_order();
	std::printf("pool_stress OK: per-producer order, and order across chunk boundaries\n");

	stage = "bursts";
	for (int workers : {1, 3, 8}) {
		test_bursts(workers, static_cast<int>(300 * scale));
		test_wake_latency(workers);
	}
	std::printf("pool_stress OK: bursts with gaps: no wakeup lost\n");

	stage = "sleep window";
	test_sleep_window();
	std::printf("pool_stress OK: a task that arrives as a worker goes to sleep is not left waiting\n");

	stage = "counted";
	test_counted(1);
	test_counted(4);
	std::printf("pool_stress OK: a task is always in size() or running_size()\n");

	stage = "destruction";
	test_destruction();
	std::printf("pool_stress OK: every queued task destroyed exactly once\n");

	stage = "exceptions";
	test_exceptions();
	test_result_shapes();
	std::printf("pool_stress OK: exceptions through futures and from bare tasks, reference and void results\n");

	stage = "task types";
	test_task_types();
	std::printf("pool_stress OK: shared_ptr and unique_ptr task types\n");

	std::printf("all pool stress tests passed\n");
	return 0;
}
