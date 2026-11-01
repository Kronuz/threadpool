// Trivial-task throughput of ThreadPool::enqueue against the frozen pre-change pool, a mutex +
// condition variable pool, and asio::thread_pool (when asio is found).
//
//   enqueue_bench [reps] [tasks-per-producer] [shared|local|cap40|idle|latency|trickle]
//
// trickle: one producer submits a task every gap_us microseconds; reports the delay from submit
// to the start of the task (median, p99) and the CPU the whole process used per task.
// idle: the CPU an idle pool uses (its workers' wakeups), as a share of one core.
// latency: ns for one future round trip, async(f).get() back to back, which includes waking a
// worker that may have gone to sleep.
//
// Every pool runs the same stateless task from P producer threads into W workers. The figure is
// wall-clock ns per task, from the first submit until the last task has run (min and median over
// the reps, and the spread max/min, which is the A/A noise: a ratio inside it is a tie). Pools
// take turns within a rep, so slow drift of the machine lands on all of them.
//
// Workloads:
//   shared  each task bumps one shared atomic (the count every worker contends on: the original
//           survey's workload; the last task wakes the waiter)
//   local   each task bumps a counter of its worker's own (what a pool's own cost looks like)
//   cap40   as local, with 40 bytes captured: past std::function's inline buffer on libc++
//
// Build with -DTHREADPOOL_BUILD_BENCH=ON (fetches asio 1.30.2, or takes -DASIO_INCLUDE_DIR=<asio>/asio/include),
// or by hand:
//   c++ -std=c++20 -O2 -I. -Itest [-DASIO_STANDALONE -I<asio>] bench/enqueue_bench.cc thread.cc
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "threadpool.hh"
#include "threadpool_old.hh"

#if __has_include(<asio.hpp>)
#  ifndef ASIO_STANDALONE
#    define ASIO_STANDALONE
#  endif
#  include <asio.hpp>
#  define HAVE_ASIO 1
#endif

namespace {

double now_s() {
	return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// The mutex + condition variable pool everyone writes first.
struct NaivePool {
	std::mutex m;
	std::condition_variable cv;
	std::deque<std::function<void()>> q;
	bool done = false;
	std::vector<std::thread> th;

	explicit NaivePool(int n) {
		for (int i = 0; i < n; i++) {
			th.emplace_back([this] {
				for (;;) {
					std::function<void()> f;
					{
						std::unique_lock<std::mutex> l(m);
						cv.wait(l, [&] { return done || !q.empty(); });
						if (q.empty()) {
							return;
						}
						f = std::move(q.front());
						q.pop_front();
					}
					f();
				}
			});
		}
	}

	template <typename F>
	void enqueue(F&& f) {
		{
			std::lock_guard<std::mutex> l(m);
			q.push_back(std::forward<F>(f));
		}
		cv.notify_one();
	}

	~NaivePool() {
		{
			std::lock_guard<std::mutex> l(m);
			done = true;
		}
		cv.notify_all();
		for (auto& t : th) {
			t.join();
		}
	}
};

// Workload completion: how the benchmark knows every task ran.
struct alignas(128) Slot {
	std::atomic<unsigned long> n{0};
};

std::mutex registry_mutex;
std::vector<Slot*> registry;  // one slot per worker thread, never freed

Slot* register_slot() {
	auto* slot = new Slot;
	std::lock_guard<std::mutex> lk(registry_mutex);
	registry.push_back(slot);
	return slot;
}

unsigned long registry_sum() {
	std::lock_guard<std::mutex> lk(registry_mutex);
	unsigned long sum = 0;
	for (auto* slot : registry) {
		sum += slot->n.load(std::memory_order_acquire);
	}
	return sum;
}

std::atomic<unsigned long> shared_count{0};
std::atomic<unsigned long> shared_target{0};
std::atomic<int> shared_done{0};

struct Shared {
	static constexpr const char* name = "shared";
	static unsigned long begin(unsigned long total) {
		shared_count.store(0);
		shared_target.store(total);
		shared_done.store(0);
		return 0;
	}
	static auto task() {
		return [] {
			if (shared_count.fetch_add(1, std::memory_order_acq_rel) + 1 == shared_target.load(std::memory_order_relaxed)) {
				shared_done.store(1, std::memory_order_release);
				shared_done.notify_one();
			}
		};
	}
	static void wait(unsigned long) {
		shared_done.wait(0, std::memory_order_acquire);
	}
	static bool exact(unsigned long target) { return shared_count.load(std::memory_order_acquire) == target; }
};

struct Local {
	static constexpr const char* name = "local";
	static unsigned long begin(unsigned long) { return registry_sum(); }
	static void bump() {
		thread_local Slot* mine = register_slot();
		mine->n.store(mine->n.load(std::memory_order_relaxed) + 1, std::memory_order_release);
	}
	static auto task() {
		return [] { bump(); };
	}
	static void wait(unsigned long target) {
		while (registry_sum() < target) {
			std::this_thread::sleep_for(std::chrono::microseconds(20));
		}
	}
	static bool exact(unsigned long target) { return registry_sum() == target; }
};

struct Cap40 : Local {
	static constexpr const char* name = "cap40";
	static auto task() {
		unsigned long pad[5] = {1, 2, 3, 4, 5};
		return [pad] {
			asm volatile("" ::"r"(pad[0]) : "memory");
			bump();
		};
	}
};

// Runs P producers x N tasks through submit(task) and returns ns per task.
template <typename Workload, typename Submit>
double run_once(int P, long N, Submit&& submit) {
	const unsigned long total = static_cast<unsigned long>(P) * static_cast<unsigned long>(N);
	const unsigned long base = Workload::begin(total);
	const unsigned long target = base + total;
	std::atomic<int> ready{0};
	std::atomic<bool> go{false};
	std::vector<std::thread> producers;
	for (int p = 0; p < P; p++) {
		producers.emplace_back([&] {
			ready.fetch_add(1);
			while (!go.load(std::memory_order_acquire)) {
			}
			for (long i = 0; i < N; i++) {
				submit(Workload::task());
			}
		});
	}
	while (ready.load() < P) {
		std::this_thread::yield();
	}
	const double t0 = now_s();
	go.store(true, std::memory_order_release);
	for (auto& t : producers) {
		t.join();
	}
	Workload::wait(target);
	const double ns = (now_s() - t0) * 1e9 / static_cast<double>(total);
	// Every pool must have run exactly the tasks it was given: a lost or repeated task aborts.
	if (!Workload::exact(target)) {
		std::fprintf(stderr, "a pool ran a different number of tasks than it was given\n");
		std::abort();
	}
	return ns;
}

struct Result {
	std::string name;
	std::vector<double> ns;
	double min() const { return *std::min_element(ns.begin(), ns.end()); }
	double max() const { return *std::max_element(ns.begin(), ns.end()); }
	double median() const {
		auto v = ns;
		std::sort(v.begin(), v.end());
		return v[v.size() / 2];
	}
};

template <typename Workload>
void bench(int W, int P, long N, int reps) {
	threadpool_old::ThreadPool<> old_pool("O{:02}", W);
	ThreadPool<> new_pool("N{:02}", W);
	NaivePool naive(W);
#ifdef HAVE_ASIO
	asio::thread_pool asio_pool(W);
#endif

	std::vector<Result> results;
	auto add = [&](const char* name) { results.push_back({name, {}}); };
	add("old ThreadPool");
	add("new ThreadPool");
	add("mutex+cv naive");
#ifdef HAVE_ASIO
	add("asio::thread_pool");
#endif

	auto old_submit = [&](auto&& t) { old_pool.enqueue(std::forward<decltype(t)>(t)); };
	auto new_submit = [&](auto&& t) { new_pool.enqueue(std::forward<decltype(t)>(t)); };
	auto naive_submit = [&](auto&& t) { naive.enqueue(std::forward<decltype(t)>(t)); };
#ifdef HAVE_ASIO
	auto asio_submit = [&](auto&& t) { asio::post(asio_pool, std::forward<decltype(t)>(t)); };
#endif

	for (int rep = -1; rep < reps; rep++) {  // rep -1 warms up
		auto record = [&](std::size_t i, double ns) {
			if (rep >= 0) {
				results[i].ns.push_back(ns);
			}
		};
		// Rotate the order so no pool always follows the same neighbor.
		for (int k = 0; k < static_cast<int>(results.size()); k++) {
			switch ((k + rep + 1) % static_cast<int>(results.size())) {
				case 0: record(0, run_once<Workload>(P, N, old_submit)); break;
				case 1: record(1, run_once<Workload>(P, N, new_submit)); break;
				case 2: record(2, run_once<Workload>(P, N, naive_submit)); break;
#ifdef HAVE_ASIO
				case 3: record(3, run_once<Workload>(P, N, asio_submit)); break;
#endif
			}
		}
	}

	std::printf("--- %s: %d producers, %d workers, %ld tasks each (ns per task; min / median / spread)\n", Workload::name, P, W, N);
	const double naive_min = results[2].min();
#ifdef HAVE_ASIO
	const double asio_min = results[3].min();
#endif
	for (auto& r : results) {
		std::printf("  %-20s %8.1f %8.1f   %.2f", r.name.c_str(), r.min(), r.median(), r.max() / r.min());
		if (r.name == "new ThreadPool") {
			std::printf("   vs naive %.2fx", r.min() / naive_min);
#ifdef HAVE_ASIO
			std::printf(", vs asio %.2fx", r.min() / asio_min);
#endif
			std::printf(", vs old %.2fx", r.min() / results[0].min());
		}
		std::printf("\n");
	}
	std::fflush(stdout);

	old_pool.end();
	old_pool.join();
	new_pool.end();
	new_pool.join();
}

double cpu_seconds() {
	rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	return static_cast<double>(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) + 1e-6 * static_cast<double>(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
}

template <typename Pool>
void idle_cpu(const char* name, int W) {
	Pool pool("I{:02}", static_cast<std::size_t>(W));
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	const double c0 = cpu_seconds();
	const double t0 = now_s();
	std::this_thread::sleep_for(std::chrono::seconds(4));
	std::printf("  %-20s %d idle workers: %.3f%% of one core\n", name, W, 100 * (cpu_seconds() - c0) / (now_s() - t0));
}

// ns per async(f).get(); the median of reps batches.
template <typename Submit>
void latency(const char* name, int reps, Submit&& round_trip) {
	std::vector<double> batches;
	for (int r = 0; r < reps; r++) {
		const int n = 2000;
		const double t0 = now_s();
		for (int i = 0; i < n; i++) {
			round_trip();
		}
		batches.push_back((now_s() - t0) * 1e9 / n);
	}
	std::sort(batches.begin(), batches.end());
	std::printf("  %-20s %8.0f ns (min %.0f, spread %.2f)\n", name, batches[batches.size() / 2], batches.front(), batches.back() / batches.front());
}

// Submits n tasks, one per gap_ns (the producer waits by spinning on the clock), and returns the
// submit-to-start delay of each, in ns, with the CPU seconds the process used.
template <typename Submit>
void trickle(const char* name, long n, double gap_ns, Submit&& submit) {
	std::vector<double> starts(static_cast<std::size_t>(n), 0.0);
	std::atomic<long> done{0};
	std::vector<double> submitted(static_cast<std::size_t>(n), 0.0);
	const double c0 = cpu_seconds();
	const double t0 = now_s();
	for (long i = 0; i < n; i++) {
		const double due = t0 + static_cast<double>(i) * gap_ns * 1e-9;
		while (now_s() < due) {
		}
		submitted[static_cast<std::size_t>(i)] = now_s();
		submit([&starts, &done, i] {
			starts[static_cast<std::size_t>(i)] = now_s();
			done.fetch_add(1, std::memory_order_release);
		});
	}
	while (done.load(std::memory_order_acquire) < n) {
		std::this_thread::sleep_for(std::chrono::microseconds(50));
	}
	const double cpu = cpu_seconds() - c0;
	std::vector<double> delay(static_cast<std::size_t>(n));
	for (long i = 0; i < n; i++) {
		delay[static_cast<std::size_t>(i)] = (starts[static_cast<std::size_t>(i)] - submitted[static_cast<std::size_t>(i)]) * 1e9;
	}
	std::sort(delay.begin(), delay.end());
	// The producer's own spin-wait is one core for the whole run; the rest is the pool's.
	const double wall = now_s() - t0;
	std::printf("  %-20s delay median %7.0f ns  p99 %8.0f ns   pool CPU %6.0f ns per task\n", name, delay[delay.size() / 2], delay[delay.size() * 99 / 100], (cpu - wall) * 1e9 / static_cast<double>(n));
}

void run_trickle(double gap_ns, long n) {
	const int W = 4;
	std::printf("--- trickle: one producer, a task every %.0f ns, %d workers\n", gap_ns, W);
	threadpool_old::ThreadPool<> old_pool("T{:02}", W);
	ThreadPool<> new_pool("T{:02}", W);
	NaivePool naive(W);
#ifdef HAVE_ASIO
	asio::thread_pool asio_pool(W);
#endif
	for (int r = 0; r < 2; r++) {
		trickle("old ThreadPool", n, gap_ns, [&](auto&& t) { old_pool.enqueue(std::forward<decltype(t)>(t)); });
		trickle("new ThreadPool", n, gap_ns, [&](auto&& t) { new_pool.enqueue(std::forward<decltype(t)>(t)); });
		trickle("mutex+cv naive", n, gap_ns, [&](auto&& t) { naive.enqueue(std::forward<decltype(t)>(t)); });
#ifdef HAVE_ASIO
		trickle("asio::thread_pool", n, gap_ns, [&](auto&& t) { asio::post(asio_pool, std::forward<decltype(t)>(t)); });
#endif
	}
}

void run_idle_and_latency(const std::string& only, int reps) {
	const int W = 4;
	if (only == "idle") {
		std::printf("--- idle pools (4 s each)\n");
		for (int w : {4, 8}) {
			idle_cpu<threadpool_old::ThreadPool<>>("old ThreadPool", w);
			idle_cpu<ThreadPool<>>("new ThreadPool", w);
		}
	} else {
		std::printf("--- future round trip, %d workers\n", W);
		threadpool_old::ThreadPool<> old_pool("L{:02}", W);
		ThreadPool<> new_pool("L{:02}", W);
		NaivePool naive(W);
#ifdef HAVE_ASIO
		asio::thread_pool asio_pool(W);
#endif
		for (int r = 0; r < 2; r++) {  // twice: the order is a neighbor effect, not a result
			latency("old async", reps, [&] { return old_pool.async([] { return 1; }).get(); });
			latency("new async", reps, [&] { return new_pool.async([] { return 1; }).get(); });
			latency("new post+promise", reps, [&] {
				auto p = std::make_shared<std::promise<int>>();
				auto f = p->get_future();
				new_pool.post([p] { p->set_value(1); });
				return f.get();
			});
			latency("naive+promise", reps, [&] {
				auto p = std::make_shared<std::promise<int>>();
				auto f = p->get_future();
				naive.enqueue([p] { p->set_value(1); });
				return f.get();
			});
#ifdef HAVE_ASIO
			latency("asio post+promise", reps, [&] {
				auto p = std::make_shared<std::promise<int>>();
				auto f = p->get_future();
				asio::post(asio_pool, [p] { p->set_value(1); });
				return f.get();
			});
#endif
		}
	}
}

}  // namespace

int main(int argc, char** argv) {
	const int reps = argc > 1 ? std::atoi(argv[1]) : 7;
	const long N = argc > 2 ? std::atol(argv[2]) : 100000;
	const char* only = argc > 3 ? argv[3] : "";
#ifdef HAVE_ASIO
	std::printf("asio %d.%d.%d\n", ASIO_VERSION / 100000, ASIO_VERSION / 100 % 1000, ASIO_VERSION % 100);
#endif
	if (std::string(only) == "trickle") {
		for (double gap : {1000.0, 5000.0, 50000.0}) {
			run_trickle(gap, N);
		}
		return 0;
	}
	if (std::string(only) == "idle" || std::string(only) == "latency") {
		run_idle_and_latency(only, reps);
		return 0;
	}
	for (int W : {1, 4, 8}) {
		for (int P : {1, 4}) {
			if (std::string(only).empty() || std::string(only) == "shared") {
				bench<Shared>(W, P, N, reps);
			}
			if (std::string(only).empty() || std::string(only) == "local") {
				bench<Local>(W, P, N, reps);
			}
			if (std::string(only).empty() || std::string(only) == "cap40") {
				bench<Cap40>(W, P, N, reps);
			}
		}
	}
	return 0;
}
