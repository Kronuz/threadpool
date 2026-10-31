// Trivial-task throughput of ThreadPool::enqueue against the frozen pre-change pool, a mutex +
// condition variable pool, and asio::thread_pool (when asio is found).
//
//   enqueue_bench [reps] [tasks-per-producer]
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
// Build with -DTHREADPOOL_BUILD_BENCH=ON (finds asio, or fetches it), or by hand:
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
#include <string>
#include <thread>
#include <vector>

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
	return (now_s() - t0) * 1e9 / static_cast<double>(total);
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

}  // namespace

int main(int argc, char** argv) {
	const int reps = argc > 1 ? std::atoi(argv[1]) : 7;
	const long N = argc > 2 ? std::atol(argv[2]) : 100000;
	const char* only = argc > 3 ? argv[3] : "";
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
