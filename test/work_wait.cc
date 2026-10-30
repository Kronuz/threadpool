// Tests for WorkGroup::wait() and on_idle(): a wait serves only the group it waits on, a wait
// on a group already on the calling stack aborts, no wait creates a thread, parked waiters are
// woken when the group goes idle, and waits on lower groups of an acyclic graph always end.
// Each hang is a watchdog abort, not a stuck run.
// Build: c++ -std=c++20 -I.. work_wait.cc ../thread.cc -o work_wait && ./work_wait
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <dirent.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#  include <mach/mach.h>
#elif defined(__FreeBSD__)
#  include <sys/sysctl.h>
#  include <sys/user.h>
#endif

#include "work_pool.hh"

#ifndef __has_feature
#  define __has_feature(x) 0
#endif

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

void burn(std::chrono::microseconds d) {
	auto end = Clock::now() + d;
	while (Clock::now() < end) {
	}
}

template <typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit) {
	auto end = Clock::now() + limit;
	while (!pred()) {
		if (Clock::now() > end) {
			return false;
		}
		std::this_thread::sleep_for(200us);
	}
	return true;
}

// Aborts the process if the test does not finish in time.
struct Watchdog {
	std::atomic<bool> done{false};
	std::thread thread;
	explicit Watchdog(int seconds, const char* what) :
		thread([this, seconds, what] {
			for (int i = 0; i < seconds * 10 && !done.load(); ++i) {
				std::this_thread::sleep_for(100ms);
			}
			if (!done.load()) {
				std::fprintf(stderr, "TIMEOUT: %s hung\n", what);
				std::abort();
			}
		}) {}
	~Watchdog() {
		done = true;
		thread.join();
	}
};

void note_peak(std::atomic<unsigned>& running, std::atomic<unsigned>& peak) {
	auto n = running.fetch_add(1) + 1;
	for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
	}
}

// Threads in this process now.
unsigned process_threads() {
#if defined(__linux__)
	unsigned n = 0;
	if (auto* d = opendir("/proc/self/task")) {
		while (auto* e = readdir(d)) {
			n += e->d_name[0] != '.';
		}
		closedir(d);
	}
	return n;
#elif defined(__APPLE__)
	thread_act_array_t list = nullptr;
	mach_msg_type_number_t count = 0;
	if (task_threads(mach_task_self(), &list, &count) != KERN_SUCCESS) {
		return 0;
	}
	for (mach_msg_type_number_t i = 0; i < count; ++i) {
		mach_port_deallocate(mach_task_self(), list[i]);
	}
	vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(list), count * sizeof(thread_t));
	return count;
#elif defined(__FreeBSD__)
	int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, getpid()};
	struct kinfo_proc kp{};
	size_t len = sizeof kp;
	return sysctl(mib, 4, &kp, &len, nullptr, 0) == 0 ? static_cast<unsigned>(kp.ki_numthreads) : 0;
#else
	return 0;
#endif
}

struct Child {
	int status = -1;
	std::string err;
	bool aborted(const char* needle) const {
		bool ok = WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT && err.find(needle) != std::string::npos;
		if (!ok) {
			std::fprintf(stderr, "death test: status %d, stderr: %s\n", status, err.c_str());
		}
		return ok;
	}
};

// Runs fn in a forked child with its stderr captured; a hang is the alarm's SIGALRM and a
// return is exit 0. Call it before any thread exists in this process.
template <typename F>
Child run_child(F&& fn) {
	Child child;
	int fds[2];
	if (pipe(fds) != 0) {
		return child;
	}
	pid_t pid = fork();
	if (pid == 0) {
		rlimit none{0, 0};
		setrlimit(RLIMIT_CORE, &none);
		dup2(fds[1], 2);
		close(fds[0]);
		close(fds[1]);
		alarm(20);
		fn();
		_exit(0);
	}
	close(fds[1]);
	char buf[512];
	for (ssize_t n; (n = read(fds[0], buf, sizeof buf)) > 0;) {
		child.err.append(buf, static_cast<size_t>(n));
	}
	close(fds[0]);
	waitpid(pid, &child.status, 0);
	return child;
}

// Items on this thread's stack now, and the most any stack held.
thread_local int t_nest = 0;
std::atomic<int> g_nest_peak{0};

struct Nest {
	Nest() {
		int n = ++t_nest;
		for (int m = g_nest_peak.load(); n > m && !g_nest_peak.compare_exchange_weak(m, n);) {
		}
	}
	~Nest() { --t_nest; }
};

void sleep_forever() {
	std::this_thread::sleep_for(1h);
}

// ---- state for the acyclic-graph property test (I6) ----

constexpr unsigned graph_groups = 50;

struct Graph {
	WorkPool& pool;
	std::vector<std::unique_ptr<WorkGroup>> groups;
	unsigned width[graph_groups];
	std::atomic<unsigned> running[graph_groups][3];
	std::atomic<unsigned> peak_over{0};  // times a group ran more items of a class than its width
	std::atomic<int> budget;             // items left to create
	std::atomic<std::uint64_t> ran{0};
	std::uint64_t seed;

	Graph(WorkPool& p, std::uint64_t s, int items) :
		pool(p),
		budget(items),
		seed(s) {
		for (unsigned i = 0; i < graph_groups; ++i) {
			width[i] = static_cast<unsigned>(mix(s, i, 1) % 4);  // 0 (no cap) to 3
			groups.push_back(std::make_unique<WorkGroup>(p, width[i]));
			for (auto& r : running[i]) {
				r = 0;
			}
		}
	}

	static std::uint64_t mix(std::uint64_t a, std::uint64_t b, std::uint64_t c) {
		std::uint64_t x = a * 0x9E3779B97F4A7C15ull + b * 0xBF58476D1CE4E5B9ull + c * 0x94D049BB133111EBull;
		x ^= x >> 31;
		x *= 0xD6E8FEB86659FD93ull;
		x ^= x >> 32;
		return x;
	}

	// Posts a task into group g; it may fan out to lower groups and wait for them.
	void post(unsigned g, std::uint64_t id) {
		if (budget.fetch_sub(1) <= 0) {
			return;
		}
		auto cls = static_cast<WorkClass>(mix(seed, id, 2) % 3);
		groups[g]->enqueue([this, g, id, cls] { run(g, id, cls); }, cls);
	}

	void run(unsigned g, std::uint64_t id, WorkClass cls) {
		auto c = static_cast<unsigned>(cls);
		struct Leave {
			std::atomic<unsigned>& n;
			~Leave() { n.fetch_sub(1); }
		};
		auto n = running[g][c].fetch_add(1) + 1;
		Leave leave{running[g][c]};
		if (width[g] && n > width[g]) {
			peak_over.fetch_add(1);
		}
		auto r = mix(seed, id, 3);
		if (r % 4 == 0) {
			burn(std::chrono::microseconds(r % 200));
		} else if (r % 16 == 1) {
			std::this_thread::yield();
		}
		ran.fetch_add(1);
		if (g == 0) {
			return;
		}
		// Waits only on lower groups, so the wait-for graph is acyclic.
		unsigned kids = (r >> 8) % 4;  // 0 to 3 fan-outs, each to a lower group
		unsigned target[3];
		for (unsigned k = 0; k < kids && k < 3; ++k) {
			target[k] = static_cast<unsigned>(mix(seed, id, 10 + k) % g);
			post(target[k], id * 4 + k + 1);
		}
		for (unsigned k = 0; k < kids && k < 3; ++k) {
			groups[target[k]]->wait();
		}
	}
};

// WORK_WAIT_ONLY=I6 (or I1, I2, I3, I4, I5, AL2, stop) runs one block; unset runs all.
bool want(const char* name) {
	const char* only = std::getenv("WORK_WAIT_ONLY");
	return !only || std::strstr(only, name);
}

}  // namespace

int main() {
	// ---- Death tests first, before the process has a thread: a wait that would wait for
	// itself aborts (I2). ----
	if (want("I2")) {
		bool self = run_child([] {
			WorkPool pool({.threads = 1});
			WorkGroup g(pool);
			g.enqueue([&] { g.wait(); });
			sleep_forever();
		}).aborted("WorkGroup::wait");
		assert(self);
		bool dtor = run_child([] {
			WorkPool pool({.threads = 1});
			auto* g = new WorkGroup(pool);
			g->enqueue([g] { delete g; });
			sleep_forever();
		}).aborted("~WorkGroup");
		assert(dtor);
		bool caw = run_child([] {
			WorkPool pool({.threads = 1});
			WorkGroup g(pool);
			g.enqueue([&] { g.cancel_and_wait(); });
			sleep_forever();
		}).aborted("cancel_and_wait");
		assert(caw);
		// G -> H -> G on one stack: G's item waits on H, which serves h inline, and h waits on G.
		bool cycle = run_child([] {
			WorkPool pool({.threads = 1});
			WorkGroup g(pool);
			WorkGroup h(pool);
			g.enqueue([&] {
				h.enqueue([&] { g.wait(); });
				h.wait();
			});
			sleep_forever();
		}).aborted("WorkGroup::wait");
		assert(cycle);
		// An item waiting on another group is fine: no abort.
		bool control = run_child([] {
			WorkPool pool({.threads = 1});
			WorkGroup g(pool);
			WorkGroup h(pool);
			g.enqueue([&] {
				h.enqueue([] {});
				h.wait();
			});
			g.wait();
		}).status == 0;
		assert(control);
		std::printf("work_wait OK (I2): a wait on a group on the calling stack aborts: own group, ~WorkGroup, cancel_and_wait, G -> H -> G; a child wait does not\n");
	}

	// ---- I1. Help scope: the architect's inversion shape. Groups Q -> G -> H, nothing waits
	// on Q. g (G) posts h (H, running on another worker) and calls H.wait(), with nothing to
	// serve; q (Q, a query) is queued and waits on G. If the waiter served q above g, neither
	// could finish. No item of another group ever runs on a thread inside a wait. ----
	if (want("I1")) {
		Watchdog dog(60, "I1 inversion");
		struct Case {
			unsigned threads;
			unsigned busy;
		};
		static thread_local const void* t_waiting_on = nullptr;
		static thread_local int t_lease = 0;
		for (Case cs : {Case{1, 0}, Case{2, 0}, Case{4, 2}}) {
			for (int round = 0; round < 3; ++round) {
				WorkPool pool({.threads = cs.threads, .reserve = 0});
				WorkGroup Q(pool), G(pool), H(pool), B(pool);
				std::atomic<int> q_saw_lease{-1};
				std::atomic<int> foreign{0};  // items that ran on a thread inside a wait on another group
				std::atomic<bool> h_running{false}, g_waiting{false}, q_posted{false}, g_done{false};
				auto check = [&](const void* mine) {
					if (t_waiting_on && t_waiting_on != mine) {
						foreign.fetch_add(1);
					}
				};
				for (unsigned i = 0; i < cs.busy; ++i) {
					B.enqueue([&] {
						check(&B);
						std::this_thread::sleep_for(600ms);
					});
				}
				G.enqueue([&] {
					check(&G);
					t_lease = 1;  // g holds index A's lease (a commit chore)
					H.enqueue([&] {
						check(&H);
						h_running = true;
						std::this_thread::sleep_for(300ms);
					});
					if (cs.threads > 1) {
						while (!h_running) {
							std::this_thread::yield();
						}
					}
					g_waiting = true;
					while (cs.threads > 1 && !q_posted) {
						std::this_thread::yield();  // q is on the query ring, every worker busy
					}
					t_waiting_on = &H;
					H.wait();
					t_waiting_on = nullptr;
					t_lease = 0;
					g_done = true;
				});
				while (!g_waiting) {
					std::this_thread::yield();
				}
				Q.enqueue([&] {
					check(&Q);
					q_saw_lease = t_lease;  // an unrelated chore of index B must not see A's lease
					G.wait();
				}, WorkClass::query);
				q_posted = true;
				Q.wait();
				G.wait();
				assert(g_done.load());
				assert(q_saw_lease.load() == 0);
				assert(foreign.load() == 0);
			}
		}
		std::printf("work_wait OK (I1, AR1, AR2): the Q -> G -> H inversion ends at 1, 2 and 4 workers; no foreign item ran inside a wait, and a query never saw the waiter's thread-local\n");
	}

	// ---- I3. A worker waits on another group with a width, all workers waiting at once (the
	// ring ticket of a width-capped group is not a running turn), and helpers keep the width. ----
	if (want("I3")) {
		Watchdog dog(60, "I3 width");
		for (unsigned threads : {1u, 2u, 4u}) {
			for (unsigned width : {0u, 1u, 2u, 3u}) {
				WorkPool pool({.threads = threads});
				WorkGroup h(pool, width);
				WorkGroup p(pool);
				std::atomic<int> ran{0};
				for (unsigned i = 0; i < threads; ++i) {
					p.enqueue([&] {
						std::this_thread::sleep_for(30ms);  // every worker is inside a p item
						h.enqueue([&] { ran.fetch_add(1); });
						h.wait();
					});
				}
				p.wait();
				assert(ran.load() == int(threads));
			}
		}
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup h(pool, 1);
		WorkGroup p(pool);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		for (int i = 0; i < 4; ++i) {
			p.enqueue([&] {
				for (int k = 0; k < 60; ++k) {
					h.enqueue([&] {
						note_peak(now, peak);
						burn(100us);
						now.fetch_sub(1);
					});
					h.wait();
				}
			});
		}
		p.wait();
		assert(peak.load() == 1);
		std::printf("work_wait OK (I3): workers waiting on a width-capped group completed at 1, 2 and 4 threads; width 1 held under 4 helpers (peak %u)\n", peak.load());
	}

	// ---- I4. No lost wake: a width-1 group has its width held by a 50 ms item, with three
	// items queued behind it; three workers wait on it and park; the group going idle wakes
	// all of them. ----
	if (want("I4")) {
		Watchdog dog(30, "I4 wake");
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup h(pool, 1);
		WorkGroup p(pool);
		std::atomic<bool> a_started{false};
		std::atomic<int> ran{0};
		std::atomic<int> waited{0};
		h.enqueue([&] {
			a_started = true;
			std::this_thread::sleep_for(50ms);
			ran.fetch_add(1);
		});
		assert(wait_for([&] { return a_started.load(); }, 5000ms));
		for (int i = 0; i < 3; ++i) {
			h.enqueue([&] { ran.fetch_add(1); });
		}
		for (int i = 0; i < 3; ++i) {
			p.enqueue([&] {
				h.wait();
				waited.fetch_add(1);
			});
		}
		p.wait();
		assert(waited.load() == 3);
		assert(ran.load() == 4);
		std::printf("work_wait OK (I4): three parked waiters on a width-1 group were all woken when it went idle\n");
	}

	// ---- I5. Bounded resources: a wait creates no thread and inline depth is the program's
	// nesting. 20,000 items of G each fan out to a child group of their own and wait for it;
	// the process keeps its threads throughout and no stack holds more than two items. ----
	if (want("I5")) {
		Watchdog dog(120, "I5 resources");
		for (unsigned threads : {1u, 4u}) {
			g_nest_peak = 0;
			std::atomic<bool> stop{false};
			std::atomic<unsigned> max_threads{0};
			std::thread sampler([&] {
				while (!stop.load()) {
					auto n = process_threads();
					for (auto m = max_threads.load(); n > m && !max_threads.compare_exchange_weak(m, n);) {
					}
					std::this_thread::sleep_for(200us);
				}
			});
			assert(wait_for([&] { return max_threads.load() > 0; }, 5000ms));
			unsigned base = max_threads.load();  // main, watchdog and sampler
			{
				WorkPool pool({.threads = threads});
				WorkGroup g(pool);
				std::atomic<int> done{0};
				for (int i = 0; i < 20000; ++i) {
					g.enqueue([&] {
						Nest n;
						WorkGroup child(pool);
						std::atomic<int> got{0};
						for (int k = 0; k < 2; ++k) {
							child.enqueue([&] {
								Nest m;
								got.fetch_add(1);
							});
						}
						child.wait();
						assert(got.load() == 2);
						done.fetch_add(1);
					});
				}
				g.wait();
				assert(done.load() == 20000);
			}
			stop = true;
			sampler.join();
			if (base) {
				assert(max_threads.load() <= base + threads + 2);  // slack for runtime helper threads
			}
			assert(g_nest_peak.load() == 2);
			std::printf("work_wait OK (I5): 20,000 fan-out waits on %u workers; process threads at most %u (base %u + %u), deepest stack %d items\n", threads, max_threads.load(), base, threads, g_nest_peak.load());
		}
		// A chain of 500 groups, each item waiting on the next one's, on one worker.
		{
			// The chain's depth is the stack it needs; ASan's frames are several times larger and
			// a macOS worker has 512 KB, so that build runs a shorter chain.
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
			constexpr int links = 150;
#else
			constexpr int links = 500;
#endif
			WorkPool pool({.threads = 1});
			std::vector<std::unique_ptr<WorkGroup>> chain;
			for (int i = 0; i < links; ++i) {
				chain.push_back(std::make_unique<WorkGroup>(pool));
			}
			std::atomic<int> done{0};
			struct Link {
				std::vector<std::unique_ptr<WorkGroup>>& chain;
				std::atomic<int>& done;
				void operator()(int i) {
					if (i + 1 < links) {
						chain[i + 1]->enqueue([this, i] { (*this)(i + 1); });
						chain[i + 1]->wait();
					}
					done.fetch_add(1);
				}
			} link{chain, done};
			chain[0]->enqueue([&] { link(0); });
			chain[0]->wait();
			assert(done.load() == links);
			std::printf("work_wait OK (I5): a chain of %d groups, each waited on from the previous one's item, finished on one worker\n", links);
		}
	}

	// ---- stop_requested() sees the innermost item of its own group even under another
	// group's item served inline. ----
	if (want("stop")) {
		Watchdog dog(30, "stop under inline");
		WorkPool pool({.threads = 1});
		WorkGroup g(pool);
		WorkGroup h(pool);
		std::atomic<bool> in_g{false};
		std::atomic<bool> release{false};
		std::atomic<int> seen{-1};
		g.enqueue([&] {
			in_g = true;
			h.enqueue([&] { seen = g.stop_requested() ? 1 : 0; });
			while (!release.load()) {
				std::this_thread::yield();
			}
			h.wait();
		});
		assert(wait_for([&] { return in_g.load(); }, 5000ms));
		g.cancel();
		release = true;
		g.wait();
		assert(seen.load() == 1);
		std::printf("work_wait OK: g.stop_requested() was true in an item of h served inline above g's cancelled item\n");
	}

	// ---- AL2. on_idle(): a notification runs once when the group is idle, on the thread
	// that made it idle, without a wait() (an event loop's thread passes the block guard
	// untouched). ----
	if (want("AL2")) {
		Watchdog dog(60, "on_idle");
		static std::atomic<int> guard_hits{0};
		static thread_local bool t_loop = false;
		t_loop = true;  // this thread stands for an event loop: its waits would hit the guard
		WorkPool pool({.threads = 2, .block_guard = [](const char*) noexcept { if (t_loop) { guard_hits.fetch_add(1); } }});
		WorkGroup g(pool);
		struct Node : WorkIdle {
			std::atomic<int> fired{0};
			std::atomic<std::thread::id> by{};
		};
		auto fn = [](WorkIdle* w) noexcept {
			auto* n = static_cast<Node*>(w);
			n->by = std::this_thread::get_id();
			n->fired.fetch_add(1);
		};
		// Joins g without wait(): an on_idle node and a flag.
		auto join = [&] {
			Node n;
			n.fn = fn;
			g.on_idle(n);
			assert(wait_for([&] { return n.fired.load() == 1; }, 10000ms));
		};
		// Already idle: runs at once, here.
		{
			Node n;
			n.fn = fn;
			g.on_idle(n);
			assert(n.fired.load() == 1 && n.by.load() == std::this_thread::get_id());
		}
		// Busy: runs after the last item, on a worker, once.
		{
			std::atomic<bool> release{false};
			std::atomic<bool> started{false};
			g.enqueue([&] {
				started = true;
				while (!release.load()) {
					std::this_thread::yield();
				}
			});
			assert(wait_for([&] { return started.load(); }, 5000ms));
			Node a, b, c;
			a.fn = b.fn = c.fn = fn;
			g.on_idle(a);
			g.on_idle(b);
			g.on_idle(c);
			assert(g.cancel_idle(b));  // removed before firing
			assert(!g.cancel_idle(b));
			std::this_thread::sleep_for(20ms);
			assert(a.fired.load() == 0 && c.fired.load() == 0);
			release = true;
			assert(wait_for([&] { return a.fired.load() == 1 && c.fired.load() == 1; }, 5000ms));
			assert(a.by.load() != std::this_thread::get_id());
			assert(b.fired.load() == 0);
			assert(!g.cancel_idle(a));  // already fired
			join();
			assert(a.fired.load() == 1 && c.fired.load() == 1);
		}
		// A cancel() that empties the queue of an otherwise idle group fires it.
		{
			std::atomic<bool> release{false};
			std::atomic<bool> started{false};
			g.enqueue([&] {
				started = true;
				while (!release.load()) {
					std::this_thread::yield();
				}
			});
			g.enqueue([] {});
			assert(wait_for([&] { return started.load(); }, 5000ms));
			Node n;
			n.fn = fn;
			g.on_idle(n);
			g.cancel();  // drops the queued one; the running one still holds the group
			assert(n.fired.load() == 0);
			release = true;
			assert(wait_for([&] { return n.fired.load() == 1; }, 5000ms));
			join();
		}
		// Many notifications under load: every node fires exactly once.
		{
			constexpr int nodes = 2000;
			std::vector<Node> list(nodes);
			std::atomic<bool> stop{false};
			std::thread poster([&] {
				while (!stop.load()) {
					g.enqueue([] { burn(20us); });
					std::this_thread::yield();
				}
			});
			for (auto& n : list) {
				n.fn = fn;
				g.on_idle(n);
				std::this_thread::yield();
			}
			stop = true;
			poster.join();
			join();
			assert(wait_for([&] {
				for (auto& n : list) {
					if (n.fired.load() != 1) {
						return false;
					}
				}
				return true;
			}, 10000ms));
		}
		t_loop = false;
		assert(guard_hits.load() == 0);
		std::printf("work_wait OK (AL2): on_idle fired at once on an idle group, once on the idle-making thread otherwise, 2,000 under load, cancel_idle held, the guard untouched\n");
	}

	// ---- I6. Progress on random acyclic graphs: 50 groups of widths 0 to 3, three classes,
	// items waiting on random lower groups, external and worker waiters, on 1, 2 and 4
	// workers. Every wait ends and no width is exceeded. ----
	if (want("I6")) {
		const char* env = std::getenv("WORK_WAIT_ITERATIONS");
		int iterations = env ? std::atoi(env) : 400;
		Watchdog dog(600, "I6 random graphs");
		std::uint64_t total = 0;
		for (int it = 0; it < iterations; ++it) {
			unsigned threads = it % 3 == 0 ? 1 : it % 3 == 1 ? 2 : 4;
			unsigned reserve = (it / 3) % 2 == 0 || threads == 1 ? 0 : 1;
			WorkPool pool({.threads = threads, .reserve = reserve});
			Graph graph(pool, static_cast<std::uint64_t>(it) + 1, 150);
			// A producer outside the pool posts roots while the main thread waits on the groups.
			std::thread producer([&] {
				for (unsigned k = 0; k < 40; ++k) {
					graph.post(static_cast<unsigned>(Graph::mix(graph.seed, k, 99) % graph_groups), 1000 + k * 7);
					if (k % 8 == 0) {
						std::this_thread::yield();
					}
				}
			});
			for (unsigned k = 0; k < 10; ++k) {
				graph.post(static_cast<unsigned>(graph_groups - 1 - k % 5), 5000 + k * 11);
			}
			producer.join();
			// Only after every item has run is every group idle; groups can post to lower ones
			// (each item's own waits), so wait from the top down until stable.
			for (int pass = 0; pass < 2; ++pass) {
				for (unsigned g = graph_groups; g-- > 0;) {
					graph.groups[g]->wait();
				}
			}
			assert(graph.peak_over.load() == 0);
			total += graph.ran.load();
		}
		std::printf("work_wait OK (I6): %d random acyclic graphs (50 groups, widths 0 to 3) on 1, 2 and 4 workers finished, widths held (%llu items)\n", iterations, static_cast<unsigned long long>(total));
	}

	std::printf("all work_wait tests passed\n");
	return 0;
}
