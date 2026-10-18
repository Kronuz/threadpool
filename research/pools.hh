// The scheduler candidates behind one interface, for the research harness. Tasks are
// caller-owned records (no allocation per task in any candidate but the baseline, whose
// std::function boxes are today's cost). Research code: off by default.
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common.hh"
#include "futex.hh"
#include "idle.hh"
#include "queues.hh"
#include "task_group.hh"
#include "threadpool.hh"

#if defined(RESEARCH_HAVE_TASKFLOW)
#include <taskflow/taskflow.hpp>
#endif
#if defined(RESEARCH_HAVE_MOODYCAMEL)
#include <blockingconcurrentqueue.h>
#endif

namespace research {

enum : std::uint8_t { PRIO_QUERY = 0, PRIO_FLUSH = 1, PRIO_MERGE = 2, PRIO_COUNT = 3 };

struct Group;

// A unit of work: the harness derives its payload from this. `next` links per-group FIFOs.
struct Task {
	void (*fn)(Task*) = nullptr;
	Group* group = nullptr;
	Task* next = nullptr;
	std::uint8_t prio = PRIO_FLUSH;
};

// What every candidate needs to know of a group: its concurrency cap (0: every worker).
struct Group {
	unsigned width = 0;
	virtual ~Group() = default;
};

inline thread_local const void* tls_pool = nullptr;
inline thread_local unsigned tls_worker = ~0u;

class Pool {
public:
	virtual ~Pool() = default;
	virtual const char* name() const = 0;
	virtual unsigned workers() const = 0;
	// A group with up to `width` tasks running at once (0: no cap); close() needs it idle.
	virtual Group* open(unsigned width = 0) = 0;
	virtual void close(Group* g) = 0;
	virtual void post(Task* t) = 0;
	virtual std::size_t group_bytes() const = 0;
	// The pool's internal state, for the harness watchdog when a run hangs.
	virtual void dump(FILE*) {}
	// Whether the calling thread is one of this pool's workers.
	bool on_worker() const noexcept { return tls_pool == this; }
};

////////////////////////////////////////////////////////////////////////////////
// P0: today's pool with TaskGroup (round-robin tickets), and the bare queue without groups.

class BaselinePool final : public Pool {
	struct G final : Group {
		TaskGroup<> tg;
		G(ThreadPool<>& p, unsigned w) : tg(p, w) {}
	};
	ThreadPool<> _pool;
	unsigned _n;

public:
	explicit BaselinePool(unsigned n) : _pool("R{:02}", n), _n(n) {}
	~BaselinePool() override {
		_pool.finish();
		_pool.join();
	}
	const char* name() const override { return "baseline"; }
	unsigned workers() const override { return _n; }
	void dump(FILE* f) override {
		std::fprintf(f, "  baseline: queued=%zu running=%zu workers=%zu\n", _pool.size(), _pool.running_size(), _pool.threadpool_workers());
	}
	Group* open(unsigned width) override { return new G(_pool, width); }
	void close(Group* g) override { delete static_cast<G*>(g); }
	void post(Task* t) override {
		static_cast<G*>(t->group)->tg.enqueue([t] { t->fn(t); });
	}
	std::size_t group_bytes() const override { return sizeof(G) + 200; }  // plus State's heap block
};

class RawPool final : public Pool {
	ThreadPool<> _pool;
	unsigned _n;

public:
	explicit RawPool(unsigned n) : _pool("Q{:02}", n), _n(n) {}
	~RawPool() override {
		_pool.finish();
		_pool.join();
	}
	const char* name() const override { return "raw"; }
	unsigned workers() const override { return _n; }
	void dump(FILE* f) override {
		std::fprintf(f, "  raw: queued=%zu running=%zu workers=%zu\n", _pool.size(), _pool.running_size(), _pool.threadpool_workers());
	}
	Group* open(unsigned width) override { auto* g = new Group; g->width = width; return g; }
	void close(Group* g) override { delete g; }
	void post(Task* t) override { _pool.enqueue([t] { t->fn(t); }); }
	std::size_t group_bytes() const override { return sizeof(Group); }
};

////////////////////////////////////////////////////////////////////////////////
// Shared worker-thread scaffolding: N threads, Tokio-style idle bookkeeping, a stop flag.

class ThreadedPool : public Pool {
protected:
	enum WorkerState : int { W_RUNNING = 0, W_SEARCHING = 1, W_PARKING = 2, W_PARKED = 3, W_EXITED = 4 };

	unsigned _n;
	Idle _idle;
	std::atomic<bool> _stop{false};
	std::vector<std::thread> _threads;
	std::unique_ptr<std::atomic<int>[]> _wstate;  // per worker, for the watchdog's dump
	unsigned _spin_us;

	explicit ThreadedPool(unsigned n, unsigned spin_us) : _n(n), _idle(n), _wstate(new std::atomic<int>[n]), _spin_us(spin_us) {
		for (unsigned i = 0; i < n; ++i) { _wstate[i].store(W_RUNNING, std::memory_order_relaxed); }
	}

	template <typename Self>
	void start(Self* self) {
		_threads.reserve(_n);
		for (unsigned i = 0; i < _n; ++i) {
			_threads.emplace_back([self, i] {
				self->run(i);
				self->_wstate[i].store(W_EXITED, std::memory_order_relaxed);
			});
		}
	}

	void dump_workers(FILE* f) {
		static const char* const names[] = {"run", "search", "parking", "parked", "exited"};
		std::fprintf(f, "  workers:");
		for (unsigned i = 0; i < _n; ++i) {
			int s = _wstate[i].load(std::memory_order_relaxed);
			std::fprintf(f, " %u=%s", i, s >= 0 && s <= 4 ? names[s] : "?");
		}
		std::fprintf(f, "\n");
	}

	void stop() {
		_stop.store(true, std::memory_order_release);
		_idle.notify_all();
		for (auto& th : _threads) { th.join(); }
	}

	// Parks worker idx; `searching` is updated. `pending` tells whether work exists (the
	// final check of the last searcher, and the spin phase). Returns false at shutdown.
	template <typename Pending>
	bool park(unsigned idx, bool& searching, Pending&& pending) {
		if (_stop.load(std::memory_order_acquire)) { return false; }
		_wstate[idx].store(W_PARKING, std::memory_order_relaxed);
		// Spin first, still unregistered: a burst arriving within spin_us costs no syscall.
		if (_spin_us) {
			auto end = now_ns() + std::uint64_t(_spin_us) * 1000;
			while (now_ns() < end) {
				if (pending()) { _wstate[idx].store(W_RUNNING, std::memory_order_relaxed); return true; }
				cpu_relax();
			}
		}
		bool final_check = false;
		auto observed = _idle.to_parked(idx, searching, &final_check);
		searching = false;
		// Registered now. A stop that ran before the registration unparked nobody here, so
		// the flag is checked again after it (to_parked's mutex orders the two); without this
		// a worker slept through stop() on the gate VM and join() never returned.
		bool stopping = _stop.load(std::memory_order_acquire);
		if (stopping || (final_check && pending())) {
			if (_idle.unpark_self(idx)) {
				_wstate[idx].store(W_RUNNING, std::memory_order_relaxed);
				if (stopping) { return false; }
				searching = _idle.to_searching();
				return true;
			}
			// Someone unparked us meanwhile: the park below returns at once.
		}
		_wstate[idx].store(W_PARKED, std::memory_order_relaxed);
		_idle.park(idx, observed);
		_wstate[idx].store(W_SEARCHING, std::memory_order_relaxed);
		searching = true;  // a notified worker searches first
		return !_stop.load(std::memory_order_acquire);
	}

	// Found work while searching: the last searcher wakes the next worker (gradual ramp-up).
	void found(bool& searching) {
		if (searching) {
			searching = false;
			if (_idle.from_searching()) { _idle.notify_one(); }
		}
	}

	void dump_idle(FILE* f) {
		std::fprintf(f, "  idle: searching=%u unparked=%u sleepers=%zu stop=%d\n", _idle.num_searching(), _idle.num_unparked(), _idle.num_sleepers(), int(_stop.load(std::memory_order_relaxed)));
		dump_workers(f);
	}

public:
	unsigned workers() const override { return _n; }
};

////////////////////////////////////////////////////////////////////////////////
// P1: per-worker Chase-Lev deques with stealing, a LIFO slot, an inject queue for external
// posts, Tokio's searching/parking rules. Groups are tags: no fairness, no priority.

class StealPool final : public ThreadedPool {
public:
	struct Options {
		bool lifo = true;          // the LIFO slot (at most 3 polls in a row)
		bool steal_half = true;
		unsigned spin_us = 0;
		unsigned inject_interval = 61;  // ticks between inject checks, or 200 µs of task time
		bool owner_lifo = false;   // the owner pops its deque LIFO (Tokio) or FIFO (Go's runq)
	};

private:
	struct alignas(64) Worker {
		ChaseLev<Task, 1024> deque;
		Task* lifo = nullptr;
		unsigned lifo_polls = 0;
		unsigned tick = 0;
		unsigned inject_tick = 0;
		std::uint64_t inject_time = 0;
		std::uint64_t rng = 0;
	};

	// The owner's take: FIFO costs one CAS per pop but keeps a self-reposting task from
	// holding its worker; LIFO is Tokio's.
	Task* take_own(Worker& w) {
		if (_opt.owner_lifo) { return w.deque.pop(); }
		for (int attempt = 0; attempt < 4 && !w.deque.empty(); ++attempt) {
			if (Task* t = w.deque.pop_front()) { return t; }
		}
		return nullptr;
	}
	std::vector<std::unique_ptr<Worker>> _workers;
	Mpmc<Task, 1 << 17> _inject;
	Options _opt;
	std::string _name;
	std::uint64_t _inject_cycles;  // 200 µs of task time, in cycle-counter ticks

	void inject_push(Task* t) {
		while (!_inject.push(t)) { std::this_thread::yield(); }
	}

	void push_local(Worker& w, Task* t) {
		if (!w.deque.push(t)) {
			auto half = w.deque.size() / 2;  // full: the oldest half spills to the inject queue
			for (std::int64_t i = 0; i < half; ++i) {
				if (Task* x = w.deque.steal()) { inject_push(x); }
			}
			if (!w.deque.push(t)) { inject_push(t); }
		}
		_idle.notify_one();
	}

	bool any_pending() {
		if (!_inject.empty()) { return true; }
		for (auto& w : _workers) { if (!w->deque.empty()) { return true; } }
		return false;
	}

	Task* steal(Worker& w, unsigned self) {
		for (unsigned pass = 0; pass < 2; ++pass) {
			unsigned start = static_cast<unsigned>(splitmix(w.rng) % _n);
			for (unsigned k = 0; k < _n; ++k) {
				unsigned v = (start + k) % _n;
				if (v == self) { continue; }
				Worker& victim = *_workers[v];
				Task* t = victim.deque.steal();
				if (!t) { continue; }
				if (_opt.steal_half) {
					auto extra = victim.deque.size() / 2;
					for (std::int64_t i = 0; i < extra; ++i) {
						Task* x = victim.deque.steal();
						if (!x) { break; }
						if (!w.deque.push(x)) { inject_push(x); break; }
					}
				}
				return t;
			}
			if (Task* t = _inject.pop()) { return t; }
		}
		return nullptr;
	}

public:
	explicit StealPool(unsigned n) : StealPool(n, Options{}) {}
	StealPool(unsigned n, Options opt) : ThreadedPool(n, opt.spin_us), _opt(opt) {
		_inject_cycles = static_cast<std::uint64_t>(200'000.0 * cycles_per_ns());
		_name = std::string("steal") + (opt.lifo ? "" : "-noslot") + (opt.owner_lifo ? "-lifo" : "") + (opt.steal_half ? "" : "-one") + (opt.spin_us ? "-spin" : "");
		_workers.reserve(n);
		for (unsigned i = 0; i < n; ++i) {
			_workers.emplace_back(std::make_unique<Worker>());
			_workers.back()->rng = 0x1234567ULL + i;
		}
		start(this);
	}
	~StealPool() override { stop(); }

	const char* name() const override { return _name.c_str(); }
	Group* open(unsigned width) override { auto* g = new Group; g->width = width; return g; }
	void close(Group* g) override { delete g; }
	std::size_t group_bytes() const override { return sizeof(Group); }
	void dump(FILE* f) override {
		dump_idle(f);
		std::fprintf(f, "  steal: inject=%zu deques:", _inject.size());
		for (auto& w : _workers) { std::fprintf(f, " %lld%s", static_cast<long long>(w->deque.size()), w->lifo ? "+L" : ""); }
		std::fprintf(f, "\n");
	}

	void post(Task* t) override {
		if (on_worker()) {
			Worker& w = *_workers[tls_worker];
			if (_opt.lifo) {
				Task* prev = w.lifo;
				w.lifo = t;
				if (prev) { push_local(w, prev); }
				return;
			}
			push_local(w, t);
			return;
		}
		inject_push(t);
		_idle.notify_one();
	}

	void run(unsigned idx) {
		tls_pool = this;
		tls_worker = idx;
		Worker& w = *_workers[idx];
		bool searching = false;
		for (;;) {
			Task* t = nullptr;
			// The inject queue gets a turn every inject_interval tasks, or every 200 µs of
			// task time (the cycle counter, read per task), whichever comes first.
			++w.tick;
			bool inject_turn = w.tick - w.inject_tick >= _opt.inject_interval;
			if (!inject_turn) {
				auto c = cycles();
				inject_turn = c - w.inject_time >= _inject_cycles;
				if (inject_turn) { w.inject_time = c; }
			}
			if (inject_turn) {
				w.inject_tick = w.tick;
				t = _inject.pop();
			}
			if (!t && w.lifo && w.lifo_polls < 3) {
				t = w.lifo;
				w.lifo = nullptr;
				++w.lifo_polls;
			}
			if (!t) {
				w.lifo_polls = 0;
				t = take_own(w);
				if (!t && w.lifo) { t = w.lifo; w.lifo = nullptr; }
			}
			if (!t) { t = _inject.pop(); }
			if (!t) {
				if (!searching) { searching = _idle.to_searching(); }
				if (searching) { t = steal(w, idx); }
			}
			if (t) {
				found(searching);
				t->fn(t);
				continue;
			}
			if (!park(idx, searching, [this] { return any_pending(); })) { break; }
		}
	}
};

////////////////////////////////////////////////////////////////////////////////
// P2: per-group intrusive FIFOs, a ready-list of groups, deficit round robin across groups
// by service time (a quantum of 100 µs; the deficit carries, and resets when a group idles).
// A worker serves one group for a quantum at a time, so an index's tasks stay on one core
// while they last. Tickets let a group be on the ready-list up to its width, so one hot
// group takes every core when the others are idle.

class GroupPool : public ThreadedPool {
public:
	struct Options {
		std::uint64_t quantum_ns = 100'000;
		unsigned spin_us = 0;
	};

protected:
	struct SpinLock {
		std::atomic<bool> locked{false};
		void lock() noexcept {
			for (;;) {
				if (!locked.exchange(true, std::memory_order_acquire)) { return; }
				while (locked.load(std::memory_order_relaxed)) { cpu_relax(); }
			}
		}
		void unlock() noexcept { locked.store(false, std::memory_order_release); }
	};

	struct Fifo {
		Task* head = nullptr;
		Task* tail = nullptr;
		std::uint32_t pending = 0;
		std::uint32_t tickets = 0;
		std::int64_t deficit = 0;
		void push(Task* t) noexcept {
			t->next = nullptr;
			if (tail) { tail->next = t; } else { head = t; }
			tail = t;
			++pending;
		}
		Task* pop() noexcept {
			Task* t = head;
			if (t) {
				head = t->next;
				if (!head) { tail = nullptr; }
				--pending;
			}
			return t;
		}
	};

	struct alignas(64) G : Group {
		SpinLock mu;
		Fifo q[PRIO_COUNT];
		unsigned weight = 1;
	};

	struct ReadyList {
		Mpmc<G, 1 << 18> list;
		void push(G* g) {
			while (!list.push(g)) { std::this_thread::yield(); }
		}
	};

	ReadyList _ready[PRIO_COUNT];
	Options _opt;
	std::string _name;

	unsigned cap(const G& g) const noexcept { return g.width ? g.width : _n; }

	// Whether an idle worker could take one more ticket of class c.
	virtual bool spare_capacity(unsigned) const noexcept { return _idle.num_unparked() < _n; }

	bool any_ready() {
		for (auto& r : _ready) { if (!r.list.empty()) { return true; } }
		return false;
	}

	// Serves group g's class c for one quantum; requeues it if work remains.
	void serve(G* g, unsigned c) {
		Fifo& f = g->q[c];
		g->mu.lock();
		f.deficit += static_cast<std::int64_t>(_opt.quantum_ns) * g->weight;
		if (f.deficit > static_cast<std::int64_t>(_opt.quantum_ns) * g->weight) {
			f.deficit = static_cast<std::int64_t>(_opt.quantum_ns) * g->weight;
		}
		while (f.deficit > 0) {
			Task* t = f.pop();
			if (!t) {
				--f.tickets;
				f.deficit = 0;  // no credit accrues while idle (Seastar's rule)
				g->mu.unlock();
				return;
			}
			g->mu.unlock();
			auto t0 = now_ns();
			t->fn(t);
			auto dt = static_cast<std::int64_t>(now_ns() - t0);
			g->mu.lock();
			f.deficit -= dt;
		}
		// Quantum spent with work left: back to the tail (round robin); the ticket stays. With
		// idle workers and more work than tickets, one more ticket: an index alone fans out
		// to every core (doubling per quantum), busy indexes beside each other keep one.
		bool requeue = f.pending > 0;
		bool fan_out = requeue && f.pending > f.tickets && f.tickets < cap(*g) && spare_capacity(c);
		if (!requeue) { --f.tickets; }
		if (fan_out) { ++f.tickets; }
		g->mu.unlock();
		if (requeue) { _ready[c].push(g); }
		if (fan_out) {
			_ready[c].push(g);
			_idle.notify_one();
		}
	}

	// Picks the next group in class order; false when none is ready.
	virtual G* pick(unsigned& c) {
		for (unsigned k = 0; k < PRIO_COUNT; ++k) {
			if (G* g = _ready[k].list.pop()) { c = k; return g; }
		}
		return nullptr;
	}

public:
	explicit GroupPool(unsigned n) : GroupPool(n, Options{}, "groups") {}
	GroupPool(unsigned n, Options opt, const char* nm = "groups") : ThreadedPool(n, opt.spin_us), _opt(opt), _name(nm) {
		if (opt.quantum_ns != 100'000) { _name += "-q" + std::to_string(opt.quantum_ns / 1000) + "us"; }
		if (opt.spin_us) { _name += "-spin"; }
	}
	~GroupPool() override { stop(); }

	void begin() { start(this); }

	const char* name() const override { return _name.c_str(); }
	Group* open(unsigned width) override { auto* g = new G; g->width = width; return g; }
	// A ticket outlives the task that earned it (the worker drops it under the lock after the
	// task's completion is visible to the owner), so a group is freed only once no worker holds
	// one; freeing it earlier left a worker spinning on the freed spinlock for hours.
	void close(Group* gg) override {
		auto* g = static_cast<G*>(gg);
		for (;;) {
			g->mu.lock();
			bool busy = false;
			for (auto& f : g->q) { busy = busy || f.tickets != 0 || f.pending != 0; }
			g->mu.unlock();
			if (!busy) { break; }
			std::this_thread::yield();
		}
		delete g;
	}
	std::size_t group_bytes() const override { return sizeof(G); }
	void dump(FILE* f) override {
		dump_idle(f);
		std::fprintf(f, "  groups: ready=%zu/%zu/%zu\n", _ready[0].list.size(), _ready[1].list.size(), _ready[2].list.size());
	}

	void post(Task* t) override {
		G* g = static_cast<G*>(t->group);
		unsigned c = t->prio < PRIO_COUNT ? t->prio : PRIO_FLUSH;
		Fifo& f = g->q[c];
		bool ticket = false;
		g->mu.lock();
		f.push(t);
		// The first ticket always; more only while workers are idle (see serve()).
		if (f.tickets == 0 || (f.tickets < cap(*g) && f.tickets < f.pending && spare_capacity(c))) {
			++f.tickets;
			ticket = true;
		}
		g->mu.unlock();
		if (ticket) {
			_ready[c].push(g);
			_idle.notify_one();
		}
	}

	void run(unsigned idx) {
		tls_pool = this;
		tls_worker = idx;
		bool searching = false;
		for (;;) {
			unsigned c = 0;
			G* g = pick(c);
			if (!g && !searching) {
				searching = _idle.to_searching();
				if (searching) { g = pick(c); }
			}
			if (g) {
				found(searching);
				serve(g, c);
				continue;
			}
			if (!park(idx, searching, [this] { return any_ready(); })) { break; }
		}
	}
};

inline std::unique_ptr<GroupPool> make_group_pool(unsigned n, GroupPool::Options opt = GroupPool::Options{}) {
	auto p = std::make_unique<GroupPool>(n, opt);
	p->begin();
	return p;
}

////////////////////////////////////////////////////////////////////////////////
// P3: priority classes on P2: queries before flushes before merges, with a reservation: at
// most N - reserve workers run background classes at once, so a query never waits for a
// whole task to end on every core.

class PrioPool final : public GroupPool {
	unsigned _reserve;
	unsigned _merge_cap;
	alignas(64) std::atomic<unsigned> _busy_bg{0};
	alignas(64) std::atomic<unsigned> _busy_merge{0};
	std::string _nm;

	// Background classes fan out only onto workers beyond the reserve.
	bool spare_capacity(unsigned c) const noexcept override {
		auto unparked = _idle.num_unparked();
		if (c == PRIO_QUERY) { return unparked < _n; }
		if (c == PRIO_MERGE && _busy_merge.load(std::memory_order_relaxed) >= _merge_cap) { return false; }
		return unparked < _n - _reserve && _busy_bg.load(std::memory_order_relaxed) < _n - _reserve;
	}

	G* pick(unsigned& c) override {
		if (G* g = _ready[PRIO_QUERY].list.pop()) { c = PRIO_QUERY; return g; }
		if (_busy_bg.load(std::memory_order_relaxed) >= _n - _reserve) { return nullptr; }
		if (G* g = _ready[PRIO_FLUSH].list.pop()) { c = PRIO_FLUSH; return g; }
		if (_busy_merge.load(std::memory_order_relaxed) >= _merge_cap) { return nullptr; }
		if (G* g = _ready[PRIO_MERGE].list.pop()) { c = PRIO_MERGE; return g; }
		return nullptr;
	}

public:
	// reserve: workers kept free of background work; merge_cap: workers merges may hold.
	PrioPool(unsigned n, unsigned reserve, unsigned merge_cap, Options opt) : GroupPool(n, opt, "prio"), _reserve(reserve < n ? reserve : n - 1), _merge_cap(merge_cap ? merge_cap : n) {
		_nm = "prio-r" + std::to_string(_reserve) + "-m" + std::to_string(_merge_cap);
		if (opt.spin_us) { _nm += "-spin"; }
	}
	const char* name() const override { return _nm.c_str(); }
	void dump(FILE* f) override {
		GroupPool::dump(f);
		std::fprintf(f, "  prio: busy_bg=%u busy_merge=%u reserve=%u merge_cap=%u\n", _busy_bg.load(std::memory_order_relaxed), _busy_merge.load(std::memory_order_relaxed), _reserve, _merge_cap);
	}

	void run_prio(unsigned idx) {
		tls_pool = this;
		tls_worker = idx;
		bool searching = false;
		for (;;) {
			unsigned c = 0;
			G* g = pick(c);
			if (!g && !searching) {
				searching = _idle.to_searching();
				if (searching) { g = pick(c); }
			}
			if (g) {
				found(searching);
				if (c != PRIO_QUERY) { _busy_bg.fetch_add(1, std::memory_order_relaxed); }
				if (c == PRIO_MERGE) { _busy_merge.fetch_add(1, std::memory_order_relaxed); }
				serve(g, c);
				if (c == PRIO_MERGE) { _busy_merge.fetch_sub(1, std::memory_order_relaxed); }
				if (c != PRIO_QUERY) {
					_busy_bg.fetch_sub(1, std::memory_order_relaxed);
					// A reserved worker may now take background work: wake one.
					_idle.notify_one();
				}
				continue;
			}
			if (!park(idx, searching, [this] { return any_ready_allowed(); })) { break; }
		}
	}

	bool any_ready_allowed() {
		if (!_ready[PRIO_QUERY].list.empty()) { return true; }
		if (_busy_bg.load(std::memory_order_relaxed) >= _n - _reserve) { return false; }
		if (!_ready[PRIO_FLUSH].list.empty()) { return true; }
		if (_busy_merge.load(std::memory_order_relaxed) >= _merge_cap) { return false; }
		return !_ready[PRIO_MERGE].list.empty();
	}

	void begin_prio() {
		_threads.reserve(_n);
		for (unsigned i = 0; i < _n; ++i) {
			_threads.emplace_back([this, i] {
				run_prio(i);
				_wstate[i].store(W_EXITED, std::memory_order_relaxed);
			});
		}
	}
};

inline std::unique_ptr<PrioPool> make_prio_pool(unsigned n, unsigned reserve, unsigned merge_cap, GroupPool::Options opt = GroupPool::Options{}) {
	auto p = std::make_unique<PrioPool>(n, reserve, merge_cap, opt);
	p->begin_prio();
	return p;
}

////////////////////////////////////////////////////////////////////////////////
// P4: the assumption breaker. A group has a home worker and its tasks go to that worker's
// inbox, poster regardless, so an index's work runs on the core that holds its data. Workers
// steal (half) only when idle, from the fullest inbox, and may re-home the stolen group.

class HomePool final : public ThreadedPool {
public:
	struct Options {
		bool rehome = true;
		unsigned spin_us = 0;
	};

private:
	struct alignas(64) Worker {
		Mpmc<Task, 1 << 14> inbox;
		std::uint64_t rng = 0;
	};
	struct alignas(64) G : Group {
		std::atomic<unsigned> home{0};
	};
	std::vector<std::unique_ptr<Worker>> _workers;
	Mpmc<Task, 1 << 17> _overflow;
	std::atomic<unsigned> _next_home{0};
	Options _opt;
	std::string _name;

	bool any_pending() {
		if (!_overflow.empty()) { return true; }
		for (auto& w : _workers) { if (!w->inbox.empty()) { return true; } }
		return false;
	}

	Task* steal(Worker& w, unsigned self) {
		// The fullest inbox is the victim; take half of it.
		unsigned victim = ~0u;
		std::size_t best = 0;
		for (unsigned v = 0; v < _n; ++v) {
			if (v == self) { continue; }
			auto s = _workers[v]->inbox.size();
			if (s > best) { best = s; victim = v; }
		}
		if (victim == ~0u) { return _overflow.pop(); }
		Worker& vw = *_workers[victim];
		Task* t = vw.inbox.pop();
		if (!t) { return _overflow.pop(); }
		if (_opt.rehome) {
			auto* g = static_cast<G*>(t->group);
			unsigned h = victim;
			g->home.compare_exchange_strong(h, self, std::memory_order_relaxed);
		}
		auto extra = best / 2;
		for (std::size_t i = 0; i < extra; ++i) {
			Task* x = vw.inbox.pop();
			if (!x) { break; }
			if (!w.inbox.push(x)) { while (!_overflow.push(x)) { std::this_thread::yield(); } break; }
		}
		return t;
	}

public:
	explicit HomePool(unsigned n) : HomePool(n, Options{}) {}
	HomePool(unsigned n, Options opt) : ThreadedPool(n, opt.spin_us), _opt(opt) {
		_name = std::string("home") + (opt.rehome ? "" : "-fixed") + (opt.spin_us ? "-spin" : "");
		_workers.reserve(n);
		for (unsigned i = 0; i < n; ++i) {
			_workers.emplace_back(std::make_unique<Worker>());
			_workers.back()->rng = 0x9876543ULL + i;
		}
		start(this);
	}
	~HomePool() override { stop(); }

	const char* name() const override { return _name.c_str(); }
	Group* open(unsigned width) override {
		auto* g = new G;
		g->width = width;
		g->home.store(_next_home.fetch_add(1, std::memory_order_relaxed) % _n, std::memory_order_relaxed);
		return g;
	}
	void close(Group* g) override { delete static_cast<G*>(g); }
	std::size_t group_bytes() const override { return sizeof(G); }
	void dump(FILE* f) override {
		dump_idle(f);
		std::fprintf(f, "  home: overflow=%zu inboxes:", _overflow.size());
		for (auto& w : _workers) { std::fprintf(f, " %zu", w->inbox.size()); }
		std::fprintf(f, "\n");
	}

	void post(Task* t) override {
		auto* g = static_cast<G*>(t->group);
		unsigned h = g->home.load(std::memory_order_relaxed);
		if (!_workers[h]->inbox.push(t)) {
			while (!_overflow.push(t)) { std::this_thread::yield(); }
		}
		if (!_idle.unpark_if_parked(h)) { _idle.notify_one(); }
	}

	void run(unsigned idx) {
		tls_pool = this;
		tls_worker = idx;
		Worker& w = *_workers[idx];
		bool searching = false;
		for (;;) {
			Task* t = w.inbox.pop();
			if (!t) { t = _overflow.pop(); }
			if (!t) {
				if (!searching) { searching = _idle.to_searching(); }
				if (searching) { t = steal(w, idx); }
			}
			if (t) {
				found(searching);
				t->fn(t);
				continue;
			}
			if (!park(idx, searching, [this] { return any_pending(); })) { break; }
		}
	}
};

////////////////////////////////////////////////////////////////////////////////
// References: Taskflow's executor and a moodycamel BlockingConcurrentQueue with plain workers.

#if defined(RESEARCH_HAVE_TASKFLOW)
class TaskflowPool final : public Pool {
	tf::Executor _ex;
	unsigned _n;

public:
	explicit TaskflowPool(unsigned n) : _ex(n), _n(n) {}
	~TaskflowPool() override { _ex.wait_for_all(); }
	const char* name() const override { return "taskflow"; }
	unsigned workers() const override { return _n; }
	Group* open(unsigned width) override { auto* g = new Group; g->width = width; return g; }
	void close(Group* g) override { delete g; }
	void post(Task* t) override { _ex.silent_async([t] { t->fn(t); }); }
	std::size_t group_bytes() const override { return sizeof(Group); }
};
#endif

#if defined(RESEARCH_HAVE_MOODYCAMEL)
class MoodycamelPool final : public Pool {
	moodycamel::BlockingConcurrentQueue<Task*> _q;
	std::vector<std::thread> _threads;
	unsigned _n;

public:
	explicit MoodycamelPool(unsigned n) : _n(n) {
		for (unsigned i = 0; i < n; ++i) {
			_threads.emplace_back([this] {
				tls_pool = this;
				for (;;) {
					Task* t = nullptr;
					_q.wait_dequeue(t);
					if (!t) { return; }
					t->fn(t);
				}
			});
		}
	}
	~MoodycamelPool() override {
		for (unsigned i = 0; i < _n; ++i) { _q.enqueue(nullptr); }
		for (auto& th : _threads) { th.join(); }
	}
	const char* name() const override { return "moodycamel"; }
	unsigned workers() const override { return _n; }
	Group* open(unsigned width) override { auto* g = new Group; g->width = width; return g; }
	void close(Group* g) override { delete g; }
	void post(Task* t) override { _q.enqueue(t); }
	std::size_t group_bytes() const override { return sizeof(Group); }
};
#endif

////////////////////////////////////////////////////////////////////////////////

inline std::unique_ptr<Pool> make_pool(const std::string& name, unsigned n) {
	if (name == "baseline") { return std::make_unique<BaselinePool>(n); }
	if (name == "raw") { return std::make_unique<RawPool>(n); }
	if (name == "steal") { return std::make_unique<StealPool>(n); }
	if (name == "steal-noslot") { return std::make_unique<StealPool>(n, StealPool::Options{false, true, 0, 61, false}); }
	if (name == "steal-lifo") { return std::make_unique<StealPool>(n, StealPool::Options{true, true, 0, 61, true}); }
	if (name == "steal-one") { return std::make_unique<StealPool>(n, StealPool::Options{true, false, 0, 61, false}); }
	if (name == "steal-spin") { return std::make_unique<StealPool>(n, StealPool::Options{true, true, 50, 61, false}); }
	if (name == "groups") { return make_group_pool(n); }
	if (name == "groups-spin") { return make_group_pool(n, GroupPool::Options{100'000, 50}); }
	if (name == "groups-q1ms") { return make_group_pool(n, GroupPool::Options{1'000'000, 0}); }
	if (name == "prio") { return make_prio_pool(n, 1, n - 1); }
	if (name == "prio-r0") { return make_prio_pool(n, 0, n); }
	if (name == "prio-r2") { return make_prio_pool(n, 2, n - 2); }
	if (name == "home") { return std::make_unique<HomePool>(n); }
	if (name == "home-fixed") { return std::make_unique<HomePool>(n, HomePool::Options{false, 0}); }
	if (name == "home-spin") { return std::make_unique<HomePool>(n, HomePool::Options{true, 50}); }
#if defined(RESEARCH_HAVE_TASKFLOW)
	if (name == "taskflow") { return std::make_unique<TaskflowPool>(n); }
#endif
#if defined(RESEARCH_HAVE_MOODYCAMEL)
	if (name == "moodycamel") { return std::make_unique<MoodycamelPool>(n); }
#endif
	std::fprintf(stderr, "unknown pool: %s\n", name.c_str());
	std::exit(2);
}

}  // namespace research
