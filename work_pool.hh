/*
 * Copyright (c) 2026 Germán Méndez Bravo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// WorkPool: one shared pool for many owners. Each owner is a WorkGroup with its own FIFO;
// groups wait their turn on a ready ring and are served by deficit round robin over
// service time, so one group alone takes every worker and many busy groups share them
// equally. Items are caller-owned (no allocation per post), and idle workers sleep on a
// futex each. The idle protocol is Tokio's (tokio-rs/tokio, runtime/scheduler/
// multi_thread/idle.rs, MIT); the ring is Dmitry Vyukov's bounded MPMC queue
// (1024cores.net, BSD-2-Clause). See ARCHITECTURE.md, "WorkPool".
#pragma once

#include <atomic>                // for std::atomic
#include <chrono>                // for std::chrono
#include <cstddef>               // for std::size_t
#include <cstdint>               // for std::uint32_t, std::uint64_t, std::int64_t
#include <format>                // for std::vformat, std::make_format_args
#include <memory>                // for std::unique_ptr
#include <mutex>                 // for std::mutex, std::lock_guard
#include <new>                   // for std::nothrow
#include <string>                // for std::string
#include <thread>                // for std::thread, std::this_thread
#include <type_traits>           // for std::decay_t, std::is_invocable_v
#include <utility>               // for std::forward, std::move
#include <vector>                // for std::vector

#include "thread.hh"             // for Thread, ThreadPolicyType

#ifdef THREADPOOL_TRACE_HEADER
#  include THREADPOOL_TRACE_HEADER
#else
#  include "threadpool_trace.h"
#endif

#if defined(__linux__)
#  include <climits>             // for INT_MAX
#  include <ctime>               // for timespec
#  include <linux/futex.h>       // for FUTEX_WAIT_PRIVATE, FUTEX_WAKE_PRIVATE
#  include <sys/syscall.h>       // for SYS_futex
#  include <unistd.h>            // for syscall
#elif defined(__APPLE__)
#  include <Availability.h>      // for __MAC_OS_X_VERSION_MIN_REQUIRED
#  if defined(__MAC_OS_X_VERSION_MIN_REQUIRED) && __MAC_OS_X_VERSION_MIN_REQUIRED >= 140400
#    include <os/clock.h>        // for OS_CLOCK_MACH_ABSOLUTE_TIME
#    include <os/os_sync_wait_on_address.h>
#    define THREADPOOL_OS_SYNC 1
#  else
extern "C" int __ulock_wait(std::uint32_t operation, void* addr, std::uint64_t value, std::uint32_t timeout_us);
extern "C" int __ulock_wake(std::uint32_t operation, void* addr, std::uint64_t wake_value);
#  endif
#elif defined(__FreeBSD__)
#  include <climits>             // for INT_MAX
#  include <ctime>               // for CLOCK_MONOTONIC
#  include <sys/types.h>
#  include <sys/umtx.h>          // for _umtx_op, struct _umtx_time
#endif


class WorkPool;
class WorkGroup;


// A unit of work, owned by the caller: the pool links it while queued and calls run once.
// It must stay alive until run starts; run may destroy or repost it.
struct WorkItem {
	void (*run)(WorkItem*) noexcept = nullptr;
	WorkGroup* group = nullptr;  // nullptr: the pool's own group
	WorkItem* next = nullptr;    // the pool's while queued; cancel()'s list after
	std::uint32_t epoch = 0;     // the pool's: the group's epoch when the item started
	std::uint8_t flags = 0;      // the pool's

	// Whether the item's group was cancelled after the item started; call it from run.
	bool stop_requested() const noexcept;
};


namespace threadpool_detail {

// The platform futex on a 32-bit word: Linux futex(2), macOS os_sync_wait_on_address (or
// __ulock before 14.4), FreeBSD _umtx_op, else C++20 atomic wait. Spurious returns allowed.
inline void futex_wait(std::atomic<std::uint32_t>* addr, std::uint32_t expected) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(THREADPOOL_OS_SYNC)
	os_sync_wait_on_address(addr, expected, sizeof(std::uint32_t), OS_SYNC_WAIT_ON_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wait(1 /* UL_COMPARE_AND_WAIT */ | 0x01000000 /* ULF_NO_ERRNO */, addr, expected, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, nullptr, nullptr);
#else
	addr->wait(expected, std::memory_order_relaxed);
#endif
}

// As futex_wait, for at most ns nanoseconds.
inline void futex_wait_for(std::atomic<std::uint32_t>* addr, std::uint32_t expected, std::uint64_t ns) noexcept {
#if defined(__linux__)
	timespec ts{static_cast<time_t>(ns / 1'000'000'000u), static_cast<long>(ns % 1'000'000'000u)};
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, &ts, nullptr, 0);
#elif defined(__APPLE__) && defined(THREADPOOL_OS_SYNC)
	os_sync_wait_on_address_with_timeout(addr, expected, sizeof(std::uint32_t), OS_SYNC_WAIT_ON_ADDRESS_NONE, OS_CLOCK_MACH_ABSOLUTE_TIME, ns);
#elif defined(__APPLE__)
	auto us = ns / 1000 + 1;
	__ulock_wait(1 | 0x01000000, addr, expected, us < 0xffffffffu ? static_cast<std::uint32_t>(us) : 0xffffffffu);
#elif defined(__FreeBSD__)
	struct _umtx_time tm{};
	tm._timeout.tv_sec = static_cast<time_t>(ns / 1'000'000'000u);
	tm._timeout.tv_nsec = static_cast<long>(ns % 1'000'000'000u);
	tm._flags = 0;  // relative
	tm._clockid = CLOCK_MONOTONIC;
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, reinterpret_cast<void*>(sizeof tm), &tm);
#else
	if (addr->load(std::memory_order_relaxed) == expected) {
		std::this_thread::sleep_for(std::chrono::nanoseconds(ns < 1'000'000 ? ns : 1'000'000));
	}
#endif
}

inline void futex_wake_one(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(THREADPOOL_OS_SYNC)
	os_sync_wake_by_address_any(addr, sizeof(std::uint32_t), OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, 1, nullptr, nullptr);
#else
	addr->notify_one();
#endif
}

inline void futex_wake_all(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(THREADPOOL_OS_SYNC)
	os_sync_wake_by_address_all(addr, sizeof(std::uint32_t), OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000 | 0x00000100 /* ULF_WAKE_ALL */, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, INT_MAX, nullptr, nullptr);
#else
	addr->notify_all();
#endif
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	__asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
	__asm__ __volatile__("isb" ::: "memory");
#else
	std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

// The cycle counter: a few nanoseconds per read where steady_clock can cost microseconds
// (31 µs under a VM's hpet clocksource). Only differences on one thread are used.
inline std::uint64_t ticks() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	return __builtin_ia32_rdtsc();
#elif defined(__aarch64__)
	std::uint64_t v;
	__asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(v));
	return v;
#else
	return static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Ticks per nanosecond, measured once per process.
inline double ticks_per_ns() noexcept {
	static const double rate = [] {
#if defined(__aarch64__)
		std::uint64_t freq;
		__asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(freq));
		return double(freq) / 1e9;
#elif defined(__x86_64__) || defined(__i386__)
		// Both clocks run while the thread sleeps, so a preempted calibration stays right.
		auto t0 = std::chrono::steady_clock::now();
		auto c0 = ticks();
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
		auto c1 = ticks();
		auto t1 = std::chrono::steady_clock::now();
		auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
		return ns > 0 && c1 > c0 ? double(c1 - c0) / double(ns) : 1.0;
#else
		using period = std::chrono::steady_clock::period;
		return double(period::den) / double(period::num) / 1e9;
#endif
	}();
	return rate;
}

// Test-and-test-and-set; yields after a while so a preempted holder can finish.
class SpinLock {
	std::atomic<bool> _locked{false};

public:
	void lock() noexcept {
		for (unsigned spins = 0;; ++spins) {
			// acquire: the critical section reads what the last holder wrote before unlock().
			if (!_locked.exchange(true, std::memory_order_acquire)) {
				return;
			}
			while (_locked.load(std::memory_order_relaxed)) {
				if (++spins < 64) {
					cpu_relax();
				} else {
					std::this_thread::yield();
				}
			}
		}
	}

	void unlock() noexcept {
		// release: publishes the critical section to the next lock().
		_locked.store(false, std::memory_order_release);
	}
};

// One worker's sleep: park() returns once an unpark() comes after the epoch it observed.
struct alignas(64) Parker {
	std::atomic<std::uint32_t> seq{0};

	void park(std::uint32_t observed) noexcept {
		// acquire: pairs with unpark()'s release, so the waker's writes are visible on wake.
		while (seq.load(std::memory_order_acquire) == observed) {
			futex_wait(&seq, observed);
		}
	}

	void unpark() noexcept {
		seq.fetch_add(1, std::memory_order_release);
		futex_wake_one(&seq);
	}
};

// Tokio's idle bookkeeping: one word counts searching workers (low half) and unparked
// workers (high half); sleepers wait in a LIFO list under a mutex, so the warmest wakes
// first. Only parking and waking take the mutex; a post that wakes nobody reads one word.
class Idle {
	static constexpr std::uint64_t one_searching = 1;
	static constexpr std::uint64_t one_unparked = std::uint64_t(1) << 32;

	alignas(64) std::atomic<std::uint64_t> _state;
	std::mutex _mu;
	std::vector<unsigned> _sleepers;  // under _mu
	std::unique_ptr<Parker[]> _parkers;
	unsigned _n;

	static unsigned searching(std::uint64_t s) noexcept { return static_cast<unsigned>(s & 0xffffffffu); }
	static unsigned unparked(std::uint64_t s) noexcept { return static_cast<unsigned>(s >> 32); }

	// Under _mu: removes idx from the sleepers; false when it is not there.
	bool remove(unsigned idx) noexcept {
		for (std::size_t i = 0; i < _sleepers.size(); ++i) {
			if (_sleepers[i] == idx) {
				_sleepers[i] = _sleepers.back();
				_sleepers.pop_back();
				return true;
			}
		}
		return false;
	}

public:
	explicit Idle(unsigned n) :
		_state(static_cast<std::uint64_t>(n) << 32),
		_parkers(new Parker[n]),
		_n(n) {
		_sleepers.reserve(n);
	}

	unsigned num_unparked() const noexcept { return unparked(_state.load(std::memory_order_relaxed)); }

	// After a post: wakes one sleeper when nobody searches. The post must be a seq_cst RMW
	// the parker's final check reads, so this load and to_parked()'s RMW are in one total
	// order: either the post sees the parker or the parker sees the post.
	bool notify_one() noexcept {
		auto s = _state.load(std::memory_order_seq_cst);
		if (searching(s) != 0 || unparked(s) >= _n) {
			return false;
		}
		unsigned idx;
		{
			std::lock_guard<std::mutex> lk(_mu);
			s = _state.load(std::memory_order_seq_cst);
			if (searching(s) != 0 || _sleepers.empty()) {
				return false;
			}
			idx = _sleepers.back();
			_sleepers.pop_back();
			// The woken worker starts searching; counting it now keeps racing posts from waking more.
			_state.fetch_add(one_searching | one_unparked, std::memory_order_seq_cst);
		}
		_parkers[idx].unpark();
		return true;
	}

	// Wakes every sleeper (shutdown).
	void notify_all() noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		for (auto idx : _sleepers) {
			_state.fetch_add(one_searching | one_unparked, std::memory_order_seq_cst);
			_parkers[idx].unpark();
		}
		_sleepers.clear();
	}

	// False when half the workers already search; the relaxed read makes the cap approximate.
	bool to_searching() noexcept {
		if (2 * searching(_state.load(std::memory_order_relaxed)) >= _n) {
			return false;
		}
		// seq_cst: posts that read this count skip their wake, so it must be in the same order.
		_state.fetch_add(one_searching, std::memory_order_seq_cst);
		return true;
	}

	// True when the caller was the last searcher: it must wake another (Tokio's ramp-up).
	bool from_searching() noexcept {
		return searching(_state.fetch_sub(one_searching, std::memory_order_seq_cst)) == 1;
	}

	// Registers idx as a sleeper and returns the epoch to park on. *final_check: the caller
	// was the last searcher and must look for work once more before parking.
	std::uint32_t to_parked(unsigned idx, bool was_searching, bool* final_check) noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		auto observed = _parkers[idx].seq.load(std::memory_order_relaxed);
		_sleepers.push_back(idx);
		auto prev = _state.fetch_sub(one_unparked + (was_searching ? one_searching : 0), std::memory_order_seq_cst);
		*final_check = was_searching && searching(prev) == 1;
		return observed;
	}

	// Takes idx back out of the sleepers (it found work after registering); false when a
	// waker already took it, so its park() returns at once.
	bool unpark_self(unsigned idx) noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		if (!remove(idx)) {
			return false;
		}
		_state.fetch_add(one_unparked, std::memory_order_seq_cst);
		return true;
	}

	void park(unsigned idx, std::uint32_t observed) noexcept { _parkers[idx].park(observed); }
};

// Bounded backoff for a helper with nothing to run: spin, then yield, then short sleeps.
inline void backoff(unsigned& round) noexcept {
	if (round < 16) {
		cpu_relax();
	} else if (round < 32) {
		std::this_thread::yield();
	} else {
		unsigned us = round - 32 < 10 ? 1u << (round - 32) : 1000u;
		std::this_thread::sleep_for(std::chrono::microseconds(us < 1000 ? us : 1000));
	}
	++round;
}

inline constexpr std::uint8_t item_boxed = 1;

// A boxed callable (WorkGroup::enqueue): destroy() frees it without running it.
struct BoxedItem : WorkItem {
	void (*destroy)(BoxedItem*) noexcept = nullptr;
};

// One group's queue and its place in line. Everything here is under the group's lock.
struct Lane {
	WorkItem* head = nullptr;
	WorkItem* tail = nullptr;
	std::uint32_t pending = 0;   // queued items
	std::uint32_t tickets = 0;   // turns on the ready ring or being served from it
	std::uint32_t listed = 0;    // of those, on the ring now
	std::int64_t deficit = 0;    // service owed back, in ticks (DRR's counter; <= 0)

	void push(WorkItem* item) noexcept {
		item->next = nullptr;
		if (tail) {
			tail->next = item;
		} else {
			head = item;
		}
		tail = item;
		++pending;
	}

	WorkItem* pop() noexcept {
		WorkItem* item = head;
		if (item) {
			head = item->next;
			if (!head) {
				tail = nullptr;
			}
			--pending;
		}
		return item;
	}
};

// A group's state. WorkGroup owns it until its destructor; then the last ticket off the
// ring frees it, so a group closes without waiting for its turn to come around.
struct alignas(64) GroupState {
	SpinLock mu;
	Lane lane;                   // under mu
	std::uint32_t running = 0;   // items executing, under mu
	std::uint32_t helpers = 0;   // waiting threads serving it outside the ring, under mu
	std::uint32_t waiters = 0;   // threads in WorkGroup::wait(), under mu
	unsigned width = 0;          // under mu; 0: no cap
	unsigned weight = 1;         // quanta per turn, under mu
	bool owned = true;           // under mu; false once the WorkGroup is gone
	std::atomic<std::uint32_t> epoch{0};  // cancel() bumps it
	std::atomic<std::uint32_t> sleep{0};  // sleep_until()'s futex word; cancel() bumps it
};

// The ready-list: Vyukov's bounded MPMC ring of group pointers, chained so it grows as
// groups open without a post ever blocking: a larger ring becomes the tail, consumers
// drain the old one first, and retired rings stay allocated until the pool ends (at most
// as much again as the live one). Every entry is one listed ticket, so the ring needs
// (live groups + extra tickets) entries; reserve() keeps it that large.
class ReadyRing {
	static constexpr std::size_t closed = std::size_t(1) << (sizeof(std::size_t) * 8 - 1);

	struct Cell {
		std::atomic<std::size_t> seq;
		GroupState* data;
	};

	struct Ring {
		std::size_t mask;
		std::unique_ptr<Cell[]> cells;
		alignas(64) std::atomic<std::size_t> enq{0};  // the top bit closes the ring to pushes
		alignas(64) std::atomic<std::size_t> deq{0};
		alignas(64) std::atomic<Ring*> next{nullptr};

		explicit Ring(std::size_t cap) :
			mask(cap - 1),
			cells(new Cell[cap]) {
			for (std::size_t i = 0; i < cap; ++i) {
				cells[i].seq.store(i, std::memory_order_relaxed);
				cells[i].data = nullptr;
			}
		}

		// False when closed (or, against the sizing, full): retry on the tail.
		bool push(GroupState* x) noexcept {
			auto pos = enq.load(std::memory_order_relaxed);
			for (;;) {
				if (pos & closed) {
					return false;
				}
				Cell& c = cells[pos & mask];
				// acquire: the slot's last consumer is done with it.
				auto seq = c.seq.load(std::memory_order_acquire);
				auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
				if (diff == 0) {
					// seq_cst: the post's half of the Dekker pair with a parker's final check
					// (Idle::notify_one); on failure pos reloads, closed bit included.
					if (enq.compare_exchange_weak(pos, pos + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
						c.data = x;
						// release: publishes data to the consumer that acquires seq.
						c.seq.store(pos + 1, std::memory_order_release);
						return true;
					}
				} else if (diff < 0) {
					return false;
				} else {
					pos = enq.load(std::memory_order_relaxed);
				}
			}
		}

		GroupState* pop() noexcept {
			auto pos = deq.load(std::memory_order_relaxed);
			for (;;) {
				Cell& c = cells[pos & mask];
				// acquire: pairs with push()'s release of seq, so data is visible.
				auto seq = c.seq.load(std::memory_order_acquire);
				auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
				if (diff == 0) {
					// relaxed: the cell's seq carries the ordering, not deq.
					if (deq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
						GroupState* x = c.data;
						// release: hands the slot back to the producer a lap later.
						c.seq.store(pos + mask + 1, std::memory_order_release);
						return x;
					}
				} else if (diff < 0) {
					return nullptr;  // empty, or the next entry is still being published
				} else {
					pos = deq.load(std::memory_order_relaxed);
				}
			}
		}
	};

	alignas(64) std::atomic<Ring*> _head;   // consumers start here
	alignas(64) std::atomic<Ring*> _tail;   // producers push here
	std::mutex _grow;
	std::vector<std::unique_ptr<Ring>> _rings;  // under _grow

	static std::size_t pow2(std::size_t n) noexcept {
		std::size_t cap = 64;
		while (cap < n) {
			cap <<= 1;
		}
		return cap;
	}

public:
	explicit ReadyRing(std::size_t entries) {
		_rings.push_back(std::make_unique<Ring>(pow2(entries)));
		_head.store(_rings.back().get(), std::memory_order_relaxed);
		_tail.store(_rings.back().get(), std::memory_order_relaxed);
	}

	void push(GroupState* x) noexcept {
		for (;;) {
			// acquire: pairs with reserve()'s publication of a new tail.
			if (_tail.load(std::memory_order_acquire)->push(x)) {
				return;
			}
			cpu_relax();  // closed: reserve() already published its successor as the tail
		}
	}

	GroupState* pop() noexcept {
		Ring* r = _head.load(std::memory_order_acquire);
		while (r) {
			if (GroupState* x = r->pop()) {
				return x;
			}
			Ring* next = r->next.load(std::memory_order_acquire);
			if (!next) {
				return nullptr;
			}
			// A closed ring whose every claimed entry was taken is done: move the head past it.
			auto e = r->enq.load(std::memory_order_acquire);
			if ((e & closed) && r->deq.load(std::memory_order_acquire) >= (e & ~closed)) {
				_head.compare_exchange_strong(r, next, std::memory_order_acq_rel, std::memory_order_acquire);
			}
			r = next;  // the new ring takes posts while the old one drains
		}
		return nullptr;
	}

	// The parker's final check. seq_cst: the parker's half of the Dekker pair with a post's
	// enq CAS (see Ring::push).
	bool empty() const noexcept {
		for (Ring* r = _head.load(std::memory_order_seq_cst); r; r = r->next.load(std::memory_order_seq_cst)) {
			if ((r->enq.load(std::memory_order_seq_cst) & ~closed) > r->deq.load(std::memory_order_seq_cst)) {
				return false;
			}
		}
		return true;
	}

	// Entries now; approximate.
	std::size_t size() const noexcept {
		std::size_t n = 0;
		for (Ring* r = _head.load(std::memory_order_acquire); r; r = r->next.load(std::memory_order_acquire)) {
			auto e = r->enq.load(std::memory_order_relaxed) & ~closed;
			auto d = r->deq.load(std::memory_order_relaxed);
			n += e > d ? e - d : 0;
		}
		return n;
	}

	// Grows the ring to hold at least `entries`. Called when a group opens, before it posts.
	void reserve(std::size_t entries) {
		std::lock_guard<std::mutex> lk(_grow);
		Ring* t = _tail.load(std::memory_order_relaxed);
		if (t->mask + 1 >= entries) {
			return;
		}
		_rings.push_back(std::make_unique<Ring>(pow2(2 * entries)));
		Ring* n = _rings.back().get();
		// seq_cst: a parker's empty() that sees the closed ring must also see its successor.
		t->next.store(n, std::memory_order_seq_cst);
		_tail.store(n, std::memory_order_seq_cst);
		t->enq.fetch_or(closed, std::memory_order_seq_cst);
	}
};

// What runs on this thread. One thread_local, so a worker resolves it once per item
// (macOS reaches each thread_local through a call).
struct Here {
	const WorkPool* pool = nullptr;   // the pool whose worker this is
	GroupState* state = nullptr;      // the group of the item running here
	WorkItem* item = nullptr;         // the item running here
};

inline thread_local Here tls_here;

class WorkPoolWorker : public Thread<WorkPoolWorker, ThreadPolicyType::regular> {
	WorkPool* _pool;
	unsigned _idx;
	std::string _name;

public:
	WorkPoolWorker(WorkPool* pool, unsigned idx, std::string name) :
		_pool(pool),
		_idx(idx),
		_name(std::move(name)) {}

	void operator()();

	const std::string& name() const noexcept { return _name; }
};

}  // namespace threadpool_detail


// A fixed set of named workers shared by many WorkGroups. Idle workers park on a futex
// each and use no CPU; post() never blocks or allocates.
class WorkPool {
	friend threadpool_detail::WorkPoolWorker;
	friend WorkGroup;
	using GroupState = threadpool_detail::GroupState;
	using Lane = threadpool_detail::Lane;

public:
	struct Options {
		unsigned threads = 0;              // 0: the hardware's concurrency
		std::chrono::nanoseconds quantum = std::chrono::microseconds(100);  // service per turn
		const char* name_format = "W{:02}";  // a std::format string over the worker index
	};

private:
	unsigned _n;
	std::int64_t _quantum;             // in ticks
	threadpool_detail::Idle _idle;
	threadpool_detail::ReadyRing _ready;
	std::atomic<bool> _stop{false};
	alignas(64) std::atomic<unsigned> _extras{0};   // tickets beyond each group's first; at most _n
	alignas(64) std::atomic<std::size_t> _states{1};  // group states alive, the pool's own included
	alignas(64) std::atomic<std::uint32_t> _group_wake{0};  // WorkGroup::wait()'s futex word
	GroupState _own;                   // items posted without a group

	std::vector<std::unique_ptr<threadpool_detail::WorkPoolWorker>> _workers;

	static unsigned resolve(unsigned threads) noexcept {
		if (threads) {
			return threads;
		}
		auto hw = std::thread::hardware_concurrency();
		return hw ? hw : 1;
	}

	GroupState& state_of(const WorkItem& item) noexcept;

	unsigned cap(const GroupState& s) const noexcept {
		return s.width && s.width < _n ? s.width : _n;
	}

	// Under s.mu: whether s may take one more ring ticket. Its first is free; more need
	// queued work beyond its tickets, room under its width, an idle worker, and one of the
	// pool's _n extras (more than one per worker could never run at once).
	bool grant(GroupState& s) noexcept {
		Lane& l = s.lane;
		if (l.tickets + s.helpers == 0) {
			return true;
		}
		if (l.pending <= l.tickets || l.tickets + s.helpers >= cap(s) || _idle.num_unparked() >= _n) {
			return false;
		}
		if (l.tickets == 0) {
			return true;  // a helper serves it; this is the ring's first
		}
		// relaxed: a counter for the ring's bound; the ticket itself is published under s.mu.
		if (_extras.fetch_add(1, std::memory_order_relaxed) >= _n) {
			_extras.fetch_sub(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}

	// Under s.mu: gives back one ring ticket.
	void drop(GroupState& s) noexcept {
		if (s.lane.tickets-- > 1) {
			_extras.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// Under s.mu, after an item finished or a cancel: wakes wait() callers that may be done.
	bool waiters_due(GroupState& s) noexcept {
		if (s.waiters && s.lane.pending == 0 && s.running <= 1) {
			// release: under s.mu anyway; the futex compares the word.
			_group_wake.fetch_add(1, std::memory_order_release);
			return true;
		}
		return false;
	}

	void release(GroupState* s, bool push, bool extra, bool wake, bool free) noexcept {
		if (push) {
			_ready.push(s);
		}
		if (extra) {
			_ready.push(s);
		}
		if (push || extra) {
			_idle.notify_one();
		}
		if (wake) {
			threadpool_detail::futex_wake_all(&_group_wake);
		}
		if (free) {
			delete s;
			_states.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	static void run_item(GroupState& s, WorkItem* item) noexcept {
		auto& here = threadpool_detail::tls_here;
		auto* prev_state = here.state;
		auto* prev_item = here.item;
		here.state = &s;
		here.item = item;
		item->run(item);
		here.state = prev_state;
		here.item = prev_item;
	}

	// Under s.mu with a ticket held (a ring ticket, or a helper's): one turn of s, a
	// quantum of service plus its debt, then requeue, keep, or drop the ticket. Returns
	// with s.mu released.
	void turn(GroupState& s, bool helper) noexcept {
		Lane& l = s.lane;
		std::int64_t budget = l.deficit + _quantum * s.weight;
		l.deficit = 0;
		bool wake = false;
		auto t = threadpool_detail::ticks();
		while (budget > 0) {
			WorkItem* item = l.pop();
			if (!item) {
				break;
			}
			++s.running;
			// relaxed: stop_requested() is advisory, read relaxed like TaskGroupStopToken.
			item->epoch = s.epoch.load(std::memory_order_relaxed);
			s.mu.unlock();
			run_item(s, item);
			auto now = threadpool_detail::ticks();
			budget -= static_cast<std::int64_t>(now - t);
			t = now;
			s.mu.lock();
			--s.running;
			wake |= waiters_due(s);
		}
		bool push = false;
		bool extra = false;
		if (helper) {
			--s.helpers;
		}
		if (l.pending == 0) {
			l.deficit = 0;  // DRR: an idle group keeps neither credit nor debt
			if (!helper) {
				drop(s);
			}
		} else {
			if (budget < 0) {
				l.deficit += budget;
			}
			if (helper) {
				// A helper's ticket never joins the ring, unless the group would have none.
				if (l.tickets == 0) {
					++l.tickets;
					++l.listed;
					push = true;
				}
			} else if (l.tickets > 1 && _ready.size() > l.listed) {
				drop(s);  // other groups wait: an extra ticket goes back, so shares even out
			} else {
				++l.listed;
				push = true;
				// One more per quantum while workers idle: an index alone fans out to every core.
				if (grant(s)) {
					++l.tickets;
					++l.listed;
					extra = true;
				}
			}
		}
		bool free = !s.owned && l.tickets == 0 && s.helpers == 0;
		s.mu.unlock();
		release(&s, push, extra, wake, free);
	}

	// A ring entry: one listed ticket of s.
	void serve(GroupState& s) noexcept {
		s.mu.lock();
		--s.lane.listed;
		turn(s, false);
	}

	// Runs one queued item of the caller's own group inline; false when there is none.
	bool help_own() noexcept {
		GroupState* s = threadpool_detail::tls_here.state;
		if (!s) {
			return false;
		}
		s->mu.lock();
		WorkItem* item = s->lane.pop();
		if (!item) {
			s->mu.unlock();
			return false;
		}
		++s->running;
		item->epoch = s->epoch.load(std::memory_order_relaxed);
		s->mu.unlock();
		run_item(*s, item);
		s->mu.lock();
		--s->running;
		bool wake = waiters_due(*s);
		s->mu.unlock();
		if (wake) {
			threadpool_detail::futex_wake_all(&_group_wake);
		}
		return true;
	}

	// Parks worker idx after it found nothing; false once the pool is stopping.
	bool park(unsigned idx, bool& searching) noexcept {
		if (_stop.load(std::memory_order_acquire)) {
			return false;
		}
		bool final_check = false;
		auto observed = _idle.to_parked(idx, searching, &final_check);
		searching = false;
		// acquire: to_parked() took the mutex after the destructor's notify_all() did, or
		// before it and is woken by it; either way a stop is not slept through.
		bool stopping = _stop.load(std::memory_order_acquire);
		if (stopping || (final_check && !_ready.empty())) {
			if (_idle.unpark_self(idx)) {
				if (stopping) {
					return false;
				}
				searching = _idle.to_searching();
				return true;
			}
		}
		_idle.park(idx, observed);
		searching = true;  // a woken worker is counted as searching
		return true;
	}

	void work(unsigned idx) noexcept {
		threadpool_detail::tls_here.pool = this;
		bool searching = false;
		for (;;) {
			GroupState* s = _ready.pop();
			if (!s && !searching) {
				searching = _idle.to_searching();
				if (searching) {
					s = _ready.pop();
				}
			}
			if (s) {
				if (searching) {
					searching = false;
					if (_idle.from_searching()) {
						_idle.notify_one();
					}
				}
				serve(*s);
				continue;
			}
			if (!park(idx, searching)) {
				break;
			}
		}
		threadpool_detail::tls_here.pool = nullptr;
	}

	void stop_and_join() noexcept {
		_stop.store(true, std::memory_order_release);
		_idle.notify_all();
		for (auto& worker : _workers) {
			try {
				worker->join(std::chrono::steady_clock::time_point::max());
			} catch (...) {
				L_EXC("Unhandled exception in a WorkPool worker");
			}
		}
	}

public:
	WorkPool() :
		WorkPool(Options{}) {}

	explicit WorkPool(Options options) :
		_n(resolve(options.threads)),
		_quantum(static_cast<std::int64_t>(double(options.quantum.count()) * threadpool_detail::ticks_per_ns())),
		_idle(_n),
		_ready(2 * (std::size_t(_n) + 1)) {
		if (_quantum < 1) {
			_quantum = 1;
		}
		_workers.reserve(_n);
		try {
			for (unsigned idx = 0; idx < _n; ++idx) {
				_workers.push_back(std::make_unique<threadpool_detail::WorkPoolWorker>(this, idx, std::vformat(options.name_format, std::make_format_args(idx))));
				_workers.back()->run();
			}
		} catch (...) {
			stop_and_join();
			throw;
		}
	}

	WorkPool(const WorkPool&) = delete;
	WorkPool& operator=(const WorkPool&) = delete;

	// Runs what is queued, then stops and joins the workers. Every WorkGroup must be gone,
	// and only the pool's own items may post once the destructor starts.
	~WorkPool() noexcept { stop_and_join(); }

	unsigned threads() const noexcept { return _n; }

	bool on_worker() const noexcept { return threadpool_detail::tls_here.pool == this; }

	// Queues item on item.group (the pool's own group when null) from any thread; never
	// blocks, never allocates.
	void post(WorkItem& item) noexcept {
		GroupState& s = state_of(item);
		s.mu.lock();
		s.lane.push(&item);
		bool push = grant(s);
		if (push) {
			++s.lane.tickets;
			++s.lane.listed;
		}
		bool wake = s.waiters != 0;
		if (wake) {
			_group_wake.fetch_add(1, std::memory_order_release);  // a waiting owner may help
		}
		s.mu.unlock();
		release(&s, push, false, wake, false);
	}

	// Runs the caller's own group's queued items (when called from one of its items) until
	// pred() holds, so a nested wait never blocks a worker on work it could do itself.
	// It never runs another group's items. With nothing to run it polls with backoff.
	template <typename Pred>
	void help_until(Pred&& pred) {
		unsigned round = 0;
		while (!pred()) {
			if (help_own()) {
				round = 0;
			} else {
				threadpool_detail::backoff(round);
			}
		}
	}

	// Sleeps until deadline, returning true, or until group is cancelled, returning false
	// (cancelled after the calling item started, when called from one of its items). One
	// timed futex wait for the whole interval; the cancel wakes it.
	bool sleep_until(std::chrono::steady_clock::time_point deadline, WorkGroup& group) noexcept;
};


// One owner's FIFO on a shared WorkPool: one per index. Groups take turns of `quantum *
// weight` service; `width` caps how many of its items run at once (0: no cap). The pool
// must outlive the group.
class WorkGroup {
	friend WorkPool;
	friend WorkItem;
	using GroupState = threadpool_detail::GroupState;

	WorkPool* _pool;
	GroupState* _s;

	template <typename F>
	struct Box final : threadpool_detail::BoxedItem {
		F fn;

		explicit Box(F&& f) :
			fn(std::move(f)) {}
		explicit Box(const F& f) :
			fn(f) {}

		static void invoke(WorkItem* w) noexcept {
			auto* box = static_cast<Box*>(w);
			try {
				if constexpr (std::is_invocable_v<F&, const WorkItem&>) {
					box->fn(static_cast<const WorkItem&>(*box));
				} else {
					box->fn();
				}
			} catch (...) {
				L_EXC("ERROR: Task died with an unhandled exception");
			}
			delete box;
		}

		static void drop(BoxedItem* b) noexcept { delete static_cast<Box*>(b); }
	};

public:
	explicit WorkGroup(WorkPool& pool, unsigned width = 0, unsigned weight = 1) :
		_pool(&pool),
		_s(new GroupState) {
		_s->width = width;
		_s->weight = weight ? weight : 1;
		auto states = pool._states.fetch_add(1, std::memory_order_relaxed) + 1;
		try {
			pool._ready.reserve(states + pool._n + 1);
		} catch (...) {
			pool._states.fetch_sub(1, std::memory_order_relaxed);
			delete _s;
			throw;
		}
	}

	WorkGroup(const WorkGroup&) = delete;
	WorkGroup& operator=(const WorkGroup&) = delete;

	// Cancels (freeing queued boxed items; the owner keeps its own), then waits for the
	// running ones. Must not race other calls, except from the group's own running items.
	~WorkGroup() noexcept {
		for (WorkItem* item = cancel(); item;) {
			WorkItem* next = item->next;
			if (item->flags & threadpool_detail::item_boxed) {
				auto* box = static_cast<threadpool_detail::BoxedItem*>(item);
				box->destroy(box);
			}
			item = next;
		}
		wait();
		_s->mu.lock();
		_s->owned = false;
		bool free = _s->lane.tickets == 0 && _s->helpers == 0;
		_s->mu.unlock();
		if (free) {
			delete _s;
			_pool->_states.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	WorkPool& pool() const noexcept { return *_pool; }

	void post(WorkItem& item) noexcept {
		item.group = this;
		_pool->post(item);
	}

	// Boxes func (callable as func() or func(const WorkItem&), whose stop_requested() is
	// the group's stop) and posts it: one allocation. False when the box cannot be allocated.
	template <typename Func>
	bool enqueue(Func&& func) {
		using F = std::decay_t<Func>;
		static_assert(std::is_invocable_v<F&> || std::is_invocable_v<F&, const WorkItem&>, "a WorkGroup task takes no arguments or a const WorkItem&");
		auto* box = new (std::nothrow) Box<F>(std::forward<Func>(func));
		if (!box) {
			return false;
		}
		box->run = &Box<F>::invoke;
		box->destroy = &Box<F>::drop;
		box->flags = threadpool_detail::item_boxed;
		post(*box);
		return true;
	}

	// Unlinks the queued items that have not started and returns them as a list through
	// `next`, oldest first, for the owner to free or recycle; running items see
	// stop_requested() turn true. Items posted afterwards run normally.
	WorkItem* cancel() noexcept {
		_s->mu.lock();
		WorkItem* list = _s->lane.head;
		_s->lane.head = nullptr;
		_s->lane.tail = nullptr;
		_s->lane.pending = 0;
		// relaxed: under mu; readers check it advisorily, and sleep's release orders it.
		_s->epoch.fetch_add(1, std::memory_order_relaxed);
		// release: a sleeper that sees the new word sees the new epoch.
		_s->sleep.fetch_add(1, std::memory_order_release);
		bool wake = _pool->waiters_due(*_s);
		_s->mu.unlock();
		threadpool_detail::futex_wake_all(&_s->sleep);
		if (wake) {
			threadpool_detail::futex_wake_all(&_pool->_group_wake);
		}
		return list;
	}

	// Blocks until the group has nothing queued and nothing running but the caller (when
	// called from one of its items). On a worker it serves the group meanwhile, so a wait
	// cannot deadlock on queued work; off the pool it parks.
	void wait() noexcept {
		auto& pool = *_pool;
		GroupState* s = _s;
		bool own = threadpool_detail::tls_here.state == s;
		std::uint32_t self = own ? 1 : 0;
		for (;;) {
			// acquire: pairs with waiters_due()'s increment, made under s->mu.
			auto word = pool._group_wake.load(std::memory_order_acquire);
			s->mu.lock();
			if (s->lane.pending == 0 && s->running == self) {
				s->mu.unlock();
				return;
			}
			if (s->lane.pending && pool.on_worker()) {
				if (own) {
					s->mu.unlock();
					pool.help_own();
					continue;
				}
				// A worker waiting on another group serves it as a helper, within its width.
				if (!s->width || s->lane.tickets + s->helpers < s->width) {
					++s->helpers;
					pool.turn(*s, true);
					continue;
				}
			}
			++s->waiters;
			s->mu.unlock();
			threadpool_detail::futex_wait(&pool._group_wake, word);
			s->mu.lock();
			--s->waiters;
			s->mu.unlock();
		}
	}

	std::size_t cancel_and_wait() noexcept {
		std::size_t dropped = 0;
		for (WorkItem* item = cancel(); item; item = item->next) {
			++dropped;
		}
		wait();
		return dropped;
	}

	// Whether the item running on this thread belongs to this group and saw a cancel.
	bool stop_requested() const noexcept {
		auto* item = threadpool_detail::tls_here.item;
		return item && threadpool_detail::tls_here.state == _s && item->stop_requested();
	}

	void set_width(unsigned width) noexcept {
		_s->mu.lock();
		_s->width = width;
		_s->mu.unlock();
	}

	void set_weight(unsigned weight) noexcept {
		_s->mu.lock();
		_s->weight = weight ? weight : 1;
		_s->mu.unlock();
	}
};


inline bool
WorkItem::stop_requested() const noexcept
{
	return group && group->_s->epoch.load(std::memory_order_relaxed) != epoch;
}


inline WorkPool::GroupState&
WorkPool::state_of(const WorkItem& item) noexcept
{
	return item.group ? *item.group->_s : _own;
}


inline bool
WorkPool::sleep_until(std::chrono::steady_clock::time_point deadline, WorkGroup& group) noexcept
{
	GroupState* s = group._s;
	auto* item = threadpool_detail::tls_here.item;
	auto started = item && threadpool_detail::tls_here.state == s ? item->epoch : s->epoch.load(std::memory_order_relaxed);
	for (;;) {
		// acquire: pairs with cancel()'s release of the word, so a new word means a new epoch.
		auto word = s->sleep.load(std::memory_order_acquire);
		if (s->epoch.load(std::memory_order_relaxed) != started) {
			return false;
		}
		auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			return true;
		}
		auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now).count();
		threadpool_detail::futex_wait_for(&s->sleep, word, static_cast<std::uint64_t>(ns));
	}
}


inline void
threadpool_detail::WorkPoolWorker::operator()()
{
	_pool->work(_idx);
}
