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

// WorkPool: one pool for many owners (WorkGroups), served by deficit round robin, queries
// first, with caller-owned items and futex-parked workers. See ARCHITECTURE.md, "WorkPool".
// The idle protocol follows Tokio's runtime/scheduler/multi_thread/idle.rs (MIT); the ring
// is Dmitry Vyukov's bounded MPMC queue (1024cores.net, BSD-2-Clause).
#pragma once

#include <algorithm>             // for std::max
#include <atomic>                // for std::atomic
#include <chrono>                // for std::chrono
#include <cstddef>               // for std::size_t
#include <cstdio>                // for std::fprintf
#include <cstdlib>               // for std::abort
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


// What an item is for. Queries run first; flushes and merges never take the reserve.
enum class WorkClass : std::uint8_t { query = 0, flush = 1, merge = 2 };


// A unit of work, owned by the caller: the pool links it while queued and calls run once.
// It must stay alive until run starts; run may destroy or repost it.
struct WorkItem {
	void (*run)(WorkItem*) noexcept = nullptr;
	WorkGroup* group = nullptr;  // nullptr: the pool's own group
	WorkItem* next = nullptr;    // the pool's while queued; cancel()'s list after
	std::uint32_t epoch = 0;     // the pool's: the group's epoch when the item started
	WorkClass cls = WorkClass::flush;
	std::uint8_t flags = 0;      // the pool's

	// Whether the item's group was cancelled after the item started; call it from run.
	bool stop_requested() const noexcept;
};


// A one-shot idle notification, owned by the caller: WorkGroup::on_idle() links it, and fn
// runs once, on the thread that makes the group idle (nothing queued, nothing running), or on
// the calling thread when the group already is. fn runs inside the scheduler, so it only
// posts (resumes a coroutine, wakes a loop) and never blocks; it may free the node.
struct WorkIdle {
	void (*fn)(WorkIdle*) noexcept = nullptr;
	WorkIdle* next = nullptr;    // the pool's while linked
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

inline constexpr unsigned classes = 3;

inline unsigned class_of(const WorkItem& item) noexcept {
	auto c = static_cast<unsigned>(item.cls);
	return c < classes ? c : static_cast<unsigned>(WorkClass::flush);
}

// One class of one group: its queue and its place in line, all under the group's lock.
struct Lane {
	WorkItem* head = nullptr;
	WorkItem* tail = nullptr;
	std::uint32_t pending = 0;   // queued items
	std::uint32_t tickets = 0;   // turns on the ready ring or being served from it
	std::uint32_t listed = 0;    // of those, on the ring now
	std::uint32_t helpers = 0;   // waiting threads serving it outside the ring
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
	Lane lanes[classes];         // under mu, by WorkClass
	std::uint32_t running = 0;   // items executing, under mu
	std::uint32_t waiters = 0;   // threads parked in WorkGroup::wait(), under mu
	WorkIdle* idle = nullptr;    // on_idle() notifications not yet fired, under mu
	unsigned width = 0;          // under mu; per class, 0: no cap
	unsigned weight = 1;         // quanta per turn, under mu
	bool owned = true;           // under mu; false once the WorkGroup is gone
	std::atomic<std::uint32_t> epoch{0};  // cancel() bumps it
	std::atomic<std::uint32_t> sleep{0};  // sleep_until()'s futex word; cancel() bumps it

	std::uint32_t pending() const noexcept {
		return lanes[0].pending + lanes[1].pending + lanes[2].pending;
	}

	bool unreferenced() const noexcept {
		for (const auto& l : lanes) {
			if (l.tickets || l.helpers) {
				return false;
			}
		}
		return true;
	}
};

// The ready-list: Vyukov's bounded MPMC ring of group pointers, chained so it grows as
// groups open and a post never blocks: a larger ring becomes the tail, the old one drains
// first, and retired rings live until the pool ends. Each entry is one listed ticket, so
// reserve() keeps room for (live groups + extra tickets).
class ReadyRing {
	static constexpr std::size_t closed = std::size_t(1) << (sizeof(std::size_t) * 8 - 1);

	struct Cell {
		std::atomic<std::size_t> seq;
		GroupState* data;
		std::atomic<std::uint64_t> stamp;  // when the entry was pushed, in ticks; 0: not recorded
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
				cells[i].stamp.store(0, std::memory_order_relaxed);
			}
		}

		// False when closed (or, against the sizing, full): retry on the tail.
		bool push(GroupState* x, std::uint64_t stamp) noexcept {
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
						c.stamp.store(stamp, std::memory_order_relaxed);
						// release: publishes data and stamp to the consumer that acquires seq.
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

		// The stamp of the entry next in line, or 0 when there is none (or it is not yet
		// published). Another consumer may take it meanwhile; the answer is then a stamp
		// of a recent head, which at worst delays or repeats an aging decision.
		std::uint64_t head_stamp() const noexcept {
			auto pos = deq.load(std::memory_order_relaxed);
			const Cell& c = cells[pos & mask];
			if (c.seq.load(std::memory_order_acquire) != pos + 1) {
				return 0;
			}
			return c.stamp.load(std::memory_order_relaxed);
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
	ReadyRing() {
		_rings.push_back(std::make_unique<Ring>(pow2(0)));
		_head.store(_rings.back().get(), std::memory_order_relaxed);
		_tail.store(_rings.back().get(), std::memory_order_relaxed);
	}

	// stamp: the push time in ticks for head_stamp(), or 0.
	void push(GroupState* x, std::uint64_t stamp = 0) noexcept {
		for (;;) {
			// acquire: pairs with reserve()'s publication of a new tail.
			if (_tail.load(std::memory_order_acquire)->push(x, stamp)) {
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

	// The stamp of the oldest entry still queued, or 0 when there is none.
	std::uint64_t head_stamp() const noexcept {
		for (Ring* r = _head.load(std::memory_order_acquire); r; r = r->next.load(std::memory_order_acquire)) {
			if (auto st = r->head_stamp()) {
				return st;
			}
		}
		return 0;
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
// One item running on this thread's stack; run_item() links it, innermost first.
struct Frame {
	GroupState* state;
	WorkItem* item;
	const Frame* parent;
};

struct Here {
	const WorkPool* pool = nullptr;   // the pool whose worker this is
	const Frame* top = nullptr;       // the innermost item running here

	// The innermost item of group s on this stack, or null.
	const Frame* find(const GroupState* s) const noexcept {
		for (const Frame* f = top; f; f = f->parent) {
			if (f->state == s) {
				return f;
			}
		}
		return nullptr;
	}
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


// A fixed set of named workers shared by many WorkGroups. Items run in class order,
// queries first; `reserve` workers stay free of flush and merge work so a query never
// waits for a long item to end, and merges hold at most `merge_cap` workers. Idle workers
// park on a futex each and use no CPU; post() never blocks or allocates.
class WorkPool {
	friend threadpool_detail::WorkPoolWorker;
	friend WorkGroup;
	using GroupState = threadpool_detail::GroupState;
	using Lane = threadpool_detail::Lane;
	static constexpr unsigned query = static_cast<unsigned>(WorkClass::query);
	static constexpr unsigned flush = static_cast<unsigned>(WorkClass::flush);
	static constexpr unsigned merge = static_cast<unsigned>(WorkClass::merge);

public:
	struct Options {
		unsigned threads = 0;              // 0: the hardware's concurrency
		unsigned reserve = 1;              // workers kept free of flush and merge work
		unsigned merge_cap = 0;            // workers merges may hold; 0: threads - reserve
		std::chrono::nanoseconds quantum = std::chrono::microseconds(100);  // service per turn
		const char* name_format = "W{:02}";  // a std::format string over the worker index
		// A merge ticket that has waited this long is picked ahead of flush tickets (0: never).
		std::chrono::nanoseconds merge_age = std::chrono::milliseconds(100);
		// Called with a site name when a thread that is not a worker enters wait() or
		// help_until(), before it blocks: a host aborts or logs there when the thread is one
		// that must never wait (an event loop). Null: no check.
		void (*block_guard)(const char* site) noexcept = nullptr;
	};

private:
	unsigned _n;
	unsigned _bg_cap;                  // workers flush and merge turns may hold
	unsigned _merge_cap;               // workers merge turns may hold
	std::int64_t _quantum;             // in ticks
	std::int64_t _merge_age;           // in ticks; 0: merges do not age
	void (*_block_guard)(const char*) noexcept;
	threadpool_detail::Idle _idle;
	threadpool_detail::ReadyRing _ready[threadpool_detail::classes];
	std::atomic<bool> _stop{false};
	alignas(64) std::atomic<unsigned> _busy_bg{0};     // workers in flush or merge turns
	alignas(64) std::atomic<unsigned> _busy_merge{0};  // workers in merge turns
	alignas(64) std::atomic<unsigned> _extras[threadpool_detail::classes] = {};  // tickets beyond each lane's first; each at most _n
	alignas(64) std::atomic<std::uint64_t> _aged{0};   // merge turns picked ahead of flush for their age
	alignas(64) std::atomic<std::size_t> _states{1};   // group states alive, the pool's own included
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

	void reserve_rings(std::size_t states) {
		for (auto& ring : _ready) {
			ring.reserve(states + _n + 1);
		}
	}

	unsigned cap(const GroupState& s) const noexcept {
		return s.width && s.width < _n ? s.width : _n;
	}

	// Whether a worker could take one more turn of class c: queries may use any idle
	// worker, flushes and merges only those beyond the reserve, merges at most merge_cap.
	bool spare(unsigned c) const noexcept {
		auto unparked = _idle.num_unparked();
		if (c == query) {
			return unparked < _n;
		}
		if (unparked >= _bg_cap || _busy_bg.load(std::memory_order_relaxed) >= _bg_cap) {
			return false;
		}
		return c != merge || _busy_merge.load(std::memory_order_relaxed) < _merge_cap;
	}

	// Under s.mu: whether lane c of s may take one more ring ticket. Its first is free;
	// more need queued work beyond its tickets, room under its width, a spare worker, and
	// one of the class's _n extras (more than one per worker could never run at once).
	bool grant(GroupState& s, unsigned c) noexcept {
		Lane& l = s.lanes[c];
		if (l.tickets + l.helpers == 0) {
			return true;
		}
		if (l.pending <= l.tickets || l.tickets + l.helpers >= cap(s) || !spare(c)) {
			return false;
		}
		if (l.tickets == 0) {
			return true;  // a helper serves it; this is the ring's first
		}
		// relaxed: a counter for the ring's bound; the ticket itself is published under s.mu.
		if (_extras[c].fetch_add(1, std::memory_order_relaxed) >= _n) {
			_extras[c].fetch_sub(1, std::memory_order_relaxed);
			return false;
		}
		return true;
	}

	// Under s.mu: gives back one ring ticket of lane c.
	void drop(GroupState& s, unsigned c) noexcept {
		if (s.lanes[c].tickets-- > 1) {
			_extras[c].fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// Under s.mu: whether the group has nothing queued and nothing running.
	static bool idle(const GroupState& s) noexcept {
		return s.pending() == 0 && s.running == 0;
	}

	// Under s.mu, after a turn ended or a cancel: whether a parked wait() of s has something
	// to do, because the group is idle or a helper may serve a lane now. Bumps the word the
	// waiters sleep on; the caller wakes them after unlocking.
	bool waiters_due(GroupState& s) noexcept {
		if (s.waiters && (idle(s) || helpable(s) >= 0)) {
			// release: under s.mu anyway; the futex compares the word.
			_group_wake.fetch_add(1, std::memory_order_release);
			return true;
		}
		return false;
	}

	// Under s.mu: detaches the idle notifications of a group that is idle, to run unlocked.
	static WorkIdle* take_idle(GroupState& s) noexcept {
		WorkIdle* list = nullptr;
		if (s.idle && idle(s)) {
			list = s.idle;
			s.idle = nullptr;
		}
		return list;
	}

	static void fire_idle(WorkIdle* list) noexcept {
		while (list) {
			WorkIdle* next = list->next;  // fn may free the node
			list->fn(list);
			list = next;
		}
	}

	// After unlocking s: what its critical section decided. The common case is inline.
	void release(GroupState* s, unsigned c, bool push, bool extra, bool wake, bool free) noexcept {
		if (push | extra | wake | free) {
			release_slow(s, c, push, extra, wake, free);
		}
	}

	[[gnu::noinline]] void release_slow(GroupState* s, unsigned c, bool push, bool extra, bool wake, bool free) noexcept {
		// A merge entry is stamped for aging; the clock is read only for merges.
		auto stamp = c == merge ? std::max<std::uint64_t>(threadpool_detail::ticks(), 1) : 0;
		if (push) {
			_ready[c].push(s, stamp);
		}
		if (extra) {
			_ready[c].push(s, stamp);
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

	// `here` is this thread's tls_here, resolved once by the caller (per turn, not per item).
	static void run_item(threadpool_detail::Here& here, GroupState& s, WorkItem* item) noexcept {
		threadpool_detail::Frame frame{&s, item, here.top};
		here.top = &frame;
		item->run(item);
		here.top = frame.parent;
	}

	// Under s.mu with a ticket of lane c held (a ring ticket, or a helper's): one turn, a
	// quantum of service plus the lane's debt, then requeue, keep, or drop the ticket.
	// Returns with s.mu released.
	void turn(GroupState& s, unsigned c, bool helper) noexcept {
		Lane& l = s.lanes[c];
		std::int64_t budget = l.deficit + _quantum * s.weight;
		l.deficit = 0;
		auto& here = threadpool_detail::tls_here;
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
			run_item(here, s, item);
			auto now = threadpool_detail::ticks();
			budget -= static_cast<std::int64_t>(now - t);
			t = now;
			s.mu.lock();
			--s.running;
		}
		bool push = false;
		bool extra = false;
		if (helper) {
			--l.helpers;
		}
		if (l.pending == 0) {
			l.deficit = 0;  // DRR: an idle lane keeps neither credit nor debt
			if (!helper) {
				drop(s, c);
			}
		} else {
			if (budget < 0) {
				l.deficit += budget;
			}
			if (helper) {
				// A helper's ticket never joins the ring, unless the lane would have none.
				if (l.tickets == 0) {
					++l.tickets;
					++l.listed;
					push = true;
				}
			} else if (l.tickets > 1 && _ready[c].size() > l.listed) {
				drop(s, c);  // other groups wait: an extra ticket goes back, so shares even out
			} else {
				++l.listed;
				push = true;
				// One more per quantum while workers idle: an index alone fans out to every core.
				if (grant(s, c)) {
					++l.tickets;
					++l.listed;
					extra = true;
				}
			}
		}
		bool wake = waiters_due(s);
		WorkIdle* notify = take_idle(s);
		bool free = !s.owned && s.unreferenced();
		s.mu.unlock();
		release(&s, c, push, extra, wake, free);
		if (notify) [[unlikely]] {
			fire_idle(notify);
		}
	}

	// A ring entry of class c: one listed ticket of s.
	void serve(GroupState& s, unsigned c) noexcept {
		s.mu.lock();
		Lane& l = s.lanes[c];
		--l.listed;
		// A waiting helper was admitted while this ticket sat on the ring; the width holds
		// by giving the ticket up (the helper requeues one when its turn ends).
		if (l.helpers && s.width && l.tickets - l.listed + l.helpers > s.width) {
			drop(s, c);
			bool free = !s.owned && s.unreferenced();
			s.mu.unlock();
			release(&s, c, false, false, false, free);
			return;
		}
		turn(s, c, false);
	}

	// Under s.mu: the lane a waiting worker may serve as a helper, or -1: one with queued
	// items while fewer than `width` turns of it are being served (tickets still on the ring
	// do not count).
	static int helpable(const GroupState& s) noexcept {
		for (unsigned c = 0; c < threadpool_detail::classes; ++c) {
			const auto& l = s.lanes[c];
			if (l.pending && (!s.width || l.tickets - l.listed + l.helpers < s.width)) {
				return static_cast<int>(c);
			}
		}
		return -1;
	}

	// The background slots a worker holds, kept across consecutive flush and merge turns
	// so a stream of them costs no RMW on the shared counts.
	struct Slots {
		bool bg = false;
		bool merge = false;
	};

	void release_merge(Slots& held) noexcept {
		if (held.merge) {
			held.merge = false;
			_busy_merge.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	void release_slots(Slots& held) noexcept {
		release_merge(held);
		if (held.bg) {
			held.bg = false;
			_busy_bg.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// Whether the oldest merge entry has waited longer than merge_age.
	bool merge_overdue() const noexcept {
		if (!_merge_age) {
			return false;
		}
		auto stamp = _ready[merge].head_stamp();
		return stamp && static_cast<std::int64_t>(threadpool_detail::ticks() - stamp) > _merge_age;
	}

	// The next merge entry if a merge slot (at most _merge_cap) is free, else null.
	GroupState* pick_merge(unsigned& c, Slots& held) noexcept {
		if (_ready[merge].size() == 0) {
			return nullptr;
		}
		if (!held.merge) {
			if (_busy_merge.fetch_add(1, std::memory_order_relaxed) < _merge_cap) {
				held.merge = true;
			} else {
				_busy_merge.fetch_sub(1, std::memory_order_relaxed);
			}
		}
		if (held.merge) {
			if (GroupState* s = _ready[merge].pop()) {
				c = merge;
				return s;
			}
		}
		return nullptr;
	}

	// The next ring entry in class order, with one exception: a merge entry older than
	// merge_age goes before flush, so a flood of flushes cannot starve merges. Flush and
	// merge turns need a background slot (at most _bg_cap held, so the reserve stays free
	// for queries) and merges a merge slot (at most _merge_cap). Slots are taken by RMW
	// before the pop, so the caps are exact; a query or an empty pick gives them back.
	GroupState* pick(unsigned& c, Slots& held) noexcept {
		if (GroupState* s = _ready[query].pop()) {
			release_slots(held);
			c = query;
			return s;
		}
		if (!held.bg) {
			if (_ready[flush].size() == 0 && _ready[merge].size() == 0) {
				return nullptr;
			}
			// relaxed: a count for the cap; the turn is ordered by the ring and the group lock.
			if (_busy_bg.fetch_add(1, std::memory_order_relaxed) >= _bg_cap) {
				_busy_bg.fetch_sub(1, std::memory_order_relaxed);
				return nullptr;
			}
			held.bg = true;
		}
		if (merge_overdue()) {
			if (GroupState* s = pick_merge(c, held)) {
				_aged.fetch_add(1, std::memory_order_relaxed);
				return s;
			}
		}
		if (GroupState* s = _ready[flush].pop()) {
			release_merge(held);
			c = flush;
			return s;
		}
		if (GroupState* s = pick_merge(c, held)) {
			return s;
		}
		release_slots(held);
		return nullptr;
	}

	// The parker's final check: work this worker could take now. A slot held by another
	// worker's pick() at this moment is released by a worker that then looks again.
	bool ready_for_me() const noexcept {
		if (!_ready[query].empty()) {
			return true;
		}
		if (_busy_bg.load(std::memory_order_seq_cst) >= _bg_cap) {
			return false;
		}
		if (!_ready[flush].empty()) {
			return true;
		}
		return _busy_merge.load(std::memory_order_seq_cst) < _merge_cap && !_ready[merge].empty();
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
		if (stopping || (final_check && ready_for_me())) {
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
		Slots held;
		for (;;) {
			unsigned c = flush;
			GroupState* s = pick(c, held);
			if (!s && !searching) {
				searching = _idle.to_searching();
				if (searching) {
					s = pick(c, held);
				}
			}
			if (s) {
				if (searching) {
					searching = false;
					if (_idle.from_searching()) {
						_idle.notify_one();
					}
				}
				serve(*s, c);
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
		_bg_cap(_n - (options.reserve < _n ? options.reserve : _n - 1)),
		_merge_cap(options.merge_cap && options.merge_cap < _bg_cap ? options.merge_cap : _bg_cap),
		_quantum(static_cast<std::int64_t>(double(options.quantum.count()) * threadpool_detail::ticks_per_ns())),
		_merge_age(options.merge_age.count() > 0 ? std::max<std::int64_t>(1, static_cast<std::int64_t>(double(options.merge_age.count()) * threadpool_detail::ticks_per_ns())) : 0),
		_block_guard(options.block_guard),
		_idle(_n) {
		if (_quantum < 1) {
			_quantum = 1;
		}
		reserve_rings(1);
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
	~WorkPool() noexcept {
		stop_and_join();
	}

	unsigned threads() const noexcept { return _n; }

	bool on_worker() const noexcept { return threadpool_detail::tls_here.pool == this; }

	// Merge entries waiting for a worker now (approximate), for a backlog report.
	std::size_t merge_waiting() const noexcept { return _ready[merge].size(); }

	// How long the oldest waiting merge entry has waited; zero when none does.
	std::chrono::nanoseconds merge_oldest() const noexcept {
		auto stamp = _ready[merge].head_stamp();
		if (!stamp) {
			return {};
		}
		auto waited = static_cast<std::int64_t>(threadpool_detail::ticks() - stamp);
		return std::chrono::nanoseconds(waited > 0 ? static_cast<std::int64_t>(double(waited) / threadpool_detail::ticks_per_ns()) : 0);
	}

	// Merge turns taken ahead of flush because they were older than merge_age.
	std::uint64_t merges_aged() const noexcept { return _aged.load(std::memory_order_relaxed); }

	// Queues item on item.group (the pool's own group when null) in item.cls, from any
	// thread; never blocks, never allocates.
	void post(WorkItem& item) noexcept {
		GroupState& s = state_of(item);
		unsigned c = threadpool_detail::class_of(item);
		Lane& l = s.lanes[c];
		s.mu.lock();
		l.push(&item);
		bool push = grant(s, c);
		if (push) {
			++l.tickets;
			++l.listed;
		}
		bool wake = s.waiters != 0;
		if (wake) {
			_group_wake.fetch_add(1, std::memory_order_release);  // a waiting owner may help
		}
		s.mu.unlock();
		release(&s, c, push, false, wake, false);
	}

	// Sleeps until deadline, returning true, or until group is cancelled, returning false
	// (cancelled after the calling item started, when called from one of its items). One
	// timed futex wait for the whole interval; the cancel wakes it.
	bool sleep_until(std::chrono::steady_clock::time_point deadline, WorkGroup& group) noexcept;
};


// One owner's FIFOs on a shared WorkPool, one per WorkClass: one group per index. Groups
// take turns of `quantum * weight` service per class; `width` caps how many of its items
// of one class run at once (0: no cap). The pool must outlive the group.
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

	// A wait on a group with an item on this thread's stack can only wait for itself.
	void check_stack(const char* site) const noexcept {
		if (threadpool_detail::tls_here.find(_s)) {
			std::fprintf(stderr, "threadpool: %s on a group that has an item on this thread's stack (a wait inside its own group, or a cycle of waits); post the work to a child group and wait on that\n", site);
			std::abort();
		}
	}

public:
	explicit WorkGroup(WorkPool& pool, unsigned width = 0, unsigned weight = 1) :
		_pool(&pool),
		_s(new GroupState) {
		_s->width = width;
		_s->weight = weight ? weight : 1;
		auto states = pool._states.fetch_add(1, std::memory_order_relaxed) + 1;
		try {
			pool.reserve_rings(states);
		} catch (...) {
			pool._states.fetch_sub(1, std::memory_order_relaxed);
			delete _s;
			throw;
		}
	}

	WorkGroup(const WorkGroup&) = delete;
	WorkGroup& operator=(const WorkGroup&) = delete;

	// Cancels (freeing queued boxed items; the owner keeps its own), then waits for the
	// running ones, as wait() does: aborts when called from an item of this group. Must not
	// race other calls.
	~WorkGroup() noexcept {
		check_stack("WorkGroup::~WorkGroup");
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
		bool free = _s->unreferenced();
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
	// the group's stop) and posts it in class cls: one allocation. False when the box
	// cannot be allocated.
	template <typename Func>
	bool enqueue(Func&& func, WorkClass cls = WorkClass::flush) {
		using F = std::decay_t<Func>;
		static_assert(std::is_invocable_v<F&> || std::is_invocable_v<F&, const WorkItem&>, "a WorkGroup task takes no arguments or a const WorkItem&");
		auto* box = new (std::nothrow) Box<F>(std::forward<Func>(func));
		if (!box) {
			return false;
		}
		box->run = &Box<F>::invoke;
		box->destroy = &Box<F>::drop;
		box->cls = cls;
		box->flags = threadpool_detail::item_boxed;
		post(*box);
		return true;
	}

	// Unlinks the queued items that have not started and returns them as a list through
	// `next`, queries then flushes then merges, each oldest first, for the owner to free
	// or recycle; running items see stop_requested() turn true. Items posted afterwards
	// run normally.
	WorkItem* cancel() noexcept {
		WorkItem* list = nullptr;
		WorkItem** tail = &list;
		_s->mu.lock();
		for (auto& l : _s->lanes) {
			if (l.head) {
				*tail = l.head;
				tail = &l.tail->next;
			}
			l.head = nullptr;
			l.tail = nullptr;
			l.pending = 0;
		}
		// relaxed: under mu; readers check it advisorily, and sleep's release orders it.
		_s->epoch.fetch_add(1, std::memory_order_relaxed);
		// release: a sleeper that sees the new word sees the new epoch.
		_s->sleep.fetch_add(1, std::memory_order_release);
		bool wake = _pool->waiters_due(*_s);
		WorkIdle* notify = WorkPool::take_idle(*_s);
		_s->mu.unlock();
		threadpool_detail::futex_wake_all(&_s->sleep);
		if (wake) {
			threadpool_detail::futex_wake_all(&_pool->_group_wake);
		}
		WorkPool::fire_idle(notify);
		return list;
	}

	// Blocks until the group has nothing queued and nothing running. On a worker the caller
	// serves the group meanwhile, inline, as a helper within the group's width, and nothing
	// else (no other group's work, no query); anywhere else it parks. It creates no thread.
	// A wait on a group that has an item on the calling thread's stack, this group included,
	// is a programming error and aborts: to join work from inside a group, post it to a child
	// group and wait on that. Items of the group may run on the waiter's thread.
	void wait() noexcept {
		check_stack("WorkGroup::wait");
		auto& pool = *_pool;
		GroupState* s = _s;
		bool worker = pool.on_worker();
		if (!worker && pool._block_guard) {
			pool._block_guard("WorkGroup::wait");
		}
		for (;;) {
			// acquire: pairs with the bump made under s->mu by whatever changed the group.
			auto word = pool._group_wake.load(std::memory_order_acquire);
			s->mu.lock();
			if (WorkPool::idle(*s)) {
				s->mu.unlock();
				return;
			}
			int c = worker ? WorkPool::helpable(*s) : -1;
			if (c >= 0) {
				++s->lanes[c].helpers;
				pool.turn(*s, static_cast<unsigned>(c), true);
				continue;
			}
			++s->waiters;
			s->mu.unlock();
			threadpool_detail::futex_wait(&pool._group_wake, word);
			s->mu.lock();
			--s->waiters;
			s->mu.unlock();
		}
	}

	// Runs node->fn once when the group is idle (nothing queued, nothing running): on the
	// thread that makes it idle, or here and now when it already is. The way for an event
	// loop or a coroutine to join a group: it never blocks, so it needs no block_guard.
	void on_idle(WorkIdle& node) noexcept {
		_s->mu.lock();
		bool now = WorkPool::idle(*_s);
		if (!now) {
			node.next = _s->idle;
			_s->idle = &node;
		}
		_s->mu.unlock();
		if (now) {
			node.fn(&node);
		}
	}

	// Unlinks a notification that has not fired; false when it already fired or is firing.
	bool cancel_idle(WorkIdle& node) noexcept {
		_s->mu.lock();
		bool found = false;
		for (WorkIdle** link = &_s->idle; *link; link = &(*link)->next) {
			if (*link == &node) {
				*link = node.next;
				found = true;
				break;
			}
		}
		_s->mu.unlock();
		return found;
	}

	std::size_t cancel_and_wait() noexcept {
		check_stack("WorkGroup::cancel_and_wait");
		std::size_t dropped = 0;
		for (WorkItem* item = cancel(); item; item = item->next) {
			++dropped;
		}
		wait();
		return dropped;
	}

	// Whether the item running on this thread belongs to this group and saw a cancel.
	bool stop_requested() const noexcept {
		auto* frame = threadpool_detail::tls_here.find(_s);
		return frame && frame->item->stop_requested();
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
	auto* frame = threadpool_detail::tls_here.find(s);
	auto started = frame ? frame->item->epoch : s->epoch.load(std::memory_order_relaxed);
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
