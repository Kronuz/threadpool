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

// WorkPool: one shared pool for many owners, with caller-owned work items (no allocation
// per post) and workers that sleep on a futex each. The idle protocol is Tokio's
// (tokio-rs/tokio, runtime/scheduler/multi_thread/idle.rs, MIT): a post wakes one worker
// only when no worker is searching, at most half the workers search at once, the last
// searcher to find work wakes the next, and the last searcher to park looks once more.
// See ARCHITECTURE.md, "WorkPool".
#pragma once

#include <atomic>                // for std::atomic
#include <chrono>                // for std::chrono
#include <cstddef>               // for std::size_t
#include <cstdint>               // for std::uint32_t, std::uint64_t
#include <exception>             // for std::exception_ptr
#include <format>                // for std::vformat, std::make_format_args
#include <memory>                // for std::unique_ptr
#include <mutex>                 // for std::mutex, std::lock_guard
#include <string>                // for std::string
#include <thread>                // for std::thread, std::this_thread
#include <utility>               // for std::forward
#include <vector>                // for std::vector

#include "thread.hh"             // for Thread, ThreadPolicyType

#ifdef THREADPOOL_TRACE_HEADER
#  include THREADPOOL_TRACE_HEADER
#else
#  include "threadpool_trace.h"
#endif

#if defined(__linux__)
#  include <climits>             // for INT_MAX
#  include <linux/futex.h>       // for FUTEX_WAIT_PRIVATE, FUTEX_WAKE_PRIVATE
#  include <sys/syscall.h>       // for SYS_futex
#  include <unistd.h>            // for syscall
#elif defined(__APPLE__)
#  include <Availability.h>      // for __MAC_OS_X_VERSION_MIN_REQUIRED
#  if defined(__MAC_OS_X_VERSION_MIN_REQUIRED) && __MAC_OS_X_VERSION_MIN_REQUIRED >= 140400
#    include <os/os_sync_wait_on_address.h>
#    define THREADPOOL_OS_SYNC 1
#  else
extern "C" int __ulock_wait(std::uint32_t operation, void* addr, std::uint64_t value, std::uint32_t timeout_us);
extern "C" int __ulock_wake(std::uint32_t operation, void* addr, std::uint64_t wake_value);
#  endif
#elif defined(__FreeBSD__)
#  include <climits>             // for INT_MAX
#  include <sys/types.h>
#  include <sys/umtx.h>          // for _umtx_op
#endif


class WorkPool;


// A unit of work, owned by the caller: the pool links it while queued and calls run once.
// It must stay alive until run starts; run may destroy or repost it.
struct WorkItem {
	void (*run)(WorkItem*) noexcept = nullptr;
	WorkItem* next = nullptr;    // the pool's while queued
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

	// After a post: wakes one sleeper when nobody searches. The caller's post must be a
	// seq_cst RMW or under a lock the parker's final check also takes; either way this load
	// and to_parked()'s RMW are ordered, so a post is never missed by both sides.
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
		auto us = round - 32 < 10 ? 1u << (round - 32) : 1000u;
		std::this_thread::sleep_for(std::chrono::microseconds(us < 1000 ? us : 1000));
	}
	++round;
}

inline thread_local const WorkPool* tls_pool = nullptr;

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


// A fixed set of named workers over one FIFO of caller-owned items. Workers park on a
// futex each and use no CPU while idle; post() never blocks or allocates.
class WorkPool {
	friend threadpool_detail::WorkPoolWorker;

public:
	struct Options {
		unsigned threads = 0;              // 0: the hardware's concurrency
		const char* name_format = "W{:02}";  // a std::format string over the worker index
	};

private:
	unsigned _n;
	threadpool_detail::Idle _idle;
	std::atomic<bool> _stop{false};

	alignas(64) threadpool_detail::SpinLock _mu;
	WorkItem* _head = nullptr;         // under _mu
	WorkItem* _tail = nullptr;         // under _mu

	std::vector<std::unique_ptr<threadpool_detail::WorkPoolWorker>> _workers;

	static unsigned resolve(unsigned threads) noexcept {
		if (threads) {
			return threads;
		}
		auto hw = std::thread::hardware_concurrency();
		return hw ? hw : 1;
	}

	WorkItem* take() noexcept {
		_mu.lock();
		WorkItem* item = _head;
		if (item) {
			_head = item->next;
			if (!_head) {
				_tail = nullptr;
			}
		}
		_mu.unlock();
		return item;
	}

	bool pending() noexcept {
		_mu.lock();
		bool any = _head != nullptr;
		_mu.unlock();
		return any;
	}

	static void run(WorkItem* item) noexcept { item->run(item); }

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
		if (stopping || (final_check && pending())) {
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
		threadpool_detail::tls_pool = this;
		bool searching = false;
		for (;;) {
			WorkItem* item = take();
			if (!item && !searching) {
				searching = _idle.to_searching();
				if (searching) {
					item = take();
				}
			}
			if (item) {
				if (searching) {
					searching = false;
					if (_idle.from_searching()) {
						_idle.notify_one();
					}
				}
				run(item);
				continue;
			}
			if (!park(idx, searching)) {
				break;
			}
		}
		threadpool_detail::tls_pool = nullptr;
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
		_idle(_n) {
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

	// Runs what is queued, then stops the workers and joins them. Posting after the
	// destructor starts, from outside the pool's own items, is a bug.
	~WorkPool() noexcept { stop_and_join(); }

	unsigned threads() const noexcept { return _n; }

	bool on_worker() const noexcept { return threadpool_detail::tls_pool == this; }

	// Queues item from any thread; never blocks, never allocates.
	void post(WorkItem& item) noexcept {
		item.next = nullptr;
		_mu.lock();
		if (_tail) {
			_tail->next = &item;
		} else {
			_head = &item;
		}
		_tail = &item;
		_mu.unlock();
		// The queue's lock orders this post against a parker's final check (see notify_one).
		_idle.notify_one();
	}

	// Runs queued items on the calling thread until pred() holds, so a worker waiting on
	// work it posted never blocks the pool. With nothing to run it polls with backoff.
	template <typename Pred>
	void help_until(Pred&& pred) {
		unsigned round = 0;
		while (!pred()) {
			if (WorkItem* item = take()) {
				run(item);
				round = 0;
			} else {
				threadpool_detail::backoff(round);
			}
		}
	}
};


inline void
threadpool_detail::WorkPoolWorker::operator()()
{
	_pool->work(_idx);
}
