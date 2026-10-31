/*
 * Copyright © 2026 Germán Méndez Bravo (Kronuz)
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

#pragma once

#include <atomic>                // for std::atomic
#include <chrono>                // for std::chrono
#include <condition_variable>    // for std::condition_variable
#include <cstddef>               // for std::size_t
#include <memory>                // for std::unique_ptr, std::launder
#include <mutex>                 // for std::mutex
#include <new>                   // for placement new
#include <thread>                // for std::this_thread::yield
#include <type_traits>           // for std::is_nothrow_move_constructible_v
#include <utility>               // for std::forward, std::move


namespace threadpool_detail {

inline void pool_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	__asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
	__asm__ __volatile__("isb" ::: "memory");
#else
	std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

// Test-and-test-and-set that yields after a short spin: for a lock held a few dozen nanoseconds,
// where a waiter that sleeps in the kernel costs far more than the wait. A holder that is
// preempted makes waiters yield, not burn their slices.
class PoolSpinMutex {
	std::atomic<unsigned> _locked{0};

public:
	void lock() noexcept {
		if (!_locked.exchange(1, std::memory_order_acquire)) {
			return;
		}
		unsigned spins = 0;
		for (;;) {
			while (_locked.load(std::memory_order_relaxed)) {
				if (++spins < 64) {
					pool_relax();
				} else {
					std::this_thread::yield();
				}
			}
			if (!_locked.exchange(1, std::memory_order_acquire)) {
				return;
			}
		}
	}

	bool try_lock() noexcept {
		return !_locked.load(std::memory_order_relaxed) && !_locked.exchange(1, std::memory_order_acquire);
	}

	void unlock() noexcept {
		_locked.store(0, std::memory_order_release);
	}
};

// The queue ThreadPool's workers take from: an unbounded FIFO with many producers and many
// consumers, holding T by value in chunks of ChunkSlots slots. T must be nothrow move constructible
// and convertible to bool; a falsy item is a sentinel that counts in neither size() nor claim.
//
// A producer and a consumer never share a lock: push() takes the tail lock, pop() the head lock,
// and a consumer finds work by reading the published position, so the two sides meet only on the
// slot a task moves through. The producer links the next chunk before it publishes the last slot
// of the current one, so a consumer that finishes a chunk always finds its successor and frees
// the chunk it left. The tail lock is held for a few dozen nanoseconds, so a producer that waits
// spins; the head lock is a std::mutex, so surplus consumers sleep instead of crowding it. The
// consumers keep a copy of the published position: while the queue has a backlog they do not read
// the line the producers write.
//
// A consumer with nothing to take spins for a few microseconds, then sleeps on a condition
// variable. A producer wakes a sleeper only when one is registered and not yet signaled, so a
// flood of tasks with busy or spinning workers does not signal at all.
template <typename T, std::size_t ChunkSlots = 64>
class PoolQueue {
	static_assert(std::is_nothrow_move_constructible_v<T>, "PoolQueue holds nothrow-movable items");
	static constexpr std::size_t line = 128;  // the false-sharing granule: the larger of the targets' pairs of lines

	struct Chunk {
		Chunk* next = nullptr;  // written before the publish that covers the last slot, read after it
		alignas(T) unsigned char slot[ChunkSlots][sizeof(T)];

		void* at(std::size_t i) noexcept { return slot[i]; }
		T* item(std::size_t i) noexcept { return std::launder(static_cast<T*>(at(i))); }
	};

	// Producers: the tail lock and what it guards.
	alignas(line) PoolSpinMutex _tl;
	Chunk* _tail;
	std::size_t _tail_idx = 0;
	std::atomic<std::size_t> _tasks_pushed{0};  // truthy items pushed; stored under _tl

	// The position of the next item to publish, written under _tl, read by consumers.
#ifdef POOLQ_MERGE_TAIL
	std::atomic<std::size_t> _tail_pos{0};
#else
	alignas(line) std::atomic<std::size_t> _tail_pos{0};
#endif

	// Consumers: the head lock and what it guards.
	alignas(line) std::mutex _hl;
	Chunk* _head;
	std::size_t _head_idx = 0;
	std::atomic<std::size_t> _head_pos{0};      // items taken; stored under _hl
	std::atomic<std::size_t> _tail_cache{0};    // a _tail_pos a consumer read: items before it exist; stored under _hl
	std::atomic<std::size_t> _tasks_popped{0};  // truthy items taken; stored under _hl

	// Sleepers: the count a producer reads, and the lock-protected state behind it.
	alignas(line) std::atomic<std::size_t> _idle{0};  // sleepers nobody has signaled; stored under _pm
	alignas(line) std::mutex _pm;
	std::condition_variable _cv;
	std::size_t _waiting = 0;   // consumers in _cv.wait
	std::size_t _signaled = 0;  // wakeups handed out and not yet taken

	void publish_idle() noexcept {
		// seq_cst: a producer's publish and its read of _idle, and a sleeper's store of _idle and its
		// last look at the queue, are in one total order, so one of them sees the other.
		_idle.store(_waiting - _signaled, std::memory_order_seq_cst);
	}

	void wake_slow(std::size_t n) {
		std::size_t k;
		{
			std::lock_guard<std::mutex> lk(_pm);
			k = _waiting - _signaled;
			if (k > n) {
				k = n;
			}
			if (!k) {
				return;
			}
			_signaled += k;
			publish_idle();
		}
		if (k == 1) {
			_cv.notify_one();
		} else {
			_cv.notify_all();
		}
	}

	// After a publish: wakes up to n sleepers.
	void wake(std::size_t n) {
		if (_idle.load(std::memory_order_seq_cst) != 0) {
			wake_slow(n);
		}
	}

	// Under _tl.
	template <typename U>
	void push_locked(U&& item, bool counted, std::memory_order publish) {
		std::unique_ptr<Chunk> spare;
		if (_tail_idx == ChunkSlots - 1) {
			spare.reset(new Chunk);  // before anything changes, so a failure leaves the queue as it was
		}
		::new (_tail->at(_tail_idx)) T(std::forward<U>(item));
		if (spare) {
			_tail->next = spare.get();
			_tail = spare.release();
			_tail_idx = 0;
		} else {
			++_tail_idx;
		}
		if (counted) {
			_tasks_pushed.store(_tasks_pushed.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
		}
		_tail_pos.store(_tail_pos.load(std::memory_order_relaxed) + 1, publish);
	}

public:
	PoolQueue() :
		_tail(new Chunk),
		_head(_tail) {}

	PoolQueue(const PoolQueue&) = delete;
	PoolQueue& operator=(const PoolQueue&) = delete;

	~PoolQueue() {
		auto end = _tail_pos.load(std::memory_order_relaxed);
		for (auto pos = _head_pos.load(std::memory_order_relaxed); pos != end; ++pos) {
			_head->item(_head_idx)->~T();
			if (++_head_idx == ChunkSlots) {
				auto next = _head->next;
				delete _head;
				_head = next;
				_head_idx = 0;
			}
		}
		delete _head;
	}

	// Appends item. A failure to allocate leaves the queue as it was.
	void push(T&& item) {
		const bool counted = static_cast<bool>(item);
		{
			std::lock_guard<PoolSpinMutex> lk(_tl);
			push_locked(std::move(item), counted, std::memory_order_seq_cst);
		}
		wake(1);
	}

	// Appends count items, each made from *first++ (moved from), under one lock. Items already
	// appended stay if one throws.
	template <typename It>
	void push_bulk(It first, std::size_t count) {
		if (!count) {
			return;
		}
		{
			std::lock_guard<PoolSpinMutex> lk(_tl);
			for (std::size_t i = 0; i < count; ++i) {
				T item(std::move(*first++));
				const bool counted = static_cast<bool>(item);
				push_locked(std::move(item), counted, i + 1 == count ? std::memory_order_seq_cst : std::memory_order_release);
			}
		}
		wake(count);
	}

	// Takes the oldest item into out; false when the queue is empty. claim, if given, is set to 1
	// under the head lock before a truthy item stops counting in size(), so the item is always in
	// size() or in a claim, never in neither.
	bool try_pop(T& out, std::atomic<std::size_t>* claim = nullptr) {
		// While the queue has a backlog the consumers' copy of the tail says so, and the line the
		// producers write is not read.
		if (_head_pos.load(std::memory_order_relaxed) == _tail_cache.load(std::memory_order_relaxed) &&
			_head_pos.load(std::memory_order_relaxed) == _tail_pos.load(std::memory_order_acquire)) {
			return false;
		}
		std::lock_guard<std::mutex> lk(_hl);
		const auto pos = _head_pos.load(std::memory_order_relaxed);
		if (pos == _tail_cache.load(std::memory_order_relaxed)) {
			const auto tail = _tail_pos.load(std::memory_order_acquire);
			if (pos == tail) {
				return false;
			}
			_tail_cache.store(tail, std::memory_order_relaxed);
		}
		T* p = _head->item(_head_idx);
		out = std::move(*p);
		p->~T();
		if (static_cast<bool>(out)) {
			if (claim) {
				claim->store(1, std::memory_order_relaxed);
			}
			_tasks_popped.store(_tasks_popped.load(std::memory_order_relaxed) + 1, std::memory_order_release);
		}
		_head_pos.store(pos + 1, std::memory_order_relaxed);
		if (++_head_idx == ChunkSlots) {
			Chunk* next = _head->next;
			delete _head;
			_head = next;
			_head_idx = 0;
		}
		return true;
	}

	// Takes the oldest item into out, waiting up to timeout for one; false on a timeout.
	bool pop(T& out, std::chrono::steady_clock::duration timeout, std::atomic<std::size_t>* claim = nullptr) {
		if (try_pop(out, claim)) {
			return true;
		}
		// Spin: the next item is usually a few hundred nanoseconds away.
		const auto spin_until = std::chrono::steady_clock::now() + spin;
		for (unsigned i = 1;; ++i) {
			pool_relax();
			if (_head_pos.load(std::memory_order_relaxed) != _tail_pos.load(std::memory_order_acquire) && try_pop(out, claim)) {
				return true;
			}
			if (i % 16 == 0 && std::chrono::steady_clock::now() >= spin_until) {
				break;
			}
		}
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		for (;;) {
			bool timed_out = false;
			{
				std::unique_lock<std::mutex> lk(_pm);
				++_waiting;
				publish_idle();
				if (_head_pos.load(std::memory_order_relaxed) != _tail_pos.load(std::memory_order_seq_cst)) {
					// Work arrived before the sleep; do not wait.
					--_waiting;
					publish_idle();
				} else {
					const bool woken = _cv.wait_until(lk, deadline, [this] { return _signaled != 0; });
					if (woken) {
						--_signaled;
					} else {
						timed_out = true;
					}
					--_waiting;
					publish_idle();
				}
			}
			if (try_pop(out, claim)) {
#ifdef THREADPOOL_QUEUE_STATS
				if (timed_out) {
					rescued.fetch_add(1, std::memory_order_relaxed);
				}
#endif
				return true;
			}
			if (timed_out || std::chrono::steady_clock::now() >= deadline) {
				return false;
			}
		}
	}

#ifdef THREADPOOL_QUEUE_STATS
	// Sleepers that timed out and found an item waiting: a wakeup that should have reached them
	// was lost. Zero in a correct build; the stress test reads it.
	std::atomic<std::size_t> rescued{0};
#endif

	// The items pushed and not yet taken, truthy ones only. Approximate while the queue is in use.
	std::size_t size() const noexcept {
		const auto popped = _tasks_popped.load(std::memory_order_acquire);
		const auto pushed = _tasks_pushed.load(std::memory_order_relaxed);
		return pushed > popped ? pushed - popped : 0;
	}

	// How long a consumer with nothing to take spins before it sleeps.
	static constexpr std::chrono::nanoseconds spin{4000};
};

}  // namespace threadpool_detail
