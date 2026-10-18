// Queues for the scheduler prototypes: a fixed-capacity Chase-Lev deque with the C11 memory
// orders of Lê, Pop, Cohen, Zappa Nardelli (PPoPP 2013), and a bounded MPMC ring (Vyukov's
// sequence-per-cell scheme). Both hold pointers, so slots are lock-free atomics. Research code.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace research {

// Owner pushes and pops at the bottom; thieves take from the top. Capacity is fixed: push
// fails when full and the caller spills elsewhere (Tokio's rule: half to the inject queue).
template <typename T, std::size_t Cap>
class ChaseLev {
	static_assert((Cap & (Cap - 1)) == 0, "Cap must be a power of two");
	static constexpr std::int64_t mask = Cap - 1;

	alignas(64) std::atomic<std::int64_t> _top{0};
	alignas(64) std::atomic<std::int64_t> _bottom{0};
	alignas(64) std::atomic<T*> _buf[Cap];

public:
	ChaseLev() { for (auto& s : _buf) { s.store(nullptr, std::memory_order_relaxed); } }

	bool push(T* x) noexcept {
		auto b = _bottom.load(std::memory_order_relaxed);
		auto t = _top.load(std::memory_order_acquire);
		if (b - t >= static_cast<std::int64_t>(Cap)) { return false; }
		_buf[b & mask].store(x, std::memory_order_relaxed);
		std::atomic_thread_fence(std::memory_order_release);
		_bottom.store(b + 1, std::memory_order_relaxed);
		return true;
	}

	// The owner's take: LIFO.
	T* pop() noexcept {
		auto b = _bottom.load(std::memory_order_relaxed) - 1;
		_bottom.store(b, std::memory_order_relaxed);
		std::atomic_thread_fence(std::memory_order_seq_cst);
		auto t = _top.load(std::memory_order_relaxed);
		T* x = nullptr;
		if (t <= b) {
			x = _buf[b & mask].load(std::memory_order_relaxed);
			if (t == b) {
				// The last element: race the thieves for it.
				if (!_top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
					x = nullptr;
				}
				_bottom.store(b + 1, std::memory_order_relaxed);
			}
		} else {
			_bottom.store(b + 1, std::memory_order_relaxed);
		}
		return x;
	}

	// The owner's FIFO take (for group-ordered work): the oldest element, by the thief's path.
	T* pop_front() noexcept { return steal(); }

	// A thief's take: FIFO from the top; nullptr when empty or when it lost a race (retry).
	T* steal() noexcept {
		auto t = _top.load(std::memory_order_acquire);
		std::atomic_thread_fence(std::memory_order_seq_cst);
		auto b = _bottom.load(std::memory_order_acquire);
		if (t < b) {
			T* x = _buf[t & mask].load(std::memory_order_relaxed);
			if (!_top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
				return nullptr;
			}
			return x;
		}
		return nullptr;
	}

	std::int64_t size() const noexcept {
		auto b = _bottom.load(std::memory_order_relaxed);
		auto t = _top.load(std::memory_order_relaxed);
		return b > t ? b - t : 0;
	}

	bool empty() const noexcept { return size() == 0; }
};

// Bounded multi-producer multi-consumer ring: one CAS per operation, no allocation.
template <typename T, std::size_t Cap>
class Mpmc {
	static_assert((Cap & (Cap - 1)) == 0, "Cap must be a power of two");
	static constexpr std::size_t mask = Cap - 1;

	struct Cell {
		std::atomic<std::size_t> seq;
		T* data;
	};

	alignas(64) std::unique_ptr<Cell[]> _cells;
	alignas(64) std::atomic<std::size_t> _enq{0};
	alignas(64) std::atomic<std::size_t> _deq{0};

public:
	Mpmc() : _cells(new Cell[Cap]) {
		for (std::size_t i = 0; i < Cap; ++i) {
			_cells[i].seq.store(i, std::memory_order_relaxed);
			_cells[i].data = nullptr;
		}
	}

	bool push(T* x) noexcept {
		auto pos = _enq.load(std::memory_order_relaxed);
		for (;;) {
			Cell& c = _cells[pos & mask];
			auto seq = c.seq.load(std::memory_order_acquire);
			auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
			if (diff == 0) {
				if (_enq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
					c.data = x;
					c.seq.store(pos + 1, std::memory_order_release);
					return true;
				}
			} else if (diff < 0) {
				return false;  // full
			} else {
				pos = _enq.load(std::memory_order_relaxed);
			}
		}
	}

	T* pop() noexcept {
		auto pos = _deq.load(std::memory_order_relaxed);
		for (;;) {
			Cell& c = _cells[pos & mask];
			auto seq = c.seq.load(std::memory_order_acquire);
			auto diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
			if (diff == 0) {
				if (_deq.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
					T* x = c.data;
					c.seq.store(pos + mask + 1, std::memory_order_release);
					return x;
				}
			} else if (diff < 0) {
				return nullptr;  // empty
			} else {
				pos = _deq.load(std::memory_order_relaxed);
			}
		}
	}

	// Approximate; for "is there anything" checks before parking.
	bool empty() const noexcept {
		return _deq.load(std::memory_order_acquire) >= _enq.load(std::memory_order_acquire);
	}

	std::size_t size() const noexcept {
		auto e = _enq.load(std::memory_order_acquire);
		auto d = _deq.load(std::memory_order_acquire);
		return e > d ? e - d : 0;
	}
};

}  // namespace research
