// Idle bookkeeping after Tokio's idle.rs: one word counts searching and unparked workers; a
// sleeper list under a mutex; a Parker per worker. A post wakes one worker only when nobody
// is searching, the last searcher to find work wakes the next, and at most half the workers
// search at once. Research code.
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

#include "futex.hh"

namespace research {

class Idle {
	static constexpr std::uint32_t search_mask = 0xffff;
	static constexpr unsigned unpark_shift = 16;

	alignas(64) std::atomic<std::uint32_t> _state;  // searching | unparked << 16
	std::mutex _mu;
	std::vector<unsigned> _sleepers;
	std::vector<Parker> _parkers;
	unsigned _n;

	static std::uint32_t searching(std::uint32_t s) noexcept { return s & search_mask; }
	static std::uint32_t unparked(std::uint32_t s) noexcept { return s >> unpark_shift; }

public:
	explicit Idle(unsigned workers) : _state(static_cast<std::uint32_t>(workers) << unpark_shift), _parkers(workers), _n(workers) {
		_sleepers.reserve(workers);
	}

	unsigned workers() const noexcept { return _n; }

	// When no worker is searching and one is parked, unparks it and returns true. SeqCst so a
	// poster's queue push is ordered before a parking worker's final emptiness check.
	bool notify_one() noexcept {
		auto s = _state.load(std::memory_order_seq_cst);
		if (searching(s) != 0 || unparked(s) >= _n) { return false; }
		unsigned idx;
		{
			std::lock_guard<std::mutex> lk(_mu);
			s = _state.load(std::memory_order_seq_cst);
			if (searching(s) != 0 || unparked(s) >= _n || _sleepers.empty()) { return false; }
			idx = _sleepers.back();  // LIFO: the warmest sleeper
			_sleepers.pop_back();
			_state.fetch_add(1 | (1u << unpark_shift), std::memory_order_seq_cst);
		}
		_parkers[idx].unpark();
		return true;
	}

	// Unparks every sleeper (shutdown).
	void notify_all() noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		for (auto idx : _sleepers) {
			_state.fetch_add(1 | (1u << unpark_shift), std::memory_order_seq_cst);
			_parkers[idx].unpark();
		}
		_sleepers.clear();
	}

	// False when half the workers already search (an approximate cap).
	bool to_searching() noexcept {
		auto s = _state.load(std::memory_order_seq_cst);
		if (2 * searching(s) >= _n) { return false; }
		_state.fetch_add(1, std::memory_order_seq_cst);
		return true;
	}

	// True when this was the last searcher: the caller must wake another worker.
	bool from_searching() noexcept {
		auto prev = _state.fetch_sub(1, std::memory_order_seq_cst);
		return searching(prev) == 1;
	}

	// Registers idx as a sleeper and returns the parker epoch to park on. `was_searching`
	// drops the search count too; `*final_check` tells the caller to look at the queues once
	// more before sleeping (it was the last searcher).
	std::uint32_t to_parked(unsigned idx, bool was_searching, bool* final_check) noexcept {
		std::uint32_t observed;
		{
			std::lock_guard<std::mutex> lk(_mu);
			observed = _parkers[idx].epoch();
			_sleepers.push_back(idx);
			auto prev = _state.fetch_sub((1u << unpark_shift) + (was_searching ? 1u : 0u), std::memory_order_seq_cst);
			*final_check = was_searching && searching(prev) == 1;
		}
		return observed;
	}

	// Removes idx from the sleepers if it is there (it found work at the final check).
	bool unpark_self(unsigned idx) noexcept {
		std::lock_guard<std::mutex> lk(_mu);
		for (std::size_t i = 0; i < _sleepers.size(); ++i) {
			if (_sleepers[i] == idx) {
				_sleepers[i] = _sleepers.back();
				_sleepers.pop_back();
				_state.fetch_add(1u << unpark_shift, std::memory_order_seq_cst);
				return true;
			}
		}
		return false;
	}

	// Wakes a specific sleeping worker (a group's home); false when it is not parked.
	bool unpark_if_parked(unsigned idx) noexcept {
		if (unparked(_state.load(std::memory_order_seq_cst)) >= _n) { return false; }
		{
			std::lock_guard<std::mutex> lk(_mu);
			bool found = false;
			for (std::size_t i = 0; i < _sleepers.size(); ++i) {
				if (_sleepers[i] == idx) {
					_sleepers[i] = _sleepers.back();
					_sleepers.pop_back();
					found = true;
					break;
				}
			}
			if (!found) { return false; }
			_state.fetch_add(1 | (1u << unpark_shift), std::memory_order_seq_cst);
		}
		_parkers[idx].unpark();
		return true;
	}

	void park(unsigned idx, std::uint32_t observed) noexcept { _parkers[idx].park(observed); }

	unsigned num_unparked() const noexcept { return unparked(_state.load(std::memory_order_relaxed)); }
	unsigned num_searching() const noexcept { return searching(_state.load(std::memory_order_relaxed)); }
};

}  // namespace research
