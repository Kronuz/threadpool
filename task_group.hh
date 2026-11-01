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

#pragma once

#include <atomic>                // for std::atomic
#include <condition_variable>    // for std::condition_variable
#include <cstddef>               // for std::size_t
#include <cstdint>               // for std::uint64_t
#include <deque>                 // for std::deque
#include <functional>            // for std::function
#include <memory>                // for std::shared_ptr, std::unique_ptr
#include <mutex>                 // for std::mutex, std::lock_guard, std::unique_lock
#include <type_traits>           // for std::decay_t, std::is_invocable_v
#include <utility>               // for std::forward, std::move

#include "threadpool.hh"         // for ThreadPool, L_EXC


////////////////////////////////////////////////////////////////////////////////

template <typename Pool = ThreadPool<>>
class TaskGroup;


// Handed to a running TaskGroup task; stop_requested() turns true once the
// group cancels after the task started. Valid only during the task's call.
class TaskGroupStopToken {
	template <typename Pool>
	friend class TaskGroup;

	const std::atomic<std::uint64_t>& _epoch;
	std::uint64_t _started;

	TaskGroupStopToken(const std::atomic<std::uint64_t>& epoch, std::uint64_t started) noexcept :
		_epoch(epoch),
		_started(started) {}

public:
	TaskGroupStopToken(const TaskGroupStopToken&) = delete;
	TaskGroupStopToken& operator=(const TaskGroupStopToken&) = delete;

	bool stop_requested() const noexcept {
		return _epoch.load(std::memory_order_relaxed) != _started;
	}
};


// Owner-scoped tasks on a shared pool, round-robin across groups and FIFO
// within one (see ARCHITECTURE.md). Thread-safe, but the destructor must not
// race other callers, and wait(), cancel_and_wait(), and the destructor must
// not run inside the group's own tasks. The pool must outlive the group.
template <typename Pool>
class TaskGroup {
	// Boxed once, so no task copy, move, or destructor runs under the group's lock.
	using Function = std::function<void(const TaskGroupStopToken&)>;
	using Task = std::unique_ptr<Function>;

	struct State {
		Pool* pool;
		std::size_t max_tickets;

		std::mutex mtx;
		std::condition_variable idle;
		std::deque<Task> queued;
		std::size_t tickets;         // tickets on the pool queue, on their way to a worker, or held by one
		std::size_t running;         // tasks running: outside mtx, also the tickets workers hold
		std::atomic_bool closed;           // written under mtx
		std::atomic<std::uint64_t> epoch;  // bumped by cancel(), written under mtx
		// The group's tickets on the pool queue: up just before a ticket is pushed, down when a
		// worker enters one, so it never counts fewer than the queue holds.
		std::atomic<std::size_t> in_pool;

		State(Pool* pool, std::size_t max_tickets) :
			pool(pool),
			max_tickets(max_tickets),
			tickets(0),
			running(0),
			closed(false),
			epoch(0),
			in_pool(0) {}

		// Under mtx: whether to put one more ticket on the pool queue for the queued tasks. Only
		// when every live ticket is held by a worker: one on its way to a worker will reach the
		// tasks, and the worker that takes it lists the next. A group so widens one hand-off at a
		// time up to max_tickets, and a producer slower than the workers does not post a ticket
		// (and wake a worker) per task.
		bool list() {
			if (tickets == running && tickets < max_tickets && !queued.empty()) {
				++tickets;
				return true;
			}
			return false;
		}

		// Under mtx: takes the oldest task to run.
		Task take(std::uint64_t& started) {
			Task task = std::move(queued.front());
			queued.pop_front();
			++running;
			started = epoch.load(std::memory_order_relaxed);
			return task;
		}

		// Whether a worker that finished a task must put its ticket behind the pool's other work:
		// something besides this group's own tickets waits on the pool queue, or the pool stops
		// (finish() or end()). Atomic loads only, no lock of the pool's; off by one for the moment
		// a ticket of the group's is on its way into or out of the queue (ARCHITECTURE.md).
		bool yield() {
			const auto mine = in_pool.load(std::memory_order_relaxed);
			return pool->size() > mine || pool->finished() || pool->ending();
		}
	};

	std::shared_ptr<State> _state;

	// Puts one ticket (already counted in `tickets`) on the pool queue.
	static void post_ticket(const std::shared_ptr<State>& state) {
		auto ticket = [state]() {
			dispatch(state);
		};
		state->in_pool.fetch_add(1, std::memory_order_relaxed);
		bool posted = false;
		try {
			posted = state->pool->enqueue(std::move(ticket));
		} catch (...) {
			unpost(state);
			throw;
		}
		if unlikely(!posted) {
			unpost(state);
		}
	}

	// A ticket that did not reach the pool queue.
	static void unpost(const std::shared_ptr<State>& state) {
		state->in_pool.fetch_sub(1, std::memory_order_relaxed);
		std::lock_guard<std::mutex> lk(state->mtx);
		--state->tickets;
	}

	static void run(const std::shared_ptr<State>& state, Task& task, std::uint64_t started) {
		try {
			const TaskGroupStopToken token(state->epoch, started);
			(*task)(token);
		} catch (...) {
			L_EXC("ERROR: Task died with an unhandled exception");
		}
		// Captures die before wait() can observe the task as finished.
		task.reset();
	}

	// A ticket's body: runs the group's oldest task, and then the next one for as long as nothing
	// but the group's own tickets waits on the pool queue; then requeues the ticket at the back or
	// retires it. The task a worker runs next is the one its requeued ticket would have run when it
	// came back to the front, so turns keep their order without a trip through the pool queue per
	// task.
	static void dispatch(const std::shared_ptr<State>& state) {
		state->in_pool.fetch_sub(1, std::memory_order_relaxed);
		Task task;
		std::uint64_t started;
		bool widen;
		{
			std::lock_guard<std::mutex> lk(state->mtx);
			if (state->queued.empty()) {
				--state->tickets;
				return;
			}
			task = state->take(started);
			widen = state->list();
		}
		for (;;) {
			if (widen) {
				post_ticket(state);
			}
			run(state, task, started);
			{
				std::lock_guard<std::mutex> lk(state->mtx);
				--state->running;
				if (state->queued.empty()) {
					--state->tickets;
					if (state->running == 0) {
						state->idle.notify_all();
					}
					return;
				}
				if (state->yield()) {
					break;
				}
				task = state->take(started);
				widen = state->list();
			}
		}
		post_ticket(state);
	}

public:
	// max_concurrency caps how many of the group's tasks run at once; 0 means
	// the pool's thread count.
	explicit TaskGroup(Pool& pool, std::size_t max_concurrency = 0) {
		auto max_tickets = max_concurrency ? max_concurrency : pool.threadpool_size();
		_state = std::make_shared<State>(&pool, max_tickets ? max_tickets : 1);
	}

	TaskGroup(const TaskGroup&) = delete;
	TaskGroup& operator=(const TaskGroup&) = delete;

	~TaskGroup() noexcept {
		try {
			close();
			cancel_and_wait();
		} catch (...) {
			L_EXC("Unhandled exception in destructor");
		}
	}

	// Queues `func` (callable as func() or func(const TaskGroupStopToken&)).
	// Returns false, dropping func on the caller's thread, once the group is closed.
	template <typename Func>
	bool enqueue(Func&& func) {
		using F = std::decay_t<Func>;
		Task task;
		if constexpr (std::is_invocable_v<F&, const TaskGroupStopToken&>) {
			task = std::make_unique<Function>(std::forward<Func>(func));
		} else {
			static_assert(std::is_invocable_v<F&>, "a TaskGroup task takes no arguments or a const TaskGroupStopToken&");
			task = std::make_unique<Function>([func = std::forward<Func>(func)](const TaskGroupStopToken&) mutable {
				func();
			});
		}
		bool ticket = false;
		{
			std::lock_guard<std::mutex> lk(_state->mtx);
			if (_state->closed.load(std::memory_order_relaxed)) {
				return false;
			}
			_state->queued.push_back(std::move(task));
			ticket = _state->list();
		}
		// The pool queue's lock is never taken under the group's lock.
		if (ticket) {
			post_ticket(_state);
		}
		return true;
	}

	// Drops the queued tasks that have not started, destroying them on the
	// calling thread, and requests stop for the running ones. Returns how many
	// were dropped. Tasks queued afterwards run normally.
	std::size_t cancel() {
		std::deque<Task> dropped;
		{
			std::lock_guard<std::mutex> lk(_state->mtx);
			dropped.swap(_state->queued);
			_state->epoch.fetch_add(1, std::memory_order_relaxed);
			if (_state->running == 0) {
				_state->idle.notify_all();
			}
		}
		return dropped.size();
	}

	// Blocks until the group has no queued and no running tasks, including
	// tasks queued meanwhile (by its own tasks, say).
	void wait() {
		std::unique_lock<std::mutex> lk(_state->mtx);
		_state->idle.wait(lk, [this] {
			return _state->running == 0 && _state->queued.empty();
		});
	}

	std::size_t cancel_and_wait() {
		auto dropped = cancel();
		wait();
		return dropped;
	}

	// Rejects further enqueues; tasks already queued still run.
	void close() {
		std::lock_guard<std::mutex> lk(_state->mtx);
		_state->closed.store(true, std::memory_order_relaxed);
	}

	bool closed() {
		return _state->closed.load(std::memory_order_relaxed);
	}
};
