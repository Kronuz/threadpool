/*
 * Copyright © 2026 Germán Méndez Bravo (Kronuz)
 * Copyright (c) 2015-2019 Dubalu LLC
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

#include <array>                 // for std::array
#include <atomic>                // for std::atomic
#include <cassert>               // for assert
#include <chrono>                // for std::chrono
#include <cstddef>               // for std::size_t
#include <format>                // for std::vformat, std::make_format_args
#include <functional>            // for std::function
#include <future>                // for std::future, std::packaged_task, std::promise
#include <memory>                // for std::shared_ptr, std::unique_ptr
#include <stdexcept>             // for std::runtime_error
#include <string>                // for std::string
#include <tuple>                 // for std::make_tuple, std::apply
#include <type_traits>           // for std::invoke_result_t
#include <vector>                // for std::vector

#include "blocking_concurrent_queue.h"  // not used by the pool; kept for consumers that include it from here
#include "concurrent_queue.h"    // for ConcurrentQueue (used by TaskQueue)
#include "likely.h"              // for likely, unlikely
#include "pool_queue.h"          // for PoolQueue, the workers' queue
#include "small_task.h"          // for SmallTask, the queued form of a std::function<void()>
#include "thread.hh"             // for Thread

// Tracing is optional and fully injectable. Define THREADPOOL_TRACE_HEADER (a
// header path) before including this file to plug in your own L_EXC logging
// macro; otherwise the bundled no-op stub in threadpool_trace.h is used. See
// threadpool_trace.h.
#ifdef THREADPOOL_TRACE_HEADER
#  include THREADPOOL_TRACE_HEADER
#else
#  include "threadpool_trace.h"
#endif


using namespace std::chrono_literals;


////////////////////////////////////////////////////////////////////////////////

/* Since std::packaged_task cannot be copied, and std::function requires it can,
 * we add a dummy copy constructor to std::packaged_task. (We need to make sure
 * we actually really never copy the object though!)
 * [https://stackoverflow.com/q/39996132/167522]
 */
template <typename Result>
class PackagedTask : public std::packaged_task<Result> {
  public:
	PackagedTask() noexcept
	  : std::packaged_task<Result>() {}

	template <typename F>
	explicit PackagedTask(F&& f)
	  : std::packaged_task<Result>(std::forward<F>(f)) {}

	PackagedTask(const PackagedTask& /*unused*/) noexcept {
		// Adding this borks the compile
		assert(false);  // but should never be called!
	}

	PackagedTask(PackagedTask&& other) noexcept = default;

	PackagedTask& operator=(PackagedTask&& other) noexcept = default;
};


////////////////////////////////////////////////////////////////////////////////

template <typename TaskType, ThreadPolicyType thread_policy>
class ThreadPool;

template <typename TaskType, ThreadPolicyType thread_policy>
class ThreadPoolThread : public Thread<ThreadPoolThread<TaskType, thread_policy>, thread_policy> {
	ThreadPool<TaskType, thread_policy>* _pool;
	std::size_t _idx;
	std::string _name;

public:
	ThreadPoolThread() noexcept :
		_pool(nullptr),
		_idx(0) {}

	ThreadPoolThread(std::size_t idx, ThreadPool<TaskType, thread_policy>* pool) noexcept :
		_pool(pool),
		_idx(idx),
		_name(std::vformat(pool->_format, std::make_format_args(idx))) {}

	void operator()();

	const std::string& name() const noexcept;
};


template <typename>
struct TaskWrapper;


// The callable is held in a threadpool_detail::SmallTask, which accepts what std::function<void()>
// accepts (and callables that only move) and stores a small one inline, so queueing a task of up
// to SmallTask::inline_size bytes does not allocate.
template <>
struct TaskWrapper<std::function<void()>> {
	threadpool_detail::SmallTask t;

	TaskWrapper() noexcept {}

	template <typename T>
	TaskWrapper(T&& t) :
		t(std::forward<T>(t)) {}

	void operator()() {
		t.operator()();
	}

	operator bool() const noexcept {
		return !!t;
	}
};


template <typename P>
struct TaskWrapper<std::shared_ptr<P>> {
	std::shared_ptr<P> t;

	TaskWrapper() {}

	template <typename T>
	TaskWrapper(T&& t) :
		t(std::forward<T>(t)) {}

	void operator()() {
		if (t) {
			t->operator()();
		}
	}

	operator bool() const noexcept {
		return !!t;
	}
};


template <typename P>
struct TaskWrapper<std::unique_ptr<P>> {
	std::unique_ptr<P> t;

	TaskWrapper() {}

	template <typename T>
	TaskWrapper(T&& t) :
		t(std::forward<T>(t)) {}

	void operator()() {
		if (t) {
			t->operator()();
		}
	}

	operator bool() const noexcept {
		return !!t;
	}
};


template <typename TaskType = std::function<void()>, ThreadPolicyType thread_policy = ThreadPolicyType::regular>
class ThreadPool {
	friend ThreadPoolThread<TaskType, thread_policy>;

	// One worker's "a task is running" flag on a line of its own: running_size() sums them, so a
	// task start and end are plain stores, not read-modify-writes on a counter all workers share.
	struct alignas(128) Running {
		std::atomic<std::size_t> n{0};
	};

	std::vector<ThreadPoolThread<TaskType, thread_policy>> _threads;
	threadpool_detail::PoolQueue<TaskWrapper<TaskType>> _queue;
	std::unique_ptr<Running[]> _running;

	const char* _format;

	std::atomic_bool _ending;
	std::atomic_bool _finished;
	std::atomic_size_t _workers;

public:
	ThreadPool(const char* format, std::size_t num_threads, std::size_t /*queue_size*/ = 1000) :
		_threads(num_threads),
		_running(new Running[num_threads]),
		_format(format),
		_ending(false),
		_finished(false),
		_workers(0) {
		for (std::size_t idx = 0; idx < num_threads; ++idx) {
			_threads[idx] = ThreadPoolThread<TaskType, thread_policy>(idx, this);
			_threads[idx].run();
		}
	}

	~ThreadPool() noexcept {
		try {
			finish();
			join();
		} catch (...) {
			L_EXC("Unhandled exception in destructor");
		}
	}


	void clear() {
		TaskWrapper<TaskType> task;
		while (_queue.try_pop(task)) {}
	}

	std::size_t size() {
		return _queue.size();
	}

	std::size_t running_size() {
		std::size_t running = 0;
		for (std::size_t idx = 0; idx < _threads.size(); ++idx) {
			running += _running[idx].n.load(std::memory_order_relaxed);
		}
		return running;
	}

	std::size_t threadpool_capacity() {
		return _threads.capacity();
	}

	std::size_t threadpool_size() {
		return _threads.size();
	}

	std::size_t threadpool_workers() {
		return _workers.load(std::memory_order_relaxed);
	}

	bool join(std::chrono::milliseconds timeout = 60s) {
		bool ret = true;
		// Divide timeout among number of running worker threads
		// to give each thread the chance to "join".
		auto threadpool_workers = _workers.load(std::memory_order_relaxed);
		if (!threadpool_workers) {
			threadpool_workers = 1;
		}
		auto single_timeout = timeout / threadpool_workers;
		for (auto& _thread : _threads) {
			auto wakeup = std::chrono::steady_clock::now() + single_timeout;
			if (!_thread.join(wakeup)) {
				ret = false;
			}
		}
		return ret;
	}

	// Flag the pool as ending, so all threads exit as soon as all queued tasks end
	void end() {
		if (!_ending.exchange(true, std::memory_order_release)) {
			for (std::size_t idx = 0; idx < _threads.size(); ++idx) {
				_queue.push(TaskWrapper<TaskType>(nullptr));
			}
		}
	}

	// Tell the tasks to finish so all threads exit as soon as possible
	void finish() noexcept {
		if (!_finished.exchange(true, std::memory_order_release)) {
			for (std::size_t idx = 0; idx < _threads.size(); ++idx) {
				_queue.push(TaskWrapper<TaskType>(nullptr));
			}
		}
	}

	template <typename Func, typename... Args>
	auto package(Func&& func, Args&&... args) {
		auto packaged_task = PackagedTask<std::invoke_result_t<Func, Args...>()>([
			func = std::forward<Func>(func),
			args = std::make_tuple(std::forward<Args>(args)...)
		]() mutable {
			return std::apply(std::move(func), std::move(args));
		});
		return packaged_task;
	}

	template <typename It>
	auto enqueue_bulk(It itemFirst, std::size_t count) {
		_queue.push_bulk(std::forward<It>(itemFirst), count);
		return true;
	}

	template <typename Func>
	auto enqueue(Func&& func) {
		_queue.push(TaskWrapper<TaskType>(std::forward<Func>(func)));
		return true;
	}

	// Runs func(args...) on a worker, with no future: the call is queued as it is, where async()
	// also makes the shared state a future needs. A worker logs what it throws (L_EXC).
	template <typename Func, typename... Args>
	auto post(Func&& func, Args&&... args) {
		return enqueue([
			func = std::forward<Func>(func),
			args = std::make_tuple(std::forward<Args>(args)...)
		]() mutable {
			std::apply(std::move(func), std::move(args));
		});
	}

	template <typename Func, typename... Args>
	auto async(Func&& func, Args&&... args) {
		using Result = std::invoke_result_t<Func, Args...>;
		std::promise<Result> promise;
		auto future = promise.get_future();
		// The promise is the future's shared state and the only allocation a small call needs;
		// package() would add a second one for the packaged task's own copy of the call. A task
		// dropped before it runs (clear(), a destroyed pool) breaks the promise, as a packaged
		// task does.
		if unlikely(!enqueue([
			promise = std::move(promise),
			func = std::forward<Func>(func),
			args = std::make_tuple(std::forward<Args>(args)...)
		]() mutable {
			try {
				if constexpr (std::is_void_v<Result>) {
					std::apply(std::move(func), std::move(args));
					promise.set_value();
				} else {
					promise.set_value(std::apply(std::move(func), std::move(args)));
				}
			} catch (...) {
				promise.set_exception(std::current_exception());
			}
		})) {
			throw std::runtime_error("Cannot enqueue task to threadpool");
		}
		return future;
	}

	bool finished() {
		return _finished.load(std::memory_order_relaxed);
	}
};


template <typename TaskType, ThreadPolicyType thread_policy>
inline const std::string&
ThreadPoolThread<TaskType, thread_policy>::name() const noexcept
{
	return _name;
}


template <typename TaskType, ThreadPolicyType thread_policy>
inline void
ThreadPoolThread<TaskType, thread_policy>::operator()()
{
	_pool->_workers.fetch_add(1, std::memory_order_relaxed);
	auto& running = _pool->_running[_idx].n;
	while (!_pool->_finished.load(std::memory_order_acquire)) {
		TaskWrapper<TaskType> task;
		// Wait up to 100ms. The queue sets running before the task leaves size(), so a task is
		// always counted in one of them.
		auto valid = _pool->_queue.pop(task, 100ms, &running);
		if likely(valid && task) {
			try {
				task();
			} catch (...) {
				L_EXC("ERROR: Task died with an unhandled exception");
			}
			running.store(0, std::memory_order_release);
		} else if (_pool->_ending.load(std::memory_order_acquire)) {
			break;
		}
	}
	_pool->_workers.fetch_sub(1, std::memory_order_relaxed);
}


////////////////////////////////////////////////////////////////////////////////

template <typename>
class TaskQueue;

template <typename R, typename... Args>
class TaskQueue<std::packaged_task<R(Args...)>> {
	using Queue = ConcurrentQueue<std::packaged_task<R(Args...)>>;
	Queue _queue;


public:
	auto enqueue(std::packaged_task<R(Args...)>&& packaged_task) {
		auto future = packaged_task.get_future();
		_queue.enqueue(std::move(packaged_task));
		return future;
	}

	bool call(Args&&... args) {
		std::packaged_task<R(Args...)> task;
		if (_queue.try_dequeue(task)) {
			task(std::forward<Args>(args)...);
			return true;
		}
		return false;
	}

	std::size_t clear() {
		std::array<std::packaged_task<R(Args...)>, ConcurrentQueueDefaultTraits::block_size> tasks;
		std::size_t cleared = 0;
		while (auto dequeued = _queue.try_dequeue_bulk(tasks.begin(), tasks.size())) {
			cleared += dequeued;
		}
		return cleared;
	}
};
