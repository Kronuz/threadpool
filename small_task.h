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

#include <cstddef>               // for std::size_t, std::nullptr_t
#include <cstring>               // for std::memcpy
#include <functional>            // for std::function, std::bad_function_call, std::invoke
#include <memory>                // for std::launder
#include <new>                   // for placement new
#include <type_traits>           // for std::decay_t, std::is_invocable_v
#include <utility>               // for std::forward, std::move


namespace threadpool_detail {

template <typename>
inline constexpr bool is_std_function_v = false;
template <typename Sig>
inline constexpr bool is_std_function_v<std::function<Sig>> = true;

// A move-only `void()` callable that stores a small callable inline. What ThreadPool queues
// for TaskType std::function<void()>: a closure of up to inline_size bytes (a promise and a few
// captured values, a std::function) is moved into the queue slot with no allocation; a larger or
// throwing-move one is boxed with one allocation, as std::function would. Calling an empty task
// throws std::bad_function_call, as std::function does. Not copyable: a queued task moves.
class SmallTask {
public:
	static constexpr std::size_t inline_size = 48;

private:
	struct Ops {
		void (*invoke)(void*);
		// Move-constructs into dst and destroys src. Null: the bytes are trivially relocatable.
		void (*relocate)(void* dst, void* src) noexcept;
		// Null: nothing to destroy.
		void (*destroy)(void*) noexcept;
	};

	template <typename D>
	static constexpr bool stored_inline =
		sizeof(D) <= inline_size && alignof(D) <= alignof(void*) && std::is_nothrow_move_constructible_v<D>;

	template <typename D, bool Inline>
	struct OpsFor;

	template <typename D>
	struct OpsFor<D, true> {
		static D& get(void* p) noexcept { return *std::launder(static_cast<D*>(p)); }
		static void invoke(void* p) { static_cast<void>(std::invoke(get(p))); }
		static void relocate(void* dst, void* src) noexcept {
			::new (dst) D(std::move(get(src)));
			get(src).~D();
		}
		static void destroy(void* p) noexcept { get(p).~D(); }
		static constexpr Ops ops{
			&invoke,
			std::is_trivially_copyable_v<D> ? nullptr : &relocate,
			std::is_trivially_destructible_v<D> ? nullptr : &destroy,
		};
	};

	template <typename D>
	struct OpsFor<D, false> {
		static D* get(void* p) noexcept { return *static_cast<D**>(p); }
		static void invoke(void* p) { static_cast<void>(std::invoke(*get(p))); }
		static void destroy(void* p) noexcept { delete get(p); }
		static constexpr Ops ops{&invoke, nullptr, &destroy};
	};

	const Ops* _ops = nullptr;
	alignas(void*) unsigned char _buf[inline_size];

	void relocate_from(SmallTask& other) noexcept {
		_ops = other._ops;
		if (_ops) {
			if (_ops->relocate) {
				_ops->relocate(_buf, other._buf);
			} else {
				std::memcpy(_buf, other._buf, inline_size);
			}
			other._ops = nullptr;
		}
	}

	void reset() noexcept {
		if (_ops) {
			if (_ops->destroy) {
				_ops->destroy(_buf);
			}
			_ops = nullptr;
		}
	}

public:
	SmallTask() noexcept = default;

	SmallTask(std::nullptr_t) noexcept {}

	template <
		typename F,
		typename D = std::decay_t<F>,
		typename = std::enable_if_t<!std::is_same_v<D, SmallTask> && std::is_invocable_v<D&>>>
	SmallTask(F&& f) {
		// An empty std::function or a null function pointer makes an empty task, as for std::function.
		if constexpr (std::is_pointer_v<D> || std::is_member_pointer_v<D> || is_std_function_v<D>) {
			if (!f) {
				return;
			}
		}
		if constexpr (stored_inline<D>) {
			::new (static_cast<void*>(_buf)) D(std::forward<F>(f));
		} else {
			*reinterpret_cast<D**>(_buf) = new D(std::forward<F>(f));
		}
		_ops = &OpsFor<D, stored_inline<D>>::ops;
	}

	SmallTask(const SmallTask&) = delete;
	SmallTask& operator=(const SmallTask&) = delete;

	SmallTask(SmallTask&& other) noexcept {
		relocate_from(other);
	}

	SmallTask& operator=(SmallTask&& other) noexcept {
		if (this != &other) {
			reset();
			relocate_from(other);
		}
		return *this;
	}

	SmallTask& operator=(std::nullptr_t) noexcept {
		reset();
		return *this;
	}

	~SmallTask() {
		reset();
	}

	explicit operator bool() const noexcept {
		return _ops != nullptr;
	}

	void operator()() {
		if (!_ops) {
			throw std::bad_function_call();
		}
		_ops->invoke(_buf);
	}
};

}  // namespace threadpool_detail
