# Architecture

This document describes the internal design of `threadpool`: how the pool, its
workers, and the queue compose; the concurrency model; the task abstractions;
and the trade-offs baked in. File references point at `threadpool.hh` unless
noted otherwise.

## Overview

`threadpool` is a fixed-size, many-producer / many-consumer worker pool. Its job
is to run submitted callables on a small set of long-lived threads, with three
submission styles (fire-and-forget, bulk, and call-and-await-future) and a clean
two-phase shutdown.

It is built in three layers:

1. A mutex-backed queue — `ConcurrentQueue` (a `std::deque` under a mutex) and
   `BlockingConcurrentQueue` (which adds a condition variable so consumers wait
   for work).
2. A thread primitive — `Thread<Impl, policy>`, a CRTP base that runs a derived
   class's `operator()` on a detached `std::thread` and exposes a joinable
   future.
3. The pool — `ThreadPool<TaskType, policy>`, which owns a vector of
   `ThreadPoolThread` workers (each a `Thread`) and one shared queue, plus the
   task abstractions (`TaskWrapper`, `PackagedTask`) that make heterogeneous
   callables uniform.

`TaskQueue` is a separate, smaller utility built on `ConcurrentQueue`: a one-shot
queue of `std::packaged_task`s you drain by calling them. It does not use the
pool.

## The queue: `ConcurrentQueue` and `BlockingConcurrentQueue`

`ConcurrentQueue<T>` (`concurrent_queue.h`) is a `std::deque<T>` guarded by a
`std::unique_ptr<std::mutex>` (a `unique_ptr` so the queue is movable). Every
operation — `enqueue`, `enqueue_bulk`, `try_dequeue`, `size`, `empty` — takes the
lock for its duration. `try_dequeue_bulk` is the one exception: it reads
`queue.empty()` and pops without taking the lock, which is only safe because the
sole caller (`TaskQueue::clear`) is single-threaded against its own queue.

`BlockingConcurrentQueue<T>` (`blocking_concurrent_queue.h`) derives from it and
adds a `std::condition_variable cond`. `enqueue` notifies one waiter,
`enqueue_bulk` notifies all. `wait_dequeue` blocks until the predicate
(`!queue.empty()`) holds; `wait_dequeue_timed(item, timeout_usecs)` is the one
the pool actually uses — with a positive timeout it `wait_until`s a deadline and
returns `false` on timeout, with `0` it polls once, with a negative value it
loops on a 1-second `wait_for`. This is what lets a worker block for up to 100 ms
waiting for a task instead of spinning.

Despite the names, neither is lock-free. They deliberately match the API of the
lock-free [moodycamel](https://github.com/cameron314/concurrentqueue) queues so
those could be swapped in, but this implementation is a plain mutex-backed deque.
For the pool's task rates the mutex is not the bottleneck.

## The thread primitive: `Thread<ThreadImpl, policy>`

`Thread<ThreadImpl, thread_policy>` (`thread.hh`) is a CRTP base: a class becomes
a runnable thread by deriving `class Foo : public Thread<Foo, policy>` and
providing `const std::string& name()` and `void operator()`.

It holds a `std::promise<void>` / `std::future<void>` pair and two
`std::atomic_bool`s, `_running` and `_joined`. `run()` flips `_running` with an
`exchange` (so a second `run()` while running is a no-op) and calls the free
function `run_thread(&Thread::_runner, this_as_ThreadImpl, thread_policy)`. The
static `_runner` is the thread entry point: it casts the argument back to the
`ThreadImpl`, calls `setup_thread(impl->name(), policy)` to name the thread, then
runs `impl->operator()()` inside a try/catch. On normal return it fulfills the
promise; on exception it stores the exception in the promise via
`set_exception`. Either way `_running` is cleared.

`join(time_point)` waits on the future until the deadline. On timeout, if the
thread is still running it returns `false` (the caller can retry); otherwise it
marks `_joined`. On ready, it `exchange`s `_joined` and calls `_future.get()`,
which **rethrows** anything the thread's `operator()` threw — so a worker's fatal
exception surfaces at the joiner. The destructor calls `join()` and swallows
exceptions, so a `Thread` is safe to drop.

`run_thread`/`setup_thread`/`set_thread_name` live in `thread.cc`.
`run_thread` simply does `std::thread(routine, arg).detach()` — the thread's
lifetime is managed through the promise/future, not a `std::thread` handle.
`set_thread_name` calls the platform `pthread_setname_np` variant (selected by
the `config.h` feature macros), records the name in an internal
`std::thread::id -> std::string` map under a mutex (so `get_thread_name` can look
it up), and then calls `THREADPOOL_THREAD_REGISTER(pthread, name)` — the
injectable hook where a crash handler registers the thread (Xapiand's
`init_thread_info`).

## The pool: `ThreadPool<TaskType, policy>`

`ThreadPool<TaskType, thread_policy>` (`threadpool.hh`) owns:

- `std::vector<ThreadPoolThread<TaskType, policy>> _threads` — the workers.
- `BlockingConcurrentQueue<TaskWrapper<TaskType>> _queue` — the shared work
  queue.
- `const char* _format` — the `std::format` string used to name workers.
- five atomics: `_ending`, `_finished`, `_enqueued`, `_running`, `_workers`.

The constructor takes `(format, num_threads, queue_size = 1000)`. It sizes the
worker vector, then constructs and `run()`s each `ThreadPoolThread`. Each worker
is named at construction by rendering `_format` with its index via
`std::vformat(_format, std::make_format_args(idx))` (e.g. `"CH{:02}"` -> `"CH00"`,
`"CH01"`, ...). `vformat` is used rather than `std::format` because `_format` is a
runtime `const char*`, not a compile-time format string.

### The worker loop

`ThreadPoolThread::operator()` (`threadpool.hh`) is the body each worker runs. It
bumps `_workers`, then loops while `_finished` is not set (acquire load). Each
iteration it `wait_dequeue_timed(task, 100000)` — blocks up to 100 ms for a task.
On a valid, non-empty task it brackets the run: `_running++`, `_enqueued--`, run
the task in a try/catch (a thrown exception is logged via `L_EXC` and swallowed,
so one bad task never kills the worker), `_running--`. If the dequeue produced
nothing and `_ending` is set, it breaks. On exit it decrements `_workers`.

The two-phase nature lives here. A `nullptr` task (enqueued by `end()`/`finish()`)
dequeues as an empty `TaskWrapper`, so `valid && task` is false; that is the
signal a worker checks `_ending` on. `finish()` additionally sets `_finished`,
which the loop condition reads, so workers stop even mid-queue.

### Submitting work

- `enqueue(func)` — `_enqueued++`, push; on a failed push, roll back `_enqueued`
  and return `false`.
- `enqueue_bulk(first, count)` — the same, by `count`.
- `package(func, args...)` — builds a `PackagedTask` whose stored closure
  `std::apply`s `func` over a captured tuple of `args`. Returns it without
  enqueuing.
- `async(func, args...)` — `package`s the call, grabs the future, enqueues a
  closure that runs the packaged task, and returns the future. If it can't
  enqueue, it throws `std::runtime_error`.

### Shutdown and joining

`end()` (drain-then-exit) and `finish()` (exit-asap) each `exchange` their flag
(so they fire once) and enqueue one `nullptr` per thread to wake every blocked
worker. `join(timeout = 60s)` divides the timeout across the running workers
(`timeout / _workers`, min 1) and joins each in turn, returning `false` if any
times out. The destructor calls `finish()` then `join()` inside a try/catch whose
catch logs through `L_EXC`.

## Task abstractions

### `TaskWrapper<TaskType>`

`TaskType` is what the queue stores. `TaskWrapper` specializes on three shapes so
the worker can invoke any of them uniformly (`threadpool.hh`):

- `TaskWrapper<std::function<void()>>` — calls `t()` directly.
- `TaskWrapper<std::shared_ptr<P>>` and `TaskWrapper<std::unique_ptr<P>>` — call
  `t->operator()()` but only if the pointer is non-null.

Each has an `operator bool` so the worker's `valid && task` check skips a null
task (which is exactly what `end()`/`finish()` enqueue as a wakeup). Using a
`shared_ptr`/`unique_ptr` task type lets tasks carry state or be polymorphic
without `std::function`'s type erasure.

### `PackagedTask<Result>`

`PackagedTask` derives from `std::packaged_task<Result>` and adds one thing: a
copy constructor that `assert(false)`s. `std::packaged_task` is move-only, but
`std::function` (and some container operations) require copyability to *compile*
even when no copy ever happens at runtime. The dummy copy constructor satisfies
the compiler; the `assert` enforces the contract that it is never actually
called. This is the documented hack from
[the StackOverflow question cited in the source](https://stackoverflow.com/q/39996132/167522).

## `TaskQueue`

`TaskQueue<std::packaged_task<R(Args...)>>` (`threadpool.hh`) is independent of
the pool. It is a `ConcurrentQueue` of packaged tasks with three operations:
`enqueue(task)` queues it and returns its future; `call(args...)` dequeues one
task and runs it with the supplied arguments (returning `false` if empty); and
`clear()` drains the queue without running anything, returning the count. Xapiand
uses it for "callbacks waiting on a condition": work that is queued now and run
later, in bulk, when a condition (a database becoming ready) is met. `clear()`'s
bulk buffer is sized by `ConcurrentQueueDefaultTraits::BLOCK_SIZE`.

## Concurrency model

The pool is many-producer, many-consumer, with all sharing mediated by the
queue's mutex and a handful of atomics:

- **Producers** (`enqueue`/`enqueue_bulk`/`async`) take the queue mutex to push
  and update `_enqueued` atomically. Any number can run concurrently.
- **Consumers** (the worker threads) take the queue mutex to pop and update
  `_running`/`_enqueued`/`_workers` atomically. The queue's condition variable
  parks idle workers so they don't spin.
- **State queries** (`size`, `running_size`, `threadpool_workers`, `finished`)
  read the atomics with relaxed loads and never lock.
- **Shutdown** is coordinated by `_ending`/`_finished` (each set once via
  `exchange`) plus the `nullptr` wakeups, and `join()` through the per-thread
  promise/future.

A task's own exception is contained: the worker's try/catch swallows it (logging
through `L_EXC`) so the worker survives. A `Thread`'s `operator()` exception (a
worker dying outside the task try/catch, which shouldn't happen here) is captured
in the promise and rethrown at `join`.

## Tracing and thread registration as injectable extension points

The library is instrumented at two points but carries none of the machinery to
service them. Both flow through hooks that are no-ops by default:

- `L_EXC(...)` — fired in the `ThreadPool` destructor's catch and in the worker's
  per-task catch (`threadpool.hh`). Reports a swallowed exception.
- `THREADPOOL_THREAD_REGISTER(pthread, name)` — fired in `set_thread_name`
  (`thread.cc`) right after a thread is named, to register it with a crash
  handler.

The defaults live in `threadpool_trace.h`, and both `threadpool.hh` and
`thread.cc` reach them through a conditional include:

```cpp
#ifdef THREADPOOL_TRACE_HEADER
#  include THREADPOOL_TRACE_HEADER   // a consumer's header, e.g. Xapiand's
#else
#  include "threadpool_trace.h"      // the bundled no-op defaults
#endif
```

So a consumer points `THREADPOOL_TRACE_HEADER` at its own header (or defines the
macros first) and gets real logging and crash registration back; otherwise the
no-ops compile the instrumentation away. Each macro in `threadpool_trace.h` is
`#ifndef`-guarded, so overriding a subset is fine. A third optional macro,
`THREADPOOL_THREAD_NAME_PREFIX` (empty by default), prepends a string to OS-level
thread names. This is a small, deliberate extension surface — two hooks and a
naming knob — not a logging framework.

## Configuration and platform detection

`thread.cc` needs to know which `pthread` thread-naming API the platform offers.
That is the only thing `config.h` carries, and it is generated by CMake from
`config.h.in` using `check_cxx_source_compiles` / `check_include_file_cxx`:

- `HAVE_PTHREADS` — `pthread.h` and `pthread_self` are available.
- `HAVE_PTHREAD_SETNAME_NP` — either the 2-arg (glibc) or 1-arg (macOS) form of
  `pthread_setname_np` compiles.
- `HAVE_PTHREAD_SET_NAME_NP` / `HAVE_PTHREAD_NP_H` — the BSD `pthread_set_name_np`
  in `<pthread_np.h>`.

`set_thread_name` (`thread.cc`) selects among these at compile time:
`pthread_setname_np(pthread, name)` on Linux, `pthread_setname_np(name)` on
macOS, `pthread_set_name_np(pthread, name)` on the BSDs.

`likely.h` is deliberately *not* part of this. It detects `__builtin_expect` on
its own with `__has_builtin` (falling back to a plain passthrough), so the
headers depend on no generated config — only `thread.cc` does.

## Design decisions and trade-offs

- **Mutex queue over lock-free.** A `std::deque` under a mutex is simple and
  obviously correct, and the condition variable gives cheap blocking. The cost is
  contention under very high task rates; the names mirror moodycamel's so a
  lock-free queue can be dropped in if that ever bites.
- **Workers poll with a 100 ms timeout** rather than blocking forever, so a
  worker periodically re-checks `_finished`/`_ending` even if no `nullptr` wakeup
  reaches it. The price is a small wakeup latency bound on shutdown.
- **Pointer task types.** Allowing `TaskType` to be a `shared_ptr`/`unique_ptr`
  callable (not just `std::function`) lets tasks carry state and be polymorphic
  without `std::function`'s allocation and type erasure on the hot path.
- **Policy as a compile-time tag.** `ThreadPolicyType` is threaded through the
  templates but ignored at runtime today. It costs nothing and reserves the seam
  for per-policy thread setup (affinity, priority) without changing any call
  site.

## Known limitations and sharp edges

- **The queue is not lock-free** despite the class names. Don't rely on lock-free
  progress guarantees.
- **`ThreadPolicyType` does nothing at runtime.** It is purely a
  source-compatibility / future-extension tag. Wiring it up is a real behavior
  change.
- **`PackagedTask`'s copy constructor `assert(false)`s.** If anything ever
  actually copies one (rather than moving), it aborts in a debug build and is UB
  in a release build. The contract is move-only.
- **`sched_getcpu` is provided only under `__APPLE__`** (Linux glibc already has
  it). On Apple Silicon it returns `-1`; callers must treat negative as
  "unknown".
- **Per-task allocation.** `async`/`package` allocate a `PackagedTask` closure
  and the queue stores type-erased wrappers; a very hot submission path pays for
  that.

## Standalone vs. Xapiand

This repository is a standalone extraction from
[Xapiand](https://github.com/Kronuz/Xapiand). The delta is pure decoupling:

- `threadpool.hh` dropped `#include "log.h"` and `#include "strings.hh"`. The two
  `L_EXC(...)` sites resolve through the injectable trace header (no-op default),
  and `strings::format(pool->_format, idx)` became
  `std::vformat(pool->_format, std::make_format_args(idx))`. The Xapiand format
  strings (`"CH{:02}"`, ...) were already `std::format` syntax, so this is a
  drop-in.
- `thread.cc` dropped `#include "traceback.h"`; `init_thread_info(...)` became
  `THREADPOOL_THREAD_REGISTER(...)` (no-op default), and the hardcoded
  `"Xapiand:"` thread-name prefix became `THREADPOOL_THREAD_NAME_PREFIX` (empty
  default).
- `likely.h` dropped `#include "config.h"` and detects `__builtin_expect`
  directly; the CMake-generated `config.h` now carries only the pthread
  thread-naming feature macros `thread.cc` needs.
- `TaskQueue::clear()`'s `Queue::BLOCK_SIZE` (which this `ConcurrentQueue` does
  not define, and which only compiled because `clear()` was never instantiated)
  became `ConcurrentQueueDefaultTraits::BLOCK_SIZE`.

The pool logic is otherwise unchanged. The tracing and crash-registration Xapiand
relies on are fully recoverable by injecting a trace header (see "Tracing and
thread registration" in `README.md`).
