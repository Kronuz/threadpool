# threadpool

A small, dependency-free C++20 **thread pool** and the thread primitives it is
built on, extracted from [Xapiand](https://github.com/Kronuz/Xapiand).

## What it is

`threadpool` is a fixed-size worker pool. You construct it with a worker-name
format and a thread count, then hand it work three ways: fire-and-forget
(`enqueue`), bulk (`enqueue_bulk`), or call-and-await-result (`async`, which
returns a `std::future`). Each worker is a long-lived thread that pulls tasks off
a shared blocking queue and runs them until the pool is told to finish. It is the
pool Xapiand uses for its HTTP/binary servers and clients, its document
indexers/preparers, and other background work.

Alongside the pool it ships the pieces underneath it:

- `Thread<Impl, policy>` — a small CRTP base that turns any class with a
  `name()` and `operator()` into a runnable, joinable thread.
- `TaskQueue` — a one-shot queue of `std::packaged_task`s you drain by calling
  them with arguments (used for "callbacks waiting on a condition").
- `ConcurrentQueue` / `BlockingConcurrentQueue` — the mutex-backed queues the
  pool sits on.

## How it works

`ThreadPool<TaskType, ThreadPolicyType>` owns a `std::vector` of worker threads
and a single `BlockingConcurrentQueue<TaskWrapper<TaskType>>`. Producers push
tasks; each worker loops on `wait_dequeue_timed` (100 ms timeout), runs whatever
it gets, and bumps a few atomic counters (`_enqueued`, `_running`, `_workers`)
so `size()`, `running_size()`, and `threadpool_workers()` can report state
without locking. Two flags control shutdown: `end()` lets workers drain the
queue first, `finish()` tells them to stop as soon as possible. `join()` waits
for them, splitting the timeout across the running workers.

`TaskType` defaults to `std::function<void()>`, but it can be a
`std::shared_ptr<P>` or `std::unique_ptr<P>` to a callable; `TaskWrapper`
specializes on each so the worker calls it correctly (and skips a null pointer).
`async(func, args...)` wraps the call in a `PackagedTask`, enqueues a closure
that runs it, and hands you the `std::future` for the result.

`ThreadPolicyType` is a compile-time tag (`regular`, `http_clients`,
`committers`, ...) carried as a template parameter. **It is currently
runtime-ignored** — `run_thread` and `setup_thread` take it unnamed and never
read it — but it is kept so Xapiand's many `ThreadPool<T, ThreadPolicyType::x>`
call sites compile unchanged, and so a consumer can wire policy-specific behavior
(affinity, priority) into `run_thread`/`setup_thread` later without touching call
sites.

## A note on the queue

Despite the names, `ConcurrentQueue` and `BlockingConcurrentQueue` are **not
lock-free**. `ConcurrentQueue` is a `std::deque` behind a `std::mutex`;
`BlockingConcurrentQueue` adds a `std::condition_variable` so consumers can wait
for work instead of spinning. The names mirror the lock-free
[moodycamel](https://github.com/cameron314/concurrentqueue) API they were
originally a drop-in for, but this implementation trades the lock-free machinery
for a simple, obviously-correct mutex-backed queue. For Xapiand's task rates the
mutex is not the bottleneck; if you need genuine lock-free throughput, swap these
two headers for moodycamel's, whose API they match.

## Install

CMake with `FetchContent`:

```cmake
include(FetchContent)
FetchContent_Declare(
  threadpool
  GIT_REPOSITORY https://github.com/Kronuz/threadpool.git
  GIT_TAG        main
)
FetchContent_MakeAvailable(threadpool)

target_link_libraries(your_target PRIVATE threadpool::threadpool)
```

The `threadpool` target is a small `STATIC` library (it compiles `thread.cc`)
that requests `cxx_std_20`, links `Threads::Threads`, and puts both the source
dir and the generated `config.h` on your include path. Then:

```cpp
#include "threadpool.hh"
```

Requires C++20 (for `std::format` in worker naming). On macOS it builds with
clang/libc++, the same toolchain Xapiand uses.

## Usage

```cpp
#include "threadpool.hh"
#include <future>

// 4 workers named "W00".."W03"; format is a std::format string, {} gets the index.
ThreadPool<> pool("W{:02}", 4);

// Fire and forget.
pool.enqueue([] { do_some_work(); });

// Call and await a result.
std::future<int> f = pool.async([](int a, int b) { return a + b; }, 2, 3);
int sum = f.get();  // 5

pool.end();   // let workers drain the queue, then exit
pool.join();  // wait for them (default 60s, split across workers)
```

A pool of callables held by pointer (so tasks can carry state / be polymorphic):

```cpp
struct Job { void operator()(); };
ThreadPool<std::shared_ptr<Job>, ThreadPolicyType::doc_indexers> pool("DI{:02}", 8);
pool.enqueue(std::make_shared<Job>());
```

`TaskQueue` for one-shot callbacks you drain yourself:

```cpp
TaskQueue<std::packaged_task<void()>> callbacks;
auto fut = callbacks.enqueue(std::packaged_task<void()>([]{ /* ... */ }));
// later, when the awaited condition is ready:
while (callbacks.call()) { }  // runs each queued callback once
```

## API reference

### `ThreadPool<TaskType = std::function<void()>, ThreadPolicyType policy = regular>`

- `ThreadPool(const char* format, size_t num_threads, size_t queue_size = 1000)`
  — starts `num_threads` workers immediately; `format` is a `std::format` string
  rendered with the worker index to name each thread (e.g. `"CH{:02}"`).
- `enqueue(Func&&)` / `enqueue_bulk(It first, size_t count)` — push work; return
  `false` if the queue rejects it.
- `async(Func&&, Args&&...) -> std::future<R>` — package a call and get its
  future; throws `std::runtime_error` if it can't enqueue.
- `package(Func&&, Args&&...) -> PackagedTask<...>` — build the packaged task
  without enqueuing (the building block `async` uses).
- `end()` — stop once the queue drains. `finish()` — stop as soon as possible.
- `join(timeout = 60s) -> bool` — wait for workers; `false` on timeout.
- `clear()` — drop queued (not-yet-running) tasks.
- `size()`, `running_size()`, `threadpool_size()`, `threadpool_capacity()`,
  `threadpool_workers()`, `finished()` — lock-free state queries.

### `Thread<ThreadImpl, ThreadPolicyType policy>`

CRTP base (`thread.hh`). Derive `class Foo : public Thread<Foo, policy>` and
give `Foo` a `const std::string& name()` and an `operator()`. Then:

- `run()` — start the thread (idempotent while running).
- `join(timeout = 60s)` or `join(time_point)` — wait; rethrows any exception the
  thread's `operator()` threw.

### Free functions (`thread.hh` / `thread.cc`)

- `set_thread_name(const std::string&)` / `get_thread_name([id])` — name the
  current OS thread and look names up (kept in an internal map).
- `setup_thread(name, policy)` — what a worker calls on startup (names itself).
- `run_thread(routine, arg, policy)` — start a detached `std::thread`.
- `sched_getcpu()` — current CPU id, or `-1` when unknowable (provided on macOS;
  x86 reads the APIC id, Apple Silicon returns `-1`).

### `ThreadPolicyType`

An enum of policy tags (`regular`, `wal_writer`, `logging`, `replication`,
`doc_matchers`, `doc_preparers`, `doc_indexers`, `committers`, `fsynchers`,
`updaters`, `http_servers`, `binary_servers`, `http_clients`, `binary_clients`).
Carried as a template parameter; runtime-ignored today (see above).

### `TaskQueue<std::packaged_task<R(Args...)>>`

- `enqueue(task) -> std::future<R>` — queue a packaged task, get its future.
- `call(Args&&...) -> bool` — dequeue and run one task with `args`; `false` if
  empty.
- `clear() -> size_t` — drop all queued tasks, returns how many.

## Tracing and thread registration

`threadpool.hh` and `thread.cc` instrument themselves through two hooks that are
no-ops by default, so the library builds with zero dependency on any logging or
crash-reporting header:

- `L_EXC(...)` — logs an exception swallowed in the pool destructor and in a
  worker's task catch-all (two sites). No-op by default.
- `THREADPOOL_THREAD_REGISTER(pthread, name)` — called right after a thread is
  named, so an external crash handler can dump per-thread callstacks. No-op by
  default; this is the seam where Xapiand's `init_thread_info` plugs back in.

The bundled `threadpool_trace.h` supplies both as `#ifndef`-guarded no-ops.
There are two ways to plug in real implementations:

1. Point `THREADPOOL_TRACE_HEADER` at a header that defines them; the library
   includes it instead of `threadpool_trace.h`:

   ```sh
   c++ -std=c++20 -DTHREADPOOL_TRACE_HEADER='"my_trace.h"' ...
   ```

2. Define the macros before including the library headers.

To recover Xapiand's exact behavior, that header maps the hooks back to
Xapiand's own facilities:

```cpp
#include "log.h"
#include "traceback.h"
#define THREADPOOL_THREAD_REGISTER(pthread, name) init_thread_info(pthread, name)
// L_EXC comes from log.h
```

One more optional knob: `THREADPOOL_THREAD_NAME_PREFIX` (a string, empty by
default) is prepended to OS-level thread names. Set
`-DTHREADPOOL_THREAD_NAME_PREFIX='"Xapiand:"'` to reproduce the original
`Xapiand:`-prefixed names.

## Build & test

```sh
cmake -B build && cmake --build build && ctest --test-dir build
# or directly:
c++ -std=c++20 -I. test/test.cc thread.cc -o test/test && ./test/test
```

The test submits 1000 tasks to a pool and checks all ran and summed correctly,
resolves 50 `async` futures, and exercises `TaskQueue`'s enqueue/call/clear. It
prints `all threadpool tests passed` and exits 0.

## Examples

[`examples/demo.cc`](examples/demo.cc) is a runnable tour. A top-level CMake build
produces it next to the test:

```sh
cmake -B build && cmake --build build && ./build/threadpool_demo
```

It submits a batch of jobs with `async()` and collects the results from their
futures; fans a few hundred tasks across four named workers and tallies how many
each one ran, so you can watch the load spread across real threads (each task
asks `get_thread_name()` who it is running as); fires a batch with `enqueue()`
and peeks at the lock-free `threadpool_workers()` / `running_size()` / `size()`
counters while the work is still in flight; runs the same batch under both
shutdowns (`end()` drains the queue, `finish()` stops as soon as possible) so you
see drain-vs-asap side by side; and ends with `TaskQueue`, the one-shot queue of
callbacks you `enqueue` now and `call`/`clear` yourself later.

## Provenance

Extracted from [Xapiand](https://github.com/Kronuz/Xapiand), where this pool runs
the HTTP and binary servers/clients, the document indexers and preparers, the
committers and fsynchers, and the replication workers. The standalone delta is
pure decoupling: Xapiand's `log.h`, `strings::format`, and `traceback.h` were
replaced by the injectable trace header and `std::format`, and the pthread
feature detection moved into a CMake-generated `config.h`. The pool logic is
otherwise identical. See [ARCHITECTURE.md](ARCHITECTURE.md) for the design and
[AGENTS.md](AGENTS.md) for the repo map and invariants.

## License

MIT, Copyright (c) 2015-2019 Dubalu LLC. See [LICENSE](LICENSE).
