# threadpool API reference

## `ThreadPool<TaskType = std::function<void()>, ThreadPolicyType policy = regular>`

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

## `TaskGroup<Pool = ThreadPool<>>` (`task_group.hh`)

An owner-scoped set of tasks on a shared `ThreadPool<std::function<void()>>`.
The pool's own behavior is unchanged; the group layers on top of `enqueue`.

```cpp
#include "task_group.hh"

ThreadPool<> pool("W{:02}", 4);
{
	TaskGroup group(pool);
	group.enqueue([] { step(); });
	group.enqueue([](const TaskGroupStopToken& stop) {
		while (!stop.stop_requested() && more()) { chunk(); }
	});
	group.wait();
}  // the destructor closes, cancels, and waits
```

- `TaskGroup(Pool& pool, size_t max_concurrency = 0)` — `max_concurrency` caps
  how many of the group's tasks run at once (`0` means the pool's thread count).
- `enqueue(Func&&) -> bool` — queue `func()` or `func(const TaskGroupStopToken&)`;
  `false` (and the callable is destroyed) once the group is closed. A task may
  enqueue to its own group.
- `cancel() -> size_t` — drop the queued tasks that have not started (they never
  run; their destructors run on the calling thread), request stop for the running
  ones, and return how many were dropped. Tasks enqueued afterwards run normally.
- `wait()` — block until the group has no queued and no running tasks.
- `cancel_and_wait() -> size_t` — both.
- `close()` / `closed()` — reject further enqueues; queued tasks still run.
- `~TaskGroup()` — `close()`, then `cancel_and_wait()`.
- `TaskGroupStopToken::stop_requested()` — a relaxed atomic load; turns true once
  the group cancels after the task started. Valid only during the task's call.

Groups are served round-robin and each group is FIFO: one group with a thousand
queued tasks delays another group's task by at most `max_concurrency` pool tasks.
Every member is callable from any thread, but the destructor must not race other
callers (the group's own running tasks may keep calling it while it waits).
`wait()`, `cancel_and_wait()`, and the destructor deadlock if called from one of
the group's own tasks, and `wait()` needs live pool workers. The pool must outlive
the group, and `pool.clear()` would strand a group's queued tasks.

## `Thread<ThreadImpl, ThreadPolicyType policy>`

CRTP base (`thread.hh`). Derive `class Foo : public Thread<Foo, policy>` and
give `Foo` a `const std::string& name()` and an `operator()`. Then:

- `run()` — start the thread (idempotent while running).
- `join(timeout = 60s)` or `join(time_point)` — wait; rethrows any exception the
  thread's `operator()` threw.

## Free functions (`thread.hh` / `thread.cc`)

- `set_thread_name(const std::string&)` / `get_thread_name([id])` — name the
  current OS thread and look names up (kept in an internal map).
- `setup_thread(name, policy)` — what a worker calls on startup (names itself).
- `run_thread(routine, arg, policy)` — start a detached `std::thread`.
- `sched_getcpu()` — current CPU id, or `-1` when unknowable (provided on macOS;
  x86 reads the APIC id, Apple Silicon returns `-1`).

## `ThreadPolicyType`

An enum of policy tags (`regular`, `wal_writer`, `logging`, `replication`,
`doc_matchers`, `doc_preparers`, `doc_indexers`, `committers`, `fsynchers`,
`updaters`, `http_servers`, `binary_servers`, `http_clients`, `binary_clients`).
Carried as a template parameter; runtime-ignored today (see above).

## `TaskQueue<std::packaged_task<R(Args...)>>`

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
