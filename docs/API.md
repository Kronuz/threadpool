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

## `WorkPool`, `WorkGroup`, `WorkItem` (`work_pool.hh`)

One pool shared by many owners, each a `WorkGroup`. Items are caller-owned, so posting
allocates nothing; groups take turns by deficit round robin; queries run first.

```cpp
#include "work_pool.hh"

WorkPool pool;                          // one worker per hardware thread, reserve 1
WorkGroup index(pool);                  // one per index

struct Piece : WorkItem {
	int shard;
};
Piece piece;
piece.run = [](WorkItem* w) noexcept { search(static_cast<Piece*>(w)->shard); };
piece.cls = WorkClass::query;
index.post(piece);                      // no allocation

index.enqueue([] { flush_partition(); });                  // boxed: one allocation
index.enqueue([](const WorkItem& self) {
	while (!self.stop_requested() && more()) { merge_a_slice(); }
}, WorkClass::merge);
index.wait();
```

### `WorkPool`

- `WorkPool(Options = {})` — starts the workers. `Options`:
  - `threads` (`0`: the hardware's concurrency).
  - `reserve` (`1`): workers kept free of flush and merge work, so a query waits for a
    wake, not for an item to end. Clamped to `threads - 1`.
  - `merge_cap` (`0`: `threads - reserve`): workers merges may hold. A worker inside a
    `wait()` serves the awaited group's items without taking a background or merge slot,
    so the caps can be exceeded by that many waiting workers, and a waiting worker serves
    no query: a host that waits often keeps `reserve >= 1`.
  - `quantum` (`100us`): service per turn; a group's turn is `quantum * weight`.
  - `name_format` (`"W{:02}"`): a `std::format` string over the worker index.
  - `merge_age` (`100ms`, `0`: never): a merge entry that has waited this long is picked
    ahead of flush entries, within `merge_cap` and the background slots.
  - `block_guard` (`nullptr`): `void (*)(const char* site) noexcept`, called when a thread
    that is not a worker enters `WorkGroup::wait()` (site `"WorkGroup::wait"`), before it
    can block. A host aborts or logs there for a thread that must never wait, such as an
    event loop (which joins a group with `on_idle()` instead). It fires on entry,
    even when the wait would return at once, and for `~WorkGroup` run on a non-worker
    (its wait), so a host's guard lets shutdown through. `sleep_until` and `~WorkPool`
    are not guarded.
- `~WorkPool()` — runs what is queued, then joins. Every `WorkGroup` must be gone.
- `post(WorkItem&)` — queue an item on `item.group` (the pool's own group when null), in
  `item.cls`, from any thread; never blocks, never allocates.
- `sleep_until(time_point, WorkGroup&) -> bool` — sleep until the deadline (`true`) or
  until the group is cancelled (`false`), in one timed futex wait.
- `threads()`, `on_worker()`.
- `merge_waiting()`, `merge_oldest()`, `merges_aged()` — the merge backlog: entries waiting
  now (approximate), how long the oldest has waited, and how many merge turns went ahead of
  flush for their age.

### `WorkGroup`

- `WorkGroup(WorkPool&, unsigned width = 0, unsigned weight = 1)` — `width` caps how many
  of the group's items of one class run at once (`0`: no cap), waiting helpers included
  (a helper is admitted while fewer than `width` turns are being served, and a ring turn
  that then finds it over the width gives its ticket up); `weight` is quanta per turn.
- `post(WorkItem&)` — set `item.group` and post it.
- `enqueue(func, WorkClass = flush) -> bool` — box `func()` or `func(const WorkItem&)` and
  post it; `false` if the box cannot be allocated. Exceptions go to `L_EXC`.
- `cancel() -> WorkItem*` — drop the queued items: boxed ones (from `enqueue`) are freed
  unrun, and the caller-owned ones come back through `next`, queries, then flushes, then
  merges, each oldest first, for the owner to free or recycle; running items see
  `stop_requested()`. Items posted afterwards run normally.
- `wait()` — until nothing is queued and nothing runs. On a worker the caller serves the
  group meanwhile, inline on its own stack, as a helper within `width`, and nothing else:
  no other group's items and no query. Off the pool it parks. A wait creates no thread.
  The contract is in "Waiting" below.
- `on_idle(WorkIdle&)`, `cancel_idle(WorkIdle&) -> bool` — the way for a thread that must
  not block (an event loop, a coroutine) to join a group. `WorkIdle` is a caller-owned
  node with a `noexcept` callback `fn(WorkIdle*)`; `on_idle` links it, and the callback
  runs once, on the thread that makes the group idle (nothing queued, nothing running), or
  at once on the calling thread when the group already is. It only posts (resumes a
  coroutine, wakes a loop) and never blocks; it may free its node. `cancel_idle` unlinks
  a node that has not fired and returns `false` when it did. No `block_guard`: it never
  waits. Which node fires first is unspecified.
- `cancel_and_wait() -> size_t` — `cancel()` (boxed items freed, caller-owned ones dropped
  unrun and left to their owner), then `wait()`; returns how many queued items it dropped.
- `stop_requested()` (for the innermost item of this group on this thread's stack),
  `set_width()`, `set_weight()`, `pool()`.
- `~WorkGroup()` — cancel (queued boxed items are freed unrun; caller-owned ones are
  left to their owner, who calls `cancel()` first to get them back), then wait. From one
  of the group's own items it aborts, as `wait()` does.

#### Waiting

1. `G.wait()` returns when nothing of G is queued or running. Call it from outside G:
   never from an item of G, nor from an item of a group whose wait is on this stack.
   `wait()`, `cancel_and_wait()` and `~WorkGroup` on a group that has an item on the
   calling thread's stack (its own group included) print the site and `std::abort()` in
   every build. To join work from inside a group, post it to a child group and wait on
   that.
2. The wait-for graph must be acyclic: an item of G blocks only by waiting on lower
   groups or on bounded external events, never on a lock, flag or future that its waiter,
   its waiter's caller, or another item of G provides. That hangs in any scheduler.
3. Hold no lock across `wait()` that the group's items take.
4. Items of G may run on the waiter's thread: they must not rely on a thread identity
   distinct from the waiter's (recursive mutexes, thread-keyed ownership) except through
   scopes they set themselves.
5. A worker inside a wait serves G or sleeps; it serves no query and takes no background
   slot. Workers lost to waits are at most the number of concurrent waits.
6. Event loops and coroutines never call `wait()` (`block_guard`); they join through
   `on_idle()`.
7. Stack: inline depth is the program's own nesting of group waits, at most the number of
   distinct groups on a wait chain; each level costs the waiting item's frame plus the
   served item's. Nothing caps it and nothing spills to another thread.

### `WorkItem`

Fields the caller sets: `run` (called once, `noexcept`; it may destroy or repost the
item), `group`, `cls`. `next`, `epoch`, and `flags` are the pool's while the item is
queued or running. `stop_requested()` tells a running item its group was cancelled
after it started. An item must stay alive until `run` starts.

`WorkClass`: `query` (latency first, may use the reserve), `flush`, `merge` (at most
`merge_cap` workers).

## `Thread<ThreadImpl, ThreadPolicyType policy>`

CRTP base (`thread.hh`). Derive `class Foo : public Thread<Foo, policy>` and
give `Foo` a `const std::string& name()` and an `operator()`. Then:

- `run()` — start the thread (a no-op while it runs). After a run ends, `run()` starts it
  again, first joining the previous run if `join()` has not, and rethrowing what it threw.
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
