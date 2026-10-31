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

1. The workers' queue — `PoolQueue` (`pool_queue.h`), an unbounded FIFO of
   chunked slots with one lock for producers and one for consumers, and the
   sleep and wake protocol that parks idle workers.
2. A thread primitive — `Thread<Impl, policy>`, a CRTP base that runs a derived
   class's `operator()` on a detached `std::thread` and exposes a joinable
   future.
3. The pool — `ThreadPool<TaskType, policy>`, which owns a vector of
   `ThreadPoolThread` workers (each a `Thread`) and one shared queue, plus the
   task abstractions (`TaskWrapper`, `SmallTask`, `PackagedTask`) that make
   heterogeneous callables uniform.

`TaskQueue` is a separate, smaller utility built on `ConcurrentQueue` (a
`std::deque` under a mutex, with `BlockingConcurrentQueue` adding a condition
variable): a one-shot queue of `std::packaged_task`s you drain by calling them.
It does not use the pool, and the pool does not use those two queues.

## The queue: `PoolQueue`

`PoolQueue<T>` (`pool_queue.h`) is what the workers take from. It holds `T` by
value in chunks of 64 slots linked into a list: an unbounded FIFO, so an enqueue
never fails or blocks.

**Two locks, one per side.** `push()` takes the tail lock, constructs the item in
the next slot, and publishes it by storing the new position (`_tail_pos`, seq_cst).
`pop()` takes the head lock and moves the oldest item out. A consumer learns that an
item exists by reading the published position, so producers and consumers meet only
on the slot a task moves through. The producer links the next chunk before it
publishes the last slot of the current one, so a consumer that finishes a chunk
always finds its successor and frees the chunk it left; a chunk is allocated once
per 64 tasks.

**The locks fit their hold times.** The tail lock is held for a few dozen
nanoseconds (a 56-byte move and two stores), so it is a test-and-test-and-set
spin lock that yields after 64 spins: a producer that waits on a `std::mutex` sleeps
in the kernel, which costs far more than the wait. The head lock is a `std::mutex`:
with many workers, the surplus ones sleep instead of crowding the lock line.

**Consumers cache the tail.** Under the head lock a consumer keeps the last tail
position it read. While the queue has a backlog, the cache says an item exists and
the line the producers write is not read at all; only an apparently empty queue
re-reads it. With a flood of tasks that removes one cache miss per task.

**Sleeping and waking.** A consumer that finds the queue empty registers under the
sleep mutex (`_waiting`), publishes the count of sleepers nobody has signaled
(`_idle`, seq_cst), looks at the queue once more, and only then waits on the
condition variable. A producer, after its seq_cst publish, reads `_idle` and takes
the sleep mutex only when it is non-zero. The two seq_cst pairs are in one total
order, so either the producer sees the sleeper or the sleeper sees the item: no
wakeup is lost. A wakeup is a token (`_signaled`): the producer adds it before
notifying, the woken worker takes it, and a burst of enqueues wakes as many sleepers
as there are tokens' worth of idle workers, not one notify per task. When the workers
are busy (a flood), `_idle` is zero and an enqueue signals nothing.

**Counters without a shared RMW.** `size()` is the truthy items pushed (stored under
the tail lock) minus the ones popped (stored under the head lock); a sentinel
(`end()`'s and `finish()`'s empty task) counts in neither. `pop()` takes a `claim`
flag, the worker's own line in the pool: the head lock sets it before the popped count
is released, so a task is always counted in `size()` or in `running_size()`.

Consumers do not spin before they sleep. A future round trip (`async(f).get()`)
sleeps the worker as it wakes the caller; a worker that spins instead keeps its core
from the caller and the round trip is 45% slower.

`ConcurrentQueue` and `BlockingConcurrentQueue` (`concurrent_queue.h`,
`blocking_concurrent_queue.h`) remain, with their API, for `TaskQueue` and for
consumers that include them. Despite the names neither is lock-free.

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
- `PoolQueue<TaskWrapper<TaskType>> _queue` — the shared work queue.
- `std::unique_ptr<Running[]> _running` — one flag per worker, each on a cache line
  of its own: set while the worker runs a task. `running_size()` sums them.
- `const char* _format` — the `std::format` string used to name workers.
- three atomics: `_ending`, `_finished`, `_workers`. `size()` is the queue's own
  count.

The constructor takes `(format, num_threads, queue_size = 1000)`; the queue is
unbounded and `queue_size` is ignored, as it always was. It sizes the
worker vector, then constructs and `run()`s each `ThreadPoolThread`. Each worker
is named at construction by rendering `_format` with its index via
`std::vformat(_format, std::make_format_args(idx))` (e.g. `"CH{:02}"` -> `"CH00"`,
`"CH01"`, ...). `vformat` is used rather than `std::format` because `_format` is a
runtime `const char*`, not a compile-time format string.

### The worker loop

`ThreadPoolThread::operator()` (`threadpool.hh`) is the body each worker runs. It
bumps `_workers`, then loops while `_finished` is not set (acquire load). Each
iteration it `pop(task, 100ms)` — blocks up to 100 ms for a task. On a valid,
non-empty task it runs it in a try/catch (a thrown exception is logged via
`L_EXC` and swallowed, so one bad task never kills the worker) and clears its
running flag; the queue set that flag under the head lock as the task left
`size()`. If the dequeue produced nothing and `_ending` is set, it breaks. On exit
it decrements `_workers`. The 100 ms wait is a deadline read once, when the worker
goes to sleep, not on every dequeue.

The two-phase nature lives here. A `nullptr` task (enqueued by `end()`/`finish()`)
dequeues as an empty `TaskWrapper`, so `valid && task` is false; that is the
signal a worker checks `_ending` on. `finish()` additionally sets `_finished`,
which the loop condition reads, so workers stop even mid-queue.

### Submitting work

- `enqueue(func)` — builds the `TaskWrapper` and pushes it; always `true`. A task
  whose callable fits `SmallTask::inline_size` (48 bytes) is stored in the queue
  slot with no allocation.
- `enqueue_bulk(first, count)` — pushes `count` items under one tail-lock hold and
  wakes up to `count` sleepers.
- `post(func, args...)` — queues `func(args...)` with no future: the bound call is
  one closure, where `async` also allocates the future's shared state.
- `package(func, args...)` — builds a `PackagedTask` whose stored closure
  `std::apply`s `func` over a captured tuple of `args`. Returns it without
  enqueuing.
- `async(func, args...)` — makes a `std::promise`, takes its future, and queues a
  closure that holds the promise and the call: the promise's shared state is the
  one allocation a small call needs (a `packaged_task` costs a second one for its own
  copy of the call). The closure runs the call and sets the value, or the exception
  it threw. A task dropped before it runs (`clear()`, a destroyed pool) breaks the
  promise, as a destroyed `packaged_task` does.

### Shutdown and joining

`end()` (drain-then-exit) and `finish()` (exit-asap) each `exchange` their flag
(so they fire once) and queue one empty task per thread: each wakes a sleeping
worker, and under `end()` each is the last thing a worker takes. `join(timeout = 60s)` divides the timeout across the running workers
(`timeout / _workers`, min 1) and joins each in turn, returning `false` if any
times out. The destructor calls `finish()` then `join()` inside a try/catch whose
catch logs through `L_EXC`.

## Task abstractions

### `TaskWrapper<TaskType>`

`TaskType` is what the queue stores. `TaskWrapper` specializes on three shapes so
the worker can invoke any of them uniformly (`threadpool.hh`):

- `TaskWrapper<std::function<void()>>` — calls `t()` directly. `t` is a
  `threadpool_detail::SmallTask` (`small_task.h`), not a `std::function`: a move-only
  `void()` callable with 48 bytes of inline storage. It accepts what `std::function`
  accepts (an empty one or a null function pointer makes an empty task) and callables
  that only move. A closure that fits and has a `noexcept` move is moved into place; a
  trivially copyable one is relocated with a `memcpy` of the buffer; a larger one is
  boxed with one allocation. `std::function`'s own buffer is 16 bytes in libstdc++ and
  24 in libc++, so the same closure that allocated there is stored inline now.
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
bulk buffer is sized by `ConcurrentQueueDefaultTraits::block_size`.

## `TaskGroup`

`TaskGroup<Pool>` (`task_group.hh`) layers owner-scoped cancellation and
cross-group fairness over an unmodified `ThreadPool<std::function<void()>>`. Its
state lives in a `shared_ptr<State>`: a mutex, a FIFO `std::deque` of boxed
tasks, counters for tickets and running tasks, a `closed` flag, and an atomic
`epoch`.

A group never puts its tasks on the pool queue. It puts *tickets* there: a
closure holding the `shared_ptr<State>` that pops and runs the group's oldest
task. A group keeps at most `max_concurrency` tickets alive (default: the pool's
thread count). After its task, a ticket requeues itself at the back of the pool
queue if the group still has work, or retires. Because each ticket runs one task
per trip through the pool's FIFO, groups are served round-robin, and a group with
a thousand queued tasks occupies at most `max_concurrency` slots ahead of anyone
else. With a one-thread pool, two busy groups alternate strictly.

- `cancel()` swaps the deque out under the lock and destroys it after unlocking,
  so dropped tasks die on the caller's thread. It bumps `epoch`; a running task's
  `TaskGroupStopToken` compares the epoch it started under with the current one.
  Tickets left on the pool queue find the deque empty and retire.
- `wait()` sleeps on a condition variable until `running == 0` and the deque is
  empty; the finishing ticket and `cancel()` notify it.
- The destructor closes, cancels, and waits. Tickets outlive the group safely
  because they own the state, and a closed group's deque stays empty, so a late
  ticket never touches the pool.
- Lock order: the pool queue's lock is never taken under the group's lock, and
  tasks are boxed in a `unique_ptr`, so no task copy, move, or destructor runs
  under the group's lock (a task's destructor may call into its own group).

Fairness is per pool trip, not per thread: plain `pool.enqueue` work shares the
same FIFO with the tickets. `pool.clear()` drops tickets the group still counts,
which strands its queued tasks; don't clear a pool that groups use.

## `WorkPool`

`WorkPool` (`work_pool.hh`) is a second, independent pool for hosts that run many
owners on one set of workers: tens of thousands of open indexes, each a `WorkGroup`,
with no thread setting. It shares only `Thread` and the trace hooks with `ThreadPool`;
`threadpool.hh` and `task_group.hh` do not change. The design and its measurements are
the research note on the `research/scheduler` branch (`docs/research/scheduler.md`).

**Items are the caller's.** A `WorkItem` is a function pointer, its group, its
`WorkClass`, and an intrusive link. The pool links items into FIFOs while they wait and
calls `run` once, so a post allocates nothing. `WorkGroup::enqueue` is the boxed
convenience: one allocation, freed after it runs.

**Lanes and the ready ring.** Each group has a lane per class (query, flush, merge): a
FIFO under the group's spinlock, a ticket count, and a deficit. A lane with queued work
holds a ticket, which is its place on that class's ready ring. A worker pops a ticket,
serves the lane for a turn, and puts the ticket back at the tail if work remains. The
ring is Dmitry Vyukov's bounded MPMC queue of group pointers (one CAS per push or pop).
Rings are chained: when a group opens and the ring could fill, a ring twice the size
becomes the tail, consumers drain the old one first, and retired rings stay allocated
until the pool ends. A post therefore never blocks.

**Deficit round robin.** A turn is `quantum * weight` of service (100 µs by default),
measured with the cycle counter (`rdtsc`, `cntvct_el0`): one read per item, where a
`steady_clock` read costs 31 µs under a VM's `hpet`. A turn that overruns leaves debt,
so a group with long items skips turns until it is even; a lane that empties keeps
neither credit nor debt. A quantum below the common item size means a group that just
became active waits for no one's backlog.

**Tickets.** A lane's first ticket is free. More are granted, one per post or per
quantum, while its queue is longer than its tickets, its width has room, and a worker is
idle beyond the reserve, so an index alone fans out to every worker within a few wakes.
Extra tickets are capped at one per worker per class, and an extra ticket retires at the
end of a turn while other groups wait, so busy groups converge on equal shares. The ring
therefore needs (open groups + workers) entries per class.

**Classes and the reserve.** Workers pick the query ring first, then flush, then merge.
Strict order alone would let long background items take every worker, so a flush or
merge turn holds one of `threads - reserve` background slots, taken with an RMW before
the pop, and merges hold at most `merge_cap`. A query waits for a wake, not for an item
to end. A merge entry carries its push time; once the oldest has waited longer than
`merge_age` (100 ms), a background pick takes it before the flush ring, under the same
slots, so a flood of flushes cannot starve merges and the reserve stays free. Starving
flushes with a flood of queries is possible; nothing ages them.

**Idle protocol.** Tokio's (`idle.rs`, MIT). One 64-bit word counts searching and
unparked workers. A post wakes a sleeper only when no worker is searching; at most half
the workers search; the last searcher to find work wakes the next; the last to park
checks the rings once more. The poster's ring CAS and the parker's state RMW are both
seq_cst, so either the post sees the parker or the parker's check sees the post. Workers
sleep on an untimed platform futex each (`futex(2)`, `os_sync_wait_on_address`,
`_umtx_op`), never `std::atomic::wait` (FreeBSD's libc++ spins first) and never a timed
wait (a timed condvar wake costs FreeBSD several times the kernel time). Shutdown unparks
every sleeper.

**Waiting without deadlock.** `WorkGroup::wait()` on a worker serves only the group it
waits on, inline, as a helper within the group's width, and nothing else: no query, no
other group's item, no new thread. A helper is admitted to a width-capped group while fewer
than `width` turns are being served (tickets still on the ring do not count, or workers
all waiting would starve the group), and a ring turn that finds a helper has taken its
place gives its ticket up. `tls_here.top` chains the items running on the stack, and a
wait on a group in the chain, its own included, aborts: the waiter would wait for work
below it. With an acyclic wait-for graph (items wait only on lower groups) every wait
ends, since a worker's wait never needs another free worker; the argument is in
`docs/mechanisms/work-pool.md`. Parked waiters wake when a turn ends with the group idle or
helpable. A thread that is not a worker calls `Options::block_guard` first, where a host
aborts for an event loop, which joins a group through `on_idle()` instead: a one-shot
callback run by the thread that makes the group idle.

**Lifetime.** A group's state is allocated when the `WorkGroup` opens and freed by
whoever drops its last reference: the destructor, or the worker that pops its last
stale ticket after a cancel. Closing never waits for the group's turn to come around.
`cancel()` returns the queued items to the owner; the destructor cancels, frees queued
boxed items, and waits for running ones. `sleep_until(deadline, group)` is one timed
futex wait on the group's word, which `cancel()` bumps.

## Concurrency model

The pool is many-producer, many-consumer, with the sharing mediated by the queue's
two locks and its sleep protocol (see "The queue"):

- **Producers** (`enqueue`/`enqueue_bulk`/`post`/`async`) take the tail lock to push
  and then, only when a worker sleeps, the sleep mutex to wake one. Any number can run
  concurrently, and they never touch a counter every worker shares.
- **Consumers** (the worker threads) take the head lock to pop. A worker that finds
  nothing sleeps on the queue's condition variable, so idle workers use no CPU beyond
  the 100 ms timeout wake.
- **State queries** (`size`, `running_size`, `threadpool_workers`, `finished`)
  read atomics with relaxed or acquire loads and never lock.
- **Shutdown** is coordinated by `_ending`/`_finished` (each set once via
  `exchange`) plus the empty-task wakeups, and `join()` through the per-thread
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

- **Two locks over lock-free.** One lock per side of a chunked list gives exact FIFO
  order, an unbounded queue, immediate reclamation of drained chunks, and no
  memory-reclamation scheme; a task never sits invisible behind a preempted producer,
  as it can in a ticketed ring. The cost is that producers serialize among themselves
  and so do consumers, which is cheap while each critical section is a few dozen
  nanoseconds.
- **No batching on the consumer side.** A worker takes one task at a time, as before.
  Taking several into a local buffer would cut lock traffic, but a task that waits for
  a later task (a future it blocks on) could then wait on its own worker's buffer: a
  deadlock the shared FIFO cannot have.
- **Workers poll with a 100 ms timeout** rather than blocking forever, so a
  worker periodically re-checks `_finished`/`_ending` even if no empty-task wakeup
  reaches it (`clear()` can consume them). The price is a small wakeup latency bound
  on shutdown and about 0.1% of a core for an idle pool.
- **Pointer task types.** Allowing `TaskType` to be a `shared_ptr`/`unique_ptr`
  callable (not just `std::function`) lets tasks carry state and be polymorphic
  without `std::function`'s allocation and type erasure on the hot path.
- **Policy as a compile-time tag.** `ThreadPolicyType` is threaded through the
  templates but ignored at runtime today. It costs nothing and reserves the seam
  for per-policy thread setup (affinity, priority) without changing any call
  site.

## Known limitations and sharp edges

- **The queue is not lock-free.** A producer preempted inside the tail lock holds up
  the other producers (they yield) until it runs again; the head lock does the same to
  consumers.
- **`ThreadPolicyType` does nothing at runtime.** It is purely a
  source-compatibility / future-extension tag. Wiring it up is a real behavior
  change.
- **`PackagedTask`'s copy constructor `assert(false)`s.** If anything ever
  actually copies one (rather than moving), it aborts in a debug build and is UB
  in a release build. The contract is move-only.
- **`sched_getcpu` is provided only under `__APPLE__`** (Linux glibc already has
  it). On Apple Silicon it returns `-1`; callers must treat negative as
  "unknown".
- **Per-task allocation.** `enqueue` and `post` of a closure up to 48 bytes allocate
  nothing; a larger one costs one allocation. `async` costs the future's shared state,
  and `package` the packaged task's state and its copy of the call.

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
  became `ConcurrentQueueDefaultTraits::block_size`.

The pool logic is otherwise unchanged. The tracing and crash-registration Xapiand
relies on are fully recoverable by injecting a trace header (see "Tracing and
thread registration" in [docs/API.md](docs/API.md)).
