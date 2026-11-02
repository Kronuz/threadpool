# Architecture

This document describes the internal design of `threadpool`: the thread primitive, the
`WorkPool` that runs on it, the concurrency model, and the trade-offs baked in. File
references point at `work_pool.hh` unless noted otherwise.

## Overview

`threadpool` is a fixed-size worker pool shared by many owners. Its job is to run
submitted work on a small set of long-lived, named threads, serving each owner's
`WorkGroup` in fair turns, with latency classes and a wait that cannot deadlock.

It is built in two layers:

1. A thread primitive — `Thread<Impl, policy>` (`thread.hh`), a CRTP base that runs a
   derived class's `operator()` on a detached `std::thread` and exposes a joinable
   future.
2. The pool — `WorkPool` (`work_pool.hh`), which owns the workers (each a `Thread`), the
   rings of ready groups and the idle protocol, and `WorkGroup`, one owner's lanes.

`ConcurrentQueue` and `BlockingConcurrentQueue` (`concurrent_queue.h`,
`blocking_concurrent_queue.h`) are separate utilities: a `std::deque` under a mutex, the
second with a condition variable. The pool does not use them.

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

## `WorkPool`

`WorkPool` (`work_pool.hh`) is the library's pool, for hosts that run many owners on one
set of workers: tens of thousands of open indexes, each a `WorkGroup`, with no thread
setting. It builds on `Thread` and the trace hooks. The design and its measurements are
in the research note `docs/research/scheduler.md`.

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

The pool is many-producer, many-consumer. A post takes the owning group's spinlock to
link the item, and wakes a sleeper only when no worker is searching (see "Idle protocol"
above). Workers pop group tickets from the class rings with one CAS each, serve a lane for
a turn under the group's spinlock, and sleep on their own futex word. `pending()`,
`running()` and `threads()` read the group's state under its lock or a constant, and never
touch the rings.

An item's own exception is contained: the worker's catch swallows it (reporting through
`L_EXC`) so the worker survives. A `Thread`'s `operator()` exception (a worker dying
outside the item catch, which should not happen here) is captured in the promise and
rethrown at `join`.

## Tracing and thread registration as injectable extension points

The library is instrumented at a few points but carries none of the machinery to
service them. They flow through hooks that are no-ops by default:

- `L_EXC(...)` — fired where `WorkPool` swallows an exception (a worker that fails to
  join, a task that throws: `work_pool.hh`).
- `THREADPOOL_THREAD_REGISTER(pthread, name)` — fired in `set_thread_name`
  (`thread.cc`) right after a thread is named, to register it with a crash
  handler.

The defaults live in `threadpool_trace.h`, and both `work_pool.hh` and
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
thread names. This is a small, deliberate extension surface — a few hooks and a
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

- **Items are the caller's.** The pool links caller-owned items and never copies them, so
  the common post allocates nothing; the boxed `enqueue` is the convenience.
- **One mechanism for all owners.** Fairness, classes, widths and cancel are the group's
  and the pool's; there is no second pool kind for a single owner (a group alone on a pool
  is the single-owner case).
- **Policy as a compile-time tag.** `ThreadPolicyType` is threaded through the `Thread`
  template and through the scheduler library's templates, but ignored at run time. It
  costs nothing and keeps those call sites unchanged.

## Known limitations and sharp edges

- **Starving flushes with a flood of queries is possible;** nothing ages them.
- **`ThreadPolicyType` does nothing at runtime.** It is a source-compatibility tag.
- **`sched_getcpu` is provided only under `__APPLE__`** (Linux glibc already has
  it). On Apple Silicon it returns `-1`; callers must treat negative as
  "unknown".

## Standalone vs. Xapiand

This repository is a standalone extraction from
[Xapiand](https://github.com/Kronuz/Xapiand). The delta is pure decoupling:

- `thread.cc` dropped `#include "traceback.h"`; `init_thread_info(...)` became
  `THREADPOOL_THREAD_REGISTER(...)` (no-op default), and the hardcoded
  `"Xapiand:"` thread-name prefix became `THREADPOOL_THREAD_NAME_PREFIX` (empty
  default).
- `likely.h` dropped `#include "config.h"` and detects `__builtin_expect`
  directly; the CMake-generated `config.h` now carries only the pthread
  thread-naming feature macros `thread.cc` needs.

The tracing and crash-registration Xapiand relies on are fully recoverable by injecting
a trace header (see "Tracing and thread registration" in [docs/API.md](docs/API.md)).
