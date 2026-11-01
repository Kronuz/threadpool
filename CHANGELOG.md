# Changelog

All notable changes to `threadpool`. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no
releases, so entries are dated.

## 2026-10-31

### Changed

- `ThreadPool` queues tasks in its own queue, `PoolQueue` (`pool_queue.h`), instead of
  `BlockingConcurrentQueue`. An unbounded FIFO of 64-slot chunks, it has one lock for
  producers and one for consumers, so an enqueue and a dequeue never wait for each
  other. The producers' lock is a spin lock (it is held a few dozen nanoseconds); the
  consumers' lock is a `std::mutex`, so surplus workers sleep. Consumers keep a copy of
  the published position, so a backlog is drained without reading the line the
  producers write.
- An enqueue signals only when a worker sleeps and has not been signaled. A flood of
  tasks with busy workers does no `notify_one` at all, where every enqueue used to
  signal; a worker's wait reads the clock once, when it goes to sleep, not on every
  dequeue.
- `size()` and `running_size()` no longer come from two atomics every producer and
  every worker read-modify-write per task. `size()` is the queue's pushed count (stored
  under the producers' lock) minus its popped count (under the consumers'), and
  `running_size()` sums one flag per worker, each on a line of its own. A task is still
  always counted in one of them: the queue sets the worker's flag before the task
  leaves `size()`.
- `TaskWrapper<std::function<void()>>` holds a `threadpool_detail::SmallTask`
  (`small_task.h`), a move-only `void()` callable with 48 bytes of inline storage, instead
  of a `std::function`. A closure that fits is queued with no allocation (the inline buffer
  of `std::function` is 16 bytes in libstdc++ and 24 in libc++); a larger one is boxed
  with one allocation, as before. It accepts everything `std::function<void()>` accepted,
  and callables that only move.
- `async` makes a `std::promise` and queues one closure that holds the promise and the
  call, instead of a `packaged_task` inside a second closure: one allocation fewer. Values,
  exceptions and `broken_promise` for a task dropped by `clear()` or a destroyed pool
  resolve as before.
- `enqueue_bulk` pushes under one lock hold and wakes up to `count` sleepers, where it
  used to `notify_all`.
- `enqueue` constructs the task before it counts it, so a callable that throws while it
  is stored leaves nothing counted.

### Added

- `ThreadPool::post(func, args...)`: queue a call with no future.
- `test/pool_diff.cc`: the pool against `test/threadpool_old.hh`, the frozen pool as of
  `ab02e17`: random scripts of `enqueue`/`async`/`post`/`enqueue_bulk` on one worker give the
  same execution order, futures and counters; `end()`, `clear()`, `finish()`, throwing tasks,
  the `async` call shapes and many-worker totals match.
- `test/pool_stress.cc` (TSan and ASan): exact counts under many producers, per-producer
  order, order across chunk boundaries, bursts with gaps in which no wakeup may be lost
  (a worker that times out and finds a task counts as a lost wakeup), a hook that queues a
  task in the window between "the queue is empty" and "I am a sleeper", the counted-task
  invariant, destruction exactly once under `clear()` and `finish()`, and the `shared_ptr`
  and `unique_ptr` task types.
- `test/small_task.cc` and `test/mutants.sh`: eleven mutants of the queue, the wake
  protocol and the tasks, each caught by a test.
- `bench/enqueue_bench.cc`: trivial-task throughput against the frozen pool, a mutex +
  condition variable pool and `asio::thread_pool`, with idle-CPU, round-trip and
  trickle modes.

### Performance

`bench/enqueue_bench.cc`, `ThreadPool::enqueue` of a trivial task from P producers into W
workers, ns per task, best of 7 runs. `old` is the pool before this change, frozen in
`test/threadpool_old.hh`; `naive` is a mutex and condition variable pool; `asio` is
`asio::thread_pool`. The spread (slowest over fastest of the 7 runs) is 1.04 to 1.4 except
where noted.

Apple M4 Pro (arm64):

| P x W | old | new | naive | asio |
|---|---|---|---|---|
| 1 x 1 | 65 | 19 | 32 | 49 |
| 1 x 4 | 262 | 34 | 157 | 227 |
| 1 x 8 | 2371 | 35 (spread 3.0) | 1643 | 1045 |
| 4 x 1 | 75 | 11 | 56 | 79 |
| 4 x 4 | 127 | 34 | 73 | 110 |
| 4 x 8 | 129 | 32 | 135 | 177 |

Intel Core i9-9980HK (x86_64):

| P x W | old | new | naive | asio |
|---|---|---|---|---|
| 1 x 1 | 249 | 32 | 132 | 225 |
| 1 x 4 | 1716 | 126 | 967 | 840 |
| 1 x 8 | 6196 | 223 (spread 1.8) | 1690 | 2289 |
| 4 x 1 | 269 | 40 | 148 | 244 |
| 4 x 4 | 533 | 146 | 226 | 389 |
| 4 x 8 | 843 | 221 | 525 | 708 |

Idle pools use the same CPU as before (0.04% of a core for 4 idle workers on the M4 Pro,
0.1% on the i9), and a future round trip takes the same 9 to 10 microseconds. One task
submitted every microsecond starts after 3 microseconds on the M4 Pro and 5 on the i9,
against 8 and 7 before.

### Unchanged

- The API, `ThreadPolicyType`, the two-phase `end()`/`finish()` shutdown, the 100 ms wake
  of an idle worker, FIFO order, the exceptions a task throws (logged by the worker, or
  set in the future) and `TaskType` as `std::shared_ptr` or `std::unique_ptr`.
  `BlockingConcurrentQueue` and `ConcurrentQueue` stay for `TaskQueue` and for consumers
  that include them.
