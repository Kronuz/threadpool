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

### Unchanged

- The API, `ThreadPolicyType`, the two-phase `end()`/`finish()` shutdown, the 100 ms wake
  of an idle worker, FIFO order, the exceptions a task throws (logged by the worker, or
  set in the future) and `TaskType` as `std::shared_ptr` or `std::unique_ptr`.
  `BlockingConcurrentQueue` and `ConcurrentQueue` stay for `TaskQueue` and for consumers
  that include them.
