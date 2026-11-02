# Changelog

All notable changes to `threadpool`. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no
releases, so entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#threadpool) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-01

### Changed

- `TaskGroup` runs a burst on the workers that hold its tickets. After a task, a worker
  runs the group's next one while nothing but the group's own tickets waits on the pool
  queue, and requeues its ticket behind anything else, as every ticket did after every
  task before. A group also lists one ticket at a time: an enqueue posts a ticket only
  when every live ticket is held by a worker, and the worker that takes a ticket lists
  the next before it runs its task, so a group still widens to `max_concurrency`, one
  hand-off at a time. Round-robin across groups, FIFO within one, `max_concurrency`,
  `cancel()`, stop tokens, exceptions and `wait()` are unchanged, and a draining worker
  requeues once the pool is stopping, so `end()` and `finish()` behave as before.

  The cause of the slow burst, from `sample` stacks of a 200 000-task burst on 16
  workers: the workers outrun the one producer and sleep, a ticket retires as soon as
  its group runs dry, and an enqueue posted a ticket whenever fewer than
  `max_concurrency` were alive, so nearly every task posted one and woke a sleeper. Since
  2026-10-31 that wake is a condition-variable signal plus the sleepers' mutex, which 14
  to 16 parking workers keep busy: 65% of the producer's samples on the M4 Pro, 45% on
  the i9, with 0.3 to 1.1 wakeups and 0.6 to 3.3 context switches per task. The
  producers' spin lock, which the gate suspected, was 2% of the samples.

### Added

- `ThreadPool::ending()`: whether `end()` was called.
- `test/task_group.cc`: a burst on busy workers lists exactly one ticket and takes fewer
  than one per ten tasks (both fail with the previous `task_group.hh`), a group of tasks
  that wait for each other widens to the pool and to its cap, and plain tasks interleave
  with a draining group in FIFO order.
- `test/task_group_stress.cc` (TSan and ASan): a 16-worker pool with bursts from one
  producer and from four, a group of `max_concurrency` 1 in FIFO order, groups beside
  plain tasks with cancels, tasks that post to their own group, and groups destroyed with
  a burst queued.
- `test/mutants.sh`: five `TaskGroup` mutants, each caught by `test/task_group.cc`.
- `kernel_time [trials] [threads] [rows]`: `rows` picks some of handoff, burst, idle and
  sleep; `kernel_time 5 16 burst` is the burst rows alone, on 16 workers.

### Performance

`kernel_time 1 W burst` (`bench/kernel_time.cc`): 200 000 tiny tasks from one producer,
wall-clock ns per task until the last one ran, the median of 5 A/B/A rounds in one strict
hold per machine. START is `e961e44`, before the pool's own queue; 2026-10-31 is `287b877`.

| burst | M4 Pro: START | 2026-10-31 | now | i9: START | 2026-10-31 | now |
|---|---|---|---|---|---|---|
| `TaskGroup`, 4 workers | 345 | 235 | **115** | 1140 | 775 | **475** |
| `TaskGroup`, 14 workers | 445 | 1365 | **125** | 1950 | 4210 | **525** |
| `TaskGroup`, 16 workers | 490 | 1640 | **125** | 1725 | 3245 | **515** |

Against START the `TaskGroup` burst is 3.0x to 3.9x faster on the M4 Pro and 2.4x to 3.8x
on the i9 (A/A within 0.90 to 1.04), and its CPU per task on 16 workers fell from 4.3 to
0.8 us (M4 Pro) and from 24 to 2.7 us (i9). The bare `ThreadPool` and `WorkPool` rows,
`enqueue`, `async` and idle CPU run the same code as on 2026-10-31 (`enqueue_bench`'s binary
is identical); all stay inside their A/A spread but the bare burst on 14 to 16 workers, which
is bimodal in both builds (below).

### Known

- One task submitted every microsecond into 4 workers (`enqueue_bench 1 20000 trickle`)
  starts later at the 99th percentile on the i9 than at START: 67 to 84 us against 44 to
  47 us, with the median at 6.2 to 6.4 against 6.9 to 7.0 us and the pool's CPU per task
  at 2.0 against 7.4 us. On the M4 Pro the same row is 3.2x better than START (15 against
  49 us). It is the 2026-10-31 wake protocol, which wakes a worker only when one sleeps
  unsignaled and never spins before sleeping; a spin would buy the tail back with CPU and
  a slower future round trip (45% slower when it was tried), so the trade stays. A
  consumer that polls only while items arrive under 3 us apart (branch `perf/trickle-p99`)
  halves the median (6.6 to 3.0 us) and keeps the CPU at 2.1 us, but its p99 is 55 to 75
  us against START's 35 to 46, and it slows the 4-producer, 1-worker enqueue row to 0.6x;
  [issue #3](https://github.com/Kronuz/threadpool/issues/3) has the numbers and next steps.
- The bare `ThreadPool` burst on 14 to 16 workers of a fresh pool is bimodal on both
  machines, at 2026-10-31 and now alike: on the i9 11 of 35 runs (7 of 20 at 2026-10-31,
  4 of 15 now) took 2.7 to 5.3 us per task, as the workers outran the producer and it
  woke one per task, and the rest 0.3 to 0.4 us; START took 5.6 to 5.9 us every time.

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
