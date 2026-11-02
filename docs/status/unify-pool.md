> Status: CURRENT (2026-11-02) workstream: one pool for the Kronuz libraries (developer).

# unify-pool

- Task: `WorkPool` is the only pool; the scheduler library moves onto it, then `ThreadPool` and its family are deleted here.
- Branch: `phase7c/unify-pool`, from main `threadpool:65bc0bc`. Scheduler branch: `phase7c/unify-pool` in the scheduler repo.
- State: step 1 (additive `WorkPool` features) and step 2 (scheduler) landed on main (`threadpool:0983f2d`, `scheduler:d94f287`); step 3 (delete the `ThreadPool` family) is on branch `phase7c/drop-threadpool`, see below.

## Step 1: what the scheduler needed

| Need | `WorkPool` today | Change |
|---|---|---|
| Per-pool thread names | `Options::name_format` | none |
| All workers run tasks | `Options::reserve = 0` | none |
| Thread policy | `ThreadPolicyType` is ignored at run time by `setup_thread`/`run_thread`; `WorkPoolWorker` is `regular` | none (the scheduler's `Thread` keeps its policy parameter) |
| Delayed or timed post | the scheduler's wheel times; the pool only runs | none |
| `size()` and `running_size()` for Xapiand's metrics | not available per group | `WorkGroup::pending()` and `running()` (new) |
| Bounded `join()` | `on_idle` + a condition variable in the scheduler | none |

## Done

- `WorkGroup::pending()` / `running()` with `test/work_counts.cc`; mutants in `test/mutants.sh`:
  "pending() counts only the query lane" and "running() reports the queued count" both fail the test
  (`work_counts.cc:42`, `:41`).

## Step 3: delete the `ThreadPool` family (branch `phase7c/drop-threadpool`)

- Deleted: `threadpool.hh` (`ThreadPool`, `ThreadPoolThread`, `PackagedTask`, `TaskWrapper`, `TaskQueue`), `task_group.hh`,
  `pool_queue.h`, `small_task.h`; tests `test.cc`, `task_group*.cc`, `pool_diff.cc`, `pool_stress.cc`, `small_task.cc`,
  `threadpool_old.hh`; `bench/enqueue_bench.cc`, the `research/` harness and its adapter (`bench/research_ab.sh`,
  `work_pool_research.hh`); their mutants. Kept: `concurrent_queue.h`, `blocking_concurrent_queue.h` (Xapiand's WAL writer
  uses `BlockingConcurrentQueue`), `thread.hh`/`thread.cc`, `threadpool_trace.h` (`L_EXC` is still `work_pool.hh`'s).
- `ThreadPolicyType` stays, trimmed to `regular` and `logging`: the scheduler's and the logger's template parameter. Removing the
  parameter itself changes `Scheduler<>`, `ScheduledTask<>`, `Debouncer` and the logger's types: a follow-up the owner has not ruled.
- `bench/kernel_time.cc` and `examples/demo.cc` run on `WorkPool`. README, `ARCHITECTURE.md`, `AGENTS.md`, `docs/API.md` and the
  CHANGELOG describe `WorkPool` alone; `docs/perf/enqueue.md`, `docs/perf/trickle/p99.md`, `docs/research/scheduler.md` are HISTORY.
- Proof: ctest 6/6 (RelWithDebInfo, asserts live); `test/mutants.sh` both counter mutants caught. No library header other than
  `thread.hh`'s enum changed, so the pool's behavior is the landed one.

## Note for the Detent lead: what was removed, and the `WorkPool` equivalents

Detent stays on its pinned threadpool and migrates itself. At `threadpool:bd60941` (the last commit with them) the old names are
still there. Gone after this branch:

| Removed | Use |
|---|---|
| `ThreadPool<T, policy> pool("fmt", n)` | `WorkPool pool({.threads = n, .reserve = 0, .name_format = "fmt"})`; `reserve = 0` lets every worker run any work (the default keeps one free for `query` items) |
| `pool.enqueue(f)`, `post(f, args...)` | `WorkGroup g(pool); g.enqueue(f)` (boxed, one allocation), or `g.post(item)` / `pool.post(item)` with a caller-owned `WorkItem` (none) |
| `async(f, args...)` -> `std::future` | none: capture a `std::promise` in `enqueue`, or post a `WorkItem` that carries the result |
| `package(...)`, `PackagedTask`, `TaskWrapper` | none |
| `TaskGroup<>(pool, max_concurrency)`, `group.enqueue(f or f(stop))`, `wait()`, `cancel()`, stop token | `WorkGroup(pool, width)`, `enqueue(f)` or `enqueue([](const WorkItem& self) { ... self.stop_requested() ... })`, `wait()`, `cancel()` / `cancel_and_wait()`; the destructor cancels and waits as before |
| `end()` (drain) | `~WorkPool` runs what is queued, then joins; `group.wait()` drains one owner |
| `finish()` (stop asap) | `group.cancel_and_wait()` before destroying the pool |
| `join(timeout)` -> `bool` | none on the pool; a group joins with `on_idle(WorkIdle&)` plus a condition variable up to a deadline (the scheduler library's `join`) |
| `clear()` | `group.cancel()` (returns caller-owned items) |
| `size()`, `running_size()` | `group.pending()`, `group.running()` |
| `threadpool_workers()`, `threadpool_size()`, `threadpool_capacity()` | `pool.threads()` |
| `finished()`, `ending()` | none |
| `TaskQueue<std::packaged_task<...>>` | none: a `std::deque` under a mutex, or `BlockingConcurrentQueue` (still in the library) |
| `ThreadPolicyType::wal_writer` and the other tags | `regular` and `logging` remain; the policy is ignored at run time |

Waiting is stricter: `WorkGroup::wait()` from an item of the same group aborts (post to a child group and wait on that; see
`docs/API.md`, "Waiting"), and an event loop never calls it (`Options::block_guard`; join with `on_idle()`).

## Next

Step 4 (repin the consumers) and step 5 (Xapiand) follow in the same wave; the lead lands this branch after CI.

## Blocked on

Nothing.
