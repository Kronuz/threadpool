# research-scheduler

- Design: none; this is a research note, `docs/research/scheduler.md`, for the performance
  engineer or designer to build from. Reviews: none yet.
- Branch / worktree: `research/scheduler` at `~/code/Kronuz/threadpool-wt/research`, from
  `origin/main` at `628679a`.
- State: implementing
- Done criteria (set by the lead; contributors tick, never edit):
  - [ ] K1 The bound: per-task overhead and wake-latency floors on this hardware (enqueue
    and dequeue in ns, an idle worker's wake in µs, futex or ulock against a condition
    variable), on the Mac (x86_64) and the strata gate VM.
  - [ ] K2 A survey, reading source freely: work-stealing deques (Chase-Lev and its C11
    memory ordering), Tokio's scheduler (LIFO slot, inject queue, budget), Go's runtime
    (P/M/G, netpoll), oneTBB arenas, Taskflow, folly's CPUThreadPoolExecutor and LifoSem,
    Rayon, Seastar's shared-nothing per-core design, io_uring as a completion source, and
    fair queuing across groups (DRR, stride scheduling).
  - [ ] K3 At least three designs prototyped under `research/` (off by default) and
    microbenchmarked against today's pool and the fastest open implementation buildable
    here: per-core deques with stealing; per-group queues with a ready-list and DRR across
    groups; priority classes; and one that breaks an assumption (tasks pinned to the core
    that owns the index's arena).
  - [ ] K4 The workloads measured with one hot group, 1,000 busy groups, and 10,000 groups
    mostly idle: tiny-task throughput; p50, p99, p99.9 of a query-class task while flushes
    and merges saturate the pool; one index's flush makespan alone against beside 1,000
    others; CPU when idle (about zero); memory per group.
  - [ ] K5 `docs/research/scheduler.md`: the bound, the survey with sources, each idea with
    its prototype result, and the top 3 recommendations, each with prototype numbers, an
    additive API sketch for threadpool (Detent keeps compiling), and what strata's executor
    and reactor's offload would change to use it.
  - [ ] K6 The branch pushed, CI green on all jobs.
- Done: nothing committed yet.
- Doing: the survey (sources fetched: Tokio's blog and `worker.rs`, Rayon's sleep README,
  folly's `LifoSem.h` and `CPUThreadPoolExecutor.h`, Taskflow's `executor.hpp`, oneTBB's
  `arena.h`, Seastar's `scheduling.hh`, the io_uring networking wiki, Apple's
  `os_sync_wait_on_address`); the prototypes under `research/`, written without compiling
  while the strict hold `ig-e5` (gate VM A/B, until 00:36) lasts.
- Next:
  1. `research/bound.cc`: the floors (mutex, CAS, cross-core line transfer, condvar wake,
     `std::atomic::wait` wake, raw futex/ulock wake, spin handoff), Mac and VM.
  2. `research/pools.hh`: the candidates behind one interface, and `research/workloads.cc`.
  3. Measure under holds (light for single-thread, strict and short for multi-core).
  4. Write the note, tick K1 to K5, push, CI (K6).
- Blocked on: the strict hold `ig-e5` for compiling and measuring (edit, read, commit, push
  meanwhile).
- Findings folded in: none.
- Last verified: nothing built (strict hold `ig-e5`, 2026-10-18 00:17).
