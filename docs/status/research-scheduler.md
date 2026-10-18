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
- Done:
  - The scaffolding (`03b0869`): `research/` with the queues (Chase-Lev with the PPoPP 2013
    orders, Vyukov's bounded MPMC), the futex layer (Linux futex, macOS
    `os_sync_wait_on_address`, FreeBSD `_umtx_op`), Tokio-style idle bookkeeping, the five
    candidates plus Taskflow and moodycamel references, `bound.cc`, `workloads.cc`; built only
    with `THREADPOOL_BUILD_RESEARCH=ON`.
  - The bound on the Mac (`docs/research/scheduler/bound-mac.txt`, light hold `sched-bound`):
    mutex 20 ns, CAS 9 ns, mutex+deque 41 ns, Chase-Lev 23 ns, MPMC ring 20 ns per op;
    cross-core round trip 139 ns; wake of a sleeping thread p50 10 µs, p99 63–77 µs, p99.9
    260–315 µs, the same through condvar, `atomic::wait`, and the raw call; futex ping-pong
    11.4 µs per round trip.
  - The survey and the designs drafted in `docs/research/scheduler.md` (sections 1, 3, 4).
- Doing: smoke-testing every workload on every pool (correctness, no timing claims), then a
  TSan build of the harness with Homebrew LLVM.
- Next:
  1. TSan over `tiny` and `tail` for each candidate.
  2. Mac measurements: `tiny` (1 / 1,000 / 10,000 groups; external and chained), `tail`
     (1 hot, 1,000 busy, 10,000 mostly idle; with merges), `makespan`, `idle`, `memory`;
     strict holds of at most 10 minutes each, interleaved, 3 to 5 trials.
  3. The gate VM (after `si-m0` ends at 01:01): sync, build, the bound and the workloads.
  4. Fill sections 2, 5, 6 of the note; tick K1 to K5; push; `gh workflow run ci.yml` (K6).
- Blocked on: nothing (light hold `si-m0` until 01:01 limits builds to nice 19 and 4 jobs, and
  keeps me off the gate VM until then).
- Findings folded in: none.
- Last verified: `research_bound` ran on the Mac 2026-10-18 00:40 (output committed); the
  library's own tests not rerun (no library code changed).
