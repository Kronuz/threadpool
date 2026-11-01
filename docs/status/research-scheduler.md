# research-scheduler

- Design: none; this is a research note, `docs/research/scheduler.md`, for the performance
  engineer or designer to build from. Reviews: none yet.
- Branch: `research/scheduler`, from
  `origin/main` at `628679a`.
- State: in review (the note is delivered; K1 to K6 met; the lead decides what lands)
- Done criteria (set by the lead; contributors tick, never edit):
  - [x] K1 The bound: per-task overhead and wake-latency floors on this hardware (enqueue
    and dequeue in ns, an idle worker's wake in µs, futex or ulock against a condition
    variable), on the Mac (x86_64) and the strata gate VM: note section 2 from
    `bound-mac.txt` and `bound-vm.txt` (Mac: queue ops 12 to 85 ns, a wake 10 µs p50 and
    63 to 77 µs p99 for condvar, atomic wait, and raw futex alike; VM: 31 µs clock reads and
    0.4 ms wakes on hpet, so the VM is not a latency floor), plus the FreeBSD VM's wake and
    timed-sleep costs in 5.6 (`handoff-*.txt`).
  - [x] K2 A survey, reading source freely: work-stealing deques (Chase-Lev and its C11
    memory ordering), Tokio's scheduler (LIFO slot, inject queue, budget), Go's runtime
    (P/M/G, netpoll), oneTBB arenas, Taskflow, folly's CPUThreadPoolExecutor and LifoSem,
    Rayon, Seastar's shared-nothing per-core design, io_uring as a completion source, and
    fair queuing across groups (DRR, stride scheduling): note section 3, with the constants
    and quotes from each source (Tokio `queue.rs`, `idle.rs`, `stats.rs`, `worker.rs`; Go
    `proc.go`; Rayon `sleep/mod.rs` and README; folly `LifoSem.h`, `ThrottledLifoSem.h`,
    `CPUThreadPoolExecutor.h`; Taskflow `executor.hpp`, `wsq.hpp`, `atomic_notifier.hpp`;
    oneTBB `arena.h`, `arena_slot.h`; Seastar `scheduling.hh`, `reactor.cc`; Eigen
    `EventCount.h`; Lê et al. PPoPP 2013; the io_uring wiki; Apple's `os_sync_wait_on_address`).
  - [x] K3 At least three designs prototyped under `research/` (off by default) and
    microbenchmarked against today's pool and the fastest open implementation buildable
    here: per-core deques with stealing; per-group queues with a ready-list and DRR across
    groups; priority classes; and one that breaks an assumption (tasks pinned to the core
    that owns the index's arena): `research/pools.hh` (`steal`, `groups`, `prio`, `home`,
    plus today's `baseline` and `raw`, Taskflow 3.10, and moodycamel 1.0.4), note sections
    4 and 5; TSan (Homebrew LLVM 23) clean on every candidate over every workload.
  - [x] K4 The workloads measured with one hot group, 1,000 busy groups, and 10,000 groups
    mostly idle: tiny-task throughput; p50, p99, p99.9 of a query-class task while flushes
    and merges saturate the pool; one index's flush makespan alone against beside 1,000
    others; CPU when idle (about zero); memory per group: note sections 5.1 to 5.5 from
    `*-mac.txt` (medians of three interleaved trials under strict holds) and `*-vm.txt`
    (three trials for tiny, two for tail and makespan, light and strict holds with remote
    timeouts). Query p50 at 1,000 busy groups: today 70 ms (Mac) and 220 ms (VM) against
    65 to 71 µs (Mac) and 26 to 34 µs (VM) for the group and deque pools; idle CPU today
    0.35 to 0.47% (Mac) and 4.8% (VM) of a core against 0.001 to 0.011%; a bursting index's
    share beside 1,000: today 7 to 16×, groups and prio 0.8 to 0.9×, deques 700×.
  - [x] K5 `docs/research/scheduler.md`: the bound, the survey with sources, each idea with
    its prototype result, and the top 3 recommendations, each with prototype numbers, an
    additive API sketch for threadpool (Detent keeps compiling), and what strata's executor
    and reactor's offload would change to use it: sections 1 to 7, with the corrections to
    the sketch at the top of section 6 (eight, from the FreeBSD probe and the gate-VM hang).
  - [x] K6 The branch pushed, CI green on all jobs: run 39137948331 (`workflow_dispatch`;
    CI runs only on main, PRs, and dispatch) on the final code commit `d112fde`, success on
    all 8 jobs (freebsd-x86_64, linux-asan-ubsan, linux-arm64-gcc, linux-x86_64-gcc,
    linux-x86_64-clang, linux-tsan, macos-arm64, macos-x86_64); the earlier run 39111176813
    at `3dd1261` was green too. The research tree is off by default, so CI proves the library
    unchanged; the research code's own checks are the Mac and VM runs and TSan in the note.
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
  - Mac matrix (`0b05da8`): `tiny` (1 / 1,000 / 10,000 groups, 1 and 4 producers, empty and
    1 µs tasks, chained), `tail` (1 hot / 1,000 busy / 10,000 with 100 busy, four merges),
    `makespan` (1,000 and 100 peers), `idle`, `memory`; strict holds `sched-mac-tiny` and
    `sched-mac-tail`, three interleaved trials; TSan clean after the harness's stats counter
    became atomic (the only reports).
  - Harness faults found by the first runs and fixed: LIFO owner pop (fairness 0.31) → FIFO;
    inject check every 61 tasks (61 ms at 1 ms tasks) → every 200 µs by the cycle counter;
    the priority pool's fan-out counting the reserved worker as idle; the spin phase
    registering the spinner as a sleeper (a wake syscall per post).
  - The note's sections 1 to 6 with the Mac numbers; `research/summarize.py` builds the
    median tables from the raw files.
  - The bound on the gate VM (`fa9be5a`, `bound-vm.txt`): queue operations within 1.5× of
    the Mac's; a clock read 31 µs and a futex wake 0.4 ms median, 2 ms p99, because HVF
    exposes no stable TSC and the guest runs on `hpet` (note section 2).
  - The Mac `tiny` rerun with the per-task stamps off (`sched-mac-tiny2`; the stamps were
    94 ns of the 46 to 140 ns per task, and 62 µs on the VM): section 5.1 refreshed.
  - The coordinator's FreeBSD probe (`research/handoff.cc`, `handoff-{mac,freebsd,vm}.txt`,
    light holds): per wake of a sleeping worker, macOS 3 to 4 µs of kernel time for every
    primitive; FreeBSD 16 to 20 µs for an untimed condvar, 37 µs for `_umtx_op`, 98.5 µs for
    the 100 ms timed condvar wait today's worker uses, and `std::atomic::wait` spinning 268 µs
    of user time; section 5.6, and the corrections at the top of section 6 (untimed futex per
    worker, `sleep_until` on one timed futex wait, cancel returns items, narrow `help_until`,
    cycle-counter time, the ticket invariant, defaults).
  - The coordinator's hang (two lifetime races, `research/pools.hh`): the shutdown lost its
    wakeup (a worker registering as a sleeper after `stop()`'s unpark slept forever), and a
    group freed while a worker still held its ticket (the owner sees the last task's latch
    before the ticket drops; the worker spun on the freed spinlock, the 100% of one core).
    Found with the harness's new per-pool watchdog (`timeout=`, `TIMEOUT pool=... phase=...`
    with idle counters and per-worker states); both fixed; churn of 40 create-run-destroy
    cycles per pool and 8 saturated `tail` trials pass on the VM. Correction 8 in section 6.
- Host note (2026-10-18): my first driver's VM half outlived its killed `ssh` and ran unheld
  for 5h36m (the hang above, 100% of one core) beside other agents' gate-VM numbers from
  01:28 to 07:04, when the coordinator killed it. Every VM command now runs under `timeout`
  bounded by its hold, and every VM run holds at least light.
  - The gate VM halves with the fixed harness (`sched-vm-a`, `sched-vm-a2` light; `sched-vm-b`
    strict 4 min; `sched-vm-tiny2` light after the quanta moved to the cycle counter), and
    the Mac `tiny` rerun on the committed code (`sched-mac-tiny3`): sections 5.1 and 5.5.
- Doing: nothing; stopped on the coordinator's word once the final CI run is recorded.
- Next (for the lead and the developer):
  1. The developer builds R1 to R3 from section 6, corrections first (the untimed futex per
     worker, `sleep_until` on one timed wait, cancel returning items, narrow `help_until`,
     cycle-counter quanta, the ticket invariant, the defaults, and the two lifetime races
     with a churn test).
  2. Open for the performance engineer (6, R2): the 0.8 to 0.9 share of a bursting group,
     the one-group spinlock case, and locality with real index data.
  3. For the lead: the gate VM's hpet clocksource (31 µs per `clock_gettime`, 0.4 ms wakes)
     taxes every per-task timestamp in strata's VM harnesses; latency floors need bare metal.
- Blocked on: nothing. For the lead: the gate VM's `hpet` clocksource (31 µs per
  `clock_gettime`, 0.4 ms wakes) taxes every per-task timestamp in strata's VM harnesses and
  makes the VM unfit for latency floors; worth `clocksource=` work or bare metal for those rows.
- Findings folded in: none.
- Last verified: 2026-10-18 01:35, Mac: `research_bound`, the matrix, and TSan on every
  candidate; the library's own tests not rerun (no library code changed; CI will).
