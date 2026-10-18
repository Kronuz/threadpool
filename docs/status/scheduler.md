# scheduler

- Design: the research note `docs/research/scheduler.md` on `research/scheduler`
  (`~/code/Kronuz/threadpool-wt/research`), section 6: R1, R2, R3, the additive API sketch,
  and the corrections at its top (read at `150152c`). Reviews: none yet.
- Branch / worktree: `feature/scheduler` at `~/code/Kronuz/threadpool-wt/scheduler`, from
  `origin/main` at `628679a`.
- State: implementing (R1 and R2 in; R3 next)
- Done criteria (set by the lead; contributors tick, never edit):
  - [ ] K1. R1 is in. Idle workers use no measurable CPU. Tiny-task throughput is at least
    today's pool and at least the research prototype's, within noise (interleaved A/B, five
    or more rounds, minutes long).
    Branch evidence (Mac, 16 workers, interleaved trials, raw files in
    `docs/status/scheduler/`; the gate rows are the lead's, on main):
    idle 0.0009 to 0.0015% of a core and 0.3 context switches a second, the prototype's
    0.0010 to 0.0015% and today's 0.47 to 0.63% with 287 to 452 (`idle.txt`, 5 trials).
    Tiny tasks, wall ns per task, medians of 5 (today / prototype `groups` / WorkPool):
    1 group 2,060 / 589 / 227; 1,000 groups 9,065 / 116 / 115; 10,000 groups 6,739 / 147 /
    134; 1 µs tasks 8,136 / 103 / 86; 4 producers 8,070 / 66 / 76 (ranges 47 to 106 against
    70 to 79; a rerun of 7, 57 / 82 with 47 to 164 against 45 to 97, and a sampled run
    88 / 85 and 264 / 110 with WorkPool at 578 to 697 ns of CPU per task against 938 to
    1,222: noise-dominated, producer-bound); worker-local chains 10.3 / 11.4 with a bench
    trampoline, then 16.9 / 14.1 (7 trials, ranges overlapping) once the adapter calls the
    task directly as the prototypes do. To finish: the full tiny matrix once more on the
    final branch, minutes long, before ticking.
  - [ ] K2. R2 is in. One group alone gets the whole pool. Beside 1,000 busy groups, each
    group's share is fair within the note's bound.
    Branch evidence: `test/work_group.cc` case 1 (64 items on 4 workers ran on all 4);
    makespan alone, medians of 5, 5.1 ms against the prototype's 5.0 (ideal 4.0); beside
    1,000 busy groups the probe's share is 0.81 to 1.02 (median 0.96) against the note's
    0.8 to 1.1, Jain's index 0.988 to 0.994 (`makespan-g1000.txt`); beside 100, 0.81 to
    1.13 (median 1.03; the prototype 0.82 to 3.38) (`makespan-g100.txt`).
  - [ ] K3. R3 is in. A query-class piece's p99 on a saturated pool matches the note's
    prototype result.
  - [ ] K4. Strictly additive. Every existing API and test is unchanged, and reactor and
    every Kronuz library that links threadpool still builds and passes (check the
    cpp-libraries index for consumers). Detent links these libraries: nothing it calls
    changes behavior.
  - [ ] K5. TSan and ASan+UBSan clean. CI green on all jobs.
    R1: CI run 39134047236 green on all 8 jobs. R2: TSan (3 runs) and ASan+UBSan (2) clean
    locally, Homebrew LLVM.
  - [ ] K6. FreeBSD: no kernel-time regression against today's pool. A condvar handoff
    cost about 3x kernel time on FreeBSD 14.4 in strata, so measure CPU and sys time there.
  - [ ] K7. The README and notes document the new API in the repo's style. Comments stay
    short.
- Done:
  - R1 (`7bd97c1`): `work_pool.hh`, `WorkPool` and `WorkItem`; the platform futex
    (`futex(2)`, `os_sync_wait_on_address` or `__ulock`, `_umtx_op`), untimed; Tokio's idle
    protocol; `help_until`; `test/work_pool.cc`; `bench/research_ab.sh` and
    `bench/work_pool_research.hh` (the research harness with WorkPool added).
  - R2 (this commit): `WorkGroup`, per-group lanes, the chained ready ring, deficit round
    robin over cycle-counter time, tickets, `cancel()` returning the queue, narrow helping,
    `sleep_until`; `test/work_group.cc` (11 cases).
- Doing: R3, the classes, the reserve, the merge cap.
- Next:
  1. Push R2 once R1's CI is green (it is), dispatch CI.
  2. R3; K3 against the note's 5.2 tables.
  3. K4: build scheduler, logger, http-log, prism, strata, and Xapiand against the branch.
  4. K6 on strata's FreeBSD VM (`~/code/Kronuz/strata/tools/vm/freebsd.sh`).
  5. K7: README, docs/API.md, ARCHITECTURE.md, AGENTS.md.
- Blocked on: nothing.
- Decisions to confirm (architect), each a reading of the note:
  1. Extra tickets are capped pool-wide at one per worker and retired at a turn's end when
     other groups wait on the ring. The note's invariant (correction 6) drops a ticket only
     when the group empties, which lets a group that fanned out while alone keep every
     worker after 1,000 others arrive; the cap also bounds the ring at (open groups +
     workers) entries instead of (threads x groups).
  2. The ring grows: rings are chained, a larger one becomes the tail when a group opens,
     the old one drains first, and retired rings stay allocated until the pool ends (at
     most as much again). A post never blocks or allocates.
  3. Correction 4's "its own group": a worker in `WorkGroup::wait()` serves the group it
     waits on (inline when it runs one of that group's items; otherwise as a helper within
     the group's width), never other groups' flush or merge items. `help_until(pred)` runs
     only the caller's own group's items. Without this a one-worker pool deadlocks on a
     wait across groups (`test/work_group.cc` case 7).
  4. A group's state outlives its `WorkGroup` until its last ticket leaves the ring, so
     `wait()` and the destructor never wait a ring round for a stale ticket.
  5. The destructor cancels: queued boxed items (`enqueue`) are freed unrun; caller-owned
     items are unlinked and left to the owner, who calls `cancel()` first to get them back.
- Findings folded in: none.
- Consumers (for K4): `scheduler` (and through it `logger`, `http-log`, `prism`, Xapiand),
  `strata`; reactor does not link threadpool (it runs its own `asio::thread_pool`).
- Last verified: 2026-10-18 07:30, Mac: all five CTest targets under Apple clang, TSan,
  and ASan+UBSan.
