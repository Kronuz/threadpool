> Status: SUPERSEDED (2026-10-29) by `phase7c-l2.md`, which finishes this branch (R1 to R3 stand; the K evidence there is the final).

# scheduler

- Design: the research note `docs/research/scheduler.md` on `research/scheduler`
  (`~/code/Kronuz/threadpool-wt/research`), section 6: R1, R2, R3, the additive API sketch,
  and the corrections at its top (read at `150152c`). Reviews: none yet.
- Branch / worktree: `feature/scheduler` at `~/code/Kronuz/threadpool-wt/scheduler`, from
  `origin/main` at `628679a`.
- State: paused (quota), at the commit after `7549809`; rebased onto main `394aaea`.
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
    R1: CI run 39134047236 green on all 8 jobs. R2: run 39135478119 green on all 8. R3: TSan
    (3 runs) and ASan+UBSan (2) clean locally, Homebrew LLVM.
  - [ ] K6. FreeBSD: no kernel-time regression against today's pool. A condvar handoff
    cost about 3x kernel time on FreeBSD 14.4 in strata, so measure CPU and sys time there.
  - [ ] K7. The README and notes document the new API in the repo's style. Comments stay
    short.
- Done:
  - R1 (`7bd97c1`): `work_pool.hh`, `WorkPool` and `WorkItem`; the platform futex
    (`futex(2)`, `os_sync_wait_on_address` or `__ulock`, `_umtx_op`), untimed; Tokio's idle
    protocol; `help_until`; `test/work_pool.cc`; `bench/research_ab.sh` and
    `bench/work_pool_research.hh` (the research harness with WorkPool added).
  - R2 (`75cdf1d`): `WorkGroup`, per-group lanes, the chained ready ring, deficit round
    robin over cycle-counter time, tickets, `cancel()` returning the queue, narrow helping,
    `sleep_until`; `test/work_group.cc` (11 cases).
  - R3 (this commit): `WorkClass` (query, flush, merge), a lane and a ring per class, picked
    in that order; `Options::reserve` (default 1) and `merge_cap` (default threads -
    reserve) held as exact slots; waiting workers also serve other groups' queries;
    `enqueue(f, cls)`; tests 12 to 15; `bench/kernel_time.cc` (K6, off by default).
- Doing: nothing (paused).
- Next, exactly what is left:
  1. Local TSan and ASan+UBSan on `7549809` (the plain suite passed, churn included; a
     strict hold stopped the sanitizer runs), then dispatch CI on the rebased branch.
  2. Rebuild the harness (`bench/research_ab.sh .scratch/ab`) and rerun under strict holds:
     `.scratch/ab/final.sh tiny`, `makespan`, `tail` (scripts in the worktree's .scratch;
     copy them into bench/ if the lead wants them tracked). `pick()` changed since the K1
     to K3 evidence above, so those rows need these reruns. K1 still trails the `groups`
     prototype on chains (12.8 against 10.2 ns at `0b7dd12`) and on 4 producers (97
     against 68 to 73): profile the chain case (`.scratch/ab/prof2.sh`) after the rerun.
  3. K4: `.scratch/consumers.sh` (scheduler, logger, http-log, prism, strata; `-j4`,
     `nice`, gated on holds), then Xapiand. Existing files are byte-identical to main.
  4. K6: sync to the FreeBSD VM (done once, `~/libs/tp-workpool`; resync), build at `-j2`
     under a light hold with `-DTHREADPOOL_BUILD_BENCH=ON`, run `build/kernel_time 5 4` under
     a strict hold, under `timeout`; compare sys and user per op against today's pool.
  5. K7: README, API, ARCHITECTURE, AGENTS written; review once the code settles.
  6. Tick K1 to K7 with the final evidence; State to "in review".
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
     `wait()` and the destructor never wait a ring round for a stale ticket. This is how
     the branch closes correction 8's second race (a group freed while a worker holds its
     ticket): whoever drops the last ticket or helper frees the state, after its final
     unlock. The note's fix instead makes the destructor wait for the tickets; both keep
     the worker off freed memory. Correction 8's first race (a sleeper registering after
     the stop's unpark) is closed since R1: `park()` loads the stop flag again after
     `to_parked()` registers under the idle mutex. `test/work_group.cc` case 16 churns 200
     pools and 800 groups under a watchdog, each group destroyed as its last item reports.
  5. The destructor cancels: queued boxed items (`enqueue`) are freed unrun; caller-owned
     items are unlinked and left to the owner, who calls `cancel()` first to get them back.
  6. The default `reserve = 1` (correction 7) keeps one worker off flush and merge work,
     so one flush-only index gets threads - 1 workers, not K2's "whole pool"; `reserve = 0`
     gives it all (test 1). Which default should hold, or should the reserve engage only
     while queries arrive? (A question for the lead and the owner, not decided here.)
  7. `width` caps each class separately: a group's query, flush, and merge lanes each hold
     up to `width` workers.
- Findings folded in: the note's corrections 1 to 8 (final at `394aaea` on main).
- For the lead: strata's FreeBSD VM runs on `kern.timecounter.hardware: HPET` too
  (`choice: TSC-low(-100) ... HPET(950)`), so its clock reads may be slow like the gate VM's;
  `bench/kernel_time.cc` keeps clock reads and timed sleeps out of its own loops.
- VM rule (the lead's): a run using more than two vCPUs on the gate VM or the FreeBSD VM
  takes a strict hold, a `-j4` build included; light holds are for one- or two-thread runs.
  K6 builds on the FreeBSD VM at `-j2` under a light hold and measures under a strict one.
- Host rule slip (for the lead): at about 07:21 to 07:25 I built and ran the TSan and ASan
  suites while `si-m2c` (strict, the small-indexes M2 rerun, 07:19 to 07:34) held the host;
  my command printed `quiet check`'s 1 without gating on it. That run may need a redo.
  Every compile and test command now exits on 1.
- Consumers (for K4): `scheduler` (and through it `logger`, `http-log`, `prism`, Xapiand),
  `strata`; reactor does not link threadpool (it runs its own `asio::thread_pool`).
- Last verified: 2026-10-18 07:30, Mac: all five CTest targets under Apple clang, TSan,
  and ASan+UBSan.
