> Status: CURRENT (2026-10-29) supersedes `scheduler.md` (the `feature/scheduler` ledger); Phase 7c step L2.

# phase7c-l2

- Design: Xapiand `docs/design/phase7c.md` D4, D5, D7 and section 5's L2; `docs/reviews/phase7c-design.md` C8
  (F15) and the offload review's V2 and V5 (`docs/reviews/engine-offload-architect.md`); the research note
  `docs/research/scheduler.md` R1 to R3 on `research/scheduler`. Reviews: none yet.
- Branch / worktree: `phase7c/l2-scheduler` at `~/code/Kronuz/threadpool-wt/l2-scheduler`, from
  `feature/scheduler` at `threadpool:5caedce`, which already sits on main `threadpool:394aaea` (the rebase
  was a no-op, so no history was rewritten; `feature/scheduler` is untouched).
- State: in review (code review for concurrency is the lead's to launch; not merged; CI to be watched by the lead).
- Done criteria (the task's list; ticked with evidence):
  - [x] L2-1. K1 to K3 reruns on the final code. Data in `docs/status/phase7c-l2/`, Mac x86_64, 16 workers,
    strict hold, 5 trials, medians (range):
    - K1 tiny external tasks, wall ns per task, today's pool / prototype `groups` / WorkPool: 1 group 1,822 / 475 /
      269 (250-277); 1,000 groups 5,975 / 133 / 138 (134-147); 10,000 groups 5,989 / 160 / 144 (134-154);
      1 us tasks 7,581 / 81 / 84; 4 producers 7,608 / 76 / 69 (40-102). Worker-local chains (no today's pool):
      prototype 4 (outlier; 10 to 17 in earlier runs) / WorkPool 13 (11-16). CPU per task, 1 group:
      24,895 / 5,116 / 2,100 ns. Idle CPU is unchanged from `scheduler.md` (R1 untouched; `test/work_pool.cc` case 6).
    - Attribution of the aging change: interleaved A/B of `threadpool:5caedce` against this branch, 7 rounds each,
      WorkPool only (`ab-*.txt`): chains 10.8 (10.1-11.7) against 10.6 (10.1-11.4); 1,000 groups 130 (99-152) against
      137 (106-151); 4 producers 58 (48-81) against 70 (49-89), ranges overlap and the workload is producer-bound;
      1 group 257 (242-275) against 253 (189-270). No difference outside noise; the 4-producer row is the one to
      watch at the gate.
    - K2 one group alone 5.1 ms for 64 x 1 ms on 16 workers (ideal 4.0; prototype 4.4, reserve 0: 4.5); beside 1,000
      busy groups the probe's share is 1.01 (1.01-1.02), reserve 0 1.02 (0.95-1.03), prototype 0.82, today's pool
      15.7; Jain 1.000.
    - K3 query wait beside 4 merges on a saturated pool, WorkPool against prototype `prio-r1-m15` (the note's R3
      prototype): 1,000 busy groups p50 8.0 us / p99 176 us / p99.9 312 us against 8.1 / 275 / 570; 1 hot group p99
      162 us against 668; 10,000 groups p99 179 us against 266. Background throughput 16.6k tasks/s against 16.5k.
  - [x] L2-2. Merge aging (D4, C8.2), `test/work_scheduling.cc` cases 1 to 4: a merge entry older than `merge_age`
    (100 ms default) goes ahead of flush under the same caps; with aging off a merge starves under a flood (the
    control); 40 starved merges are all promoted in 30 ms (`merges_aged` 40), so promotion is per entry, not per
    interval; `merge_cap` and the reserve hold (at most 3 background items at once on 4 threads, at most 2 merges);
    50 queries beside the flood and the aged merges waited 0.4 ms at worst; `merge_waiting()`, `merge_oldest()`,
    `merges_aged()` give T5's backlog.
  - [x] L2-3. The other C8 items. C8.1: parse-ahead gets its own group; case 5, a refresh waits 0 to 1 ms beside a
    200 ms parse-ahead backlog in its own group and 92 to 95 ms behind it in one FIFO. C8.3: the reserve stays
    at 1 because 7c posts refresh, open and blob read at `query` (D4's table); case 3 shows a query runs at once
    beside a saturated pool while aging promotes merges; a host that posts nothing at `query` sets `reserve = 0`.
    C8.4: V2 and V5, below.
  - [x] L2-4. V2 (a worker waits on work queued only in its own group), case 6: every class pairing on 1 and 2
    workers with width 1, and ten nested waits on one worker, complete. It found a deadlock in `threadpool:5caedce`:
    the nested wait, and two items of one group waiting together, waited on each other forever; fixed
    (`GroupState::suspended`, case 6 nested and 6b).
  - [x] L2-5. V5 (a reactor calls a waiting entry), case 7: `Options::block_guard` is called on entry to
    `WorkGroup::wait()` and `help_until()` from a thread that is not a worker, before it can block; a worker's
    wait does not call it. Xapiand sets it to `engine::check_may_block`. See Decisions.
  - [x] L2-6. A mutant per claim (`.scratch/mutants.py`, all die, assertion in brackets): M1 no aging
    (case 1 in_time); M2 aged merge ignores `merge_cap` (case 2 merge peak); M3 aged merge ignores the background
    slots (case 2 total peak); M4 one promotion per `merge_age` (case 2 took < 200 ms); M5 no guard in `wait`
    (case 7 hits == 1); M6 no guard in `help_until` (case 7 hits == 2); M7 `wait` counts only itself (nested case
    hangs; with the nested case removed, 6b hangs); M8 a turn drains the lane (case 1 flood never yields);
    M9 empty backlog report (case 1 waiting == 1). Revert checks: the new cases fail without the code they test,
    by the mutants above; the pre-existing cases pass on both.
  - [x] L2-7. Sanitizers, separate Homebrew LLVM 23 builds, clean on `threadpool:6573176` (all 6 CTest targets):
    TSan (`-O1 -g -fsanitize=thread`), once and 4 repeats of the work_ tests; ASan+UBSan (`-fno-sanitize-recover`),
    once and 3 repeats. Baseline `threadpool:5caedce` was clean under both before the change.
  - [x] L2-8. Consumers built with `FETCHCONTENT_SOURCE_DIR_THREADPOOL` pointing at this worktree (existing files
    are byte-identical to main): scheduler `2cbea95` 2/2 tests; logger `90094b5` 3/3; http-log `2f066ee` 2/2;
    prism `a05e253` builds (no tests); strata `8fac1bc` 57/57; Xapiand `4ef308995` configures with it and its
    `thread.cc` and a probe TU that includes `work_pool.hh` compile (`-fsyntax-only`, Xapiand's own flags and trace
    header); a full Xapiand build was not run (it includes only `threadpool.hh` and `thread.cc`, unchanged).
    Nothing broke. reactor does not link threadpool.
  - [x] L2-9. `cpp-libraries/tools/preflight.sh`: mac-hardened ok, mac-asan-ubsan ok, linux ok (`_GLIBCXX_ASSERTIONS`), freebsd ok; the VMs ran at `--parallel 2` under a light hold (a copy of the script without its host check, since the script waits on the strict hold the VM rule asks for).
  - [ ] L2-10. CI green on the pushed branch (the lead watches; see Next).
- Done: `threadpool:6573176` merge aging, `block_guard`, the wait fix and `test/work_scheduling.cc`; the docs
  commit after it (README untouched; API, ARCHITECTURE, AGENTS, `docs/mechanisms/work-pool.md`, this ledger and
  the K data).
- Doing: nothing.
- Next:
  1. The lead launches the concurrency code review (the new probes: `head_stamp()`'s racy peek, `suspended`
     on every exit of `wait()`, aging under `held` slots) and watches CI.
  2. K6 (FreeBSD kernel time, `bench/kernel_time.cc`) was not part of L2 and is still open from `scheduler.md`.
  3. Xapiand sets `block_guard` to `engine::check_may_block` and posts chores with `merge_age` as D4 states.
- Blocked on: nothing.
- Decisions to confirm (architect), each a reading of the task:
  1. `block_guard` is a function pointer in `Options`, called with a site name, so the library has no reactor
     knowledge. It fires for any non-worker thread on entry, even when `wait()` would return at once, so a
     violation is found deterministically; hosts that legitimately wait from outside (shutdown, tests) must have
     the guard allow it.
  2. Aging is per entry and uses a stamp in the merge ring. A pool-level "no merge for T" rule was rejected: it
     promotes one entry per interval (mutant M4 shows the test catching it).
  3. Flushes do not age against queries (the research note's open item stays open; 7c posts few queries per
     flush stream).
  4. `reserve` default stays 1 (C8.3), because 7c posts at `query`.
  5. From `scheduler.md`, still open: extra tickets capped pool-wide; the destructor cancels; `width` per class.
- Last verified: 2026-10-29, Mac x86_64, Apple clang (plain, `-O2 -g`, asserts on) and Homebrew LLVM 23 (TSan,
  ASan+UBSan): all 6 CTest targets pass.

## Log
- 2026-10-29: `threadpool:5caedce` found already on main `394aaea`; worktree `phase7c/l2-scheduler` cut from it.
  Baseline TSan and ASan+UBSan clean. Aging, guard, wait fix and tests in `threadpool:6573176`; first run of
  the nested V2 case hung, which is how the wait deadlock was found. Mutants, sanitizers, K1 to K3 reruns, A/B,
  consumer builds and preflight run the same day.
