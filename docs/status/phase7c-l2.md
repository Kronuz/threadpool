> Status: CURRENT (2026-10-29) supersedes `scheduler.md` (the `feature/scheduler` ledger); Phase 7c step L2.

# phase7c-l2

> Paths under `.scratch/` name local scratch files (probes, logs, mutants) that are not part of the repository.

- Design: Xapiand `docs/design/phase7c.md` D4, D5, D7 and section 5's L2; `docs/reviews/phase7c-design.md` C8
  (F15) and the offload review's V2 and V5 (`docs/reviews/engine-offload-architect.md`); the research note
  `docs/research/scheduler.md` R1 to R3 on `research/scheduler`. Reviews: none yet.
- Branch: `phase7c/l2-scheduler`, from
  `feature/scheduler` at `threadpool:5caedce`, which already sits on main `threadpool:394aaea` (the rebase
  was a no-op, so no history was rewritten; `feature/scheduler` is untouched).
- State: the architect's ruling on B2 and N1 (AL1, AL2, AR2) is implemented; B1, B3 and B4 closed in the re-check; re-check 2's
  B5, B6 and the two test gaps (I4b liveness, the AL2 loops) are fixed in `threadpool:e28a89c` (L2-14) and need the
  rechecker; CI and the K reruns' read are the lead's. Not merged.
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
    (`GroupState::suspended`, case 6 nested and 6b). Superseded by L2-12: an own-group wait now aborts, and V2 is
    a worker waiting on a child group whose work is queued only there (case 6 in the same file).
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
  - [x] L2-11. Review round (`docs/reviews/phase7c-l2-code.md`), each with a mutant that dies (`.scratch/mutants.py`):
    - B1 width-capped cross-group wait: `helpable` counts only turns being served (`tickets - listed + helpers < width`);
      a ring turn that finds a helper in its place gives its ticket up, so the width holds (test 6d at 1, 2, 4 threads x
      width 0 to 3, 6e peak 1 under 4 helpers). F1 (old rule) hangs 6d; F2 (no yield) fails 6e. Probes `width_xwait` at
      threads 1, 2, 4 and widths 0 to 3, and `stress_asan chain 2 4` and `chain 4 4`, pass.
    - (B2 and N1 below are superseded by L2-12; B1, B3, B4 stand.)
    - B2 stack depth: `max_inline_depth` 64 per stack. Past it a waiter starts a rescue thread (one per 64 levels, a fresh
      stack) that serves the lanes the waiter could, and sleeps; it serves no other group's queries; `rescuers` keeps the
      group state alive and `~WorkPool` waits for the threads. Test 6f: 20,000 self-waiting items and a 300-group chain on
      one worker. F3 (no rescue) hangs, F3b (no bound) overflows the stack. `stress_plain deep 1 5 100000` and
      `stress_asan deep 1 5 20000` pass.
    - B3 `work_scheduling.cc` case 5: counts and order only, on one worker (`own_between <= 2*threads+2`; one FIFO sees at
      least `200 - threads` parse items first); a mutant where a turn drains its lane fails it alone.
    - B4 `work_group.cc` case 4: sampled when the light group has finished 1,500 items, ratio in [1.5, 3.0] (measured
      2.00); a mutant ignoring the weight fails it.
    - N1: fixed in part. A wait that finds the group quiet bumps an epoch that ends the other waiters' waits (test 6c, F4
      hangs), so a sibling that left `wait()` no longer holds the rest. The same hunt found a third hang the probes hit
      at 4 workers (`stress chain 4`): an item waiting on another group stood below a same-group query served inline on
      its stack, which then waited for the item below. A running item inside any `wait()` now counts as suspended in its
      own group (test 6c2, F5 hangs). Not fixed, and documented in API.md: a sibling served inline on a waiter's stack
      that blocks on the waiter's progress hangs (`stress postwait` at 4 workers; it needs the waiter to inline the
      sibling, which no rule can tell apart from the cases that need it, since 1 worker has nobody else). Test 6c keeps to
      rounds where both items really overlap.
    - N2 (guard entry sites), N3 (waiting workers beyond the caps) are written in API.md and `work-pool.md`.
  - [x] L2-12. The architect's ruling on B2 and N1 (`docs/reviews/phase7c-l2-code.md`, "Architect ruling"), implemented in
    `threadpool:7a1a5c5` and `threadpool:19a17d0`; tests in the new `test/work_wait.cc` (CTest `work_wait`), mutants in
    `.scratch/mutl2.py` (each patches a copy of the header, builds, expects failure):
    - AL1. `wait()` serves only the group it waits on, inline, as a helper within `width`; no query, no other group, no
      thread. `tls_here.top` is a chain of frames (group, item, parent); `wait()`, `cancel_and_wait()` and `~WorkGroup`
      on a group in the chain abort after naming the site, in every build. Deleted: rescue threads (`spill`/`rescue`,
      `_rescuers`), `max_inline_depth`, `Here::depth`, `GroupState::suspended/quiet/rescuers/rescue_busy`, the `own`
      branch of `helpable`, `help_own`, `help_query`, and `help_until` (no consumer; tests that used it now poll or use
      a child group). `stop_requested()` and `sleep_until` look up the innermost item of their own group on the stack.
    - I1 help scope: the Q -> G -> H inversion at 1, 2 and 4 workers with a recorder (no foreign item inside a wait)
      and a thread-local a query must not see (AR2). Mutant: a wait that serves a query again dies, by the I2 abort
      (the query waits on G, which sits below it on the stack), and also in the random graphs.
    - I2 no wait on the stack: death tests (forked child, SIGABRT and the site on stderr) for a self-wait,
      `~WorkGroup` and `cancel_and_wait` from the group's own item, and G -> H -> G on one worker; a child wait is the
      control. Mutants: top frame only dies (G -> H -> G hangs, SIGALRM); self check dropped dies (self-wait hangs).
    - I3 helpers: former 6d and 6e (workers waiting on a width-capped group at 1, 2, 4 threads, widths 0 to 3; peak 1
      under 4 helpers). Mutants F1 (old admission rule) hangs; F2 (no ticket yield) fails `peak == 1`; both also die in
      the random graphs (F1: hang, F2: `peak_over`).
    - I4 wake: three workers parked on a width-1 group whose width is held by a 50 ms item with three queued behind
      it; all finish. Mutant: no wake at the end of a turn hangs. Honest gap: the half of the wake that fires when a
      lane becomes helpable (not idle) has no liveness effect, since the thread ending the turn requeues and serves
      the ticket itself, so its mutant (`I4b`) survives; it only lets a parked waiter share the work sooner.
    - I5 resources: 20,000 items of G, each posting two items to a child group of its own and waiting, on 1 and 4
      workers: process threads stay at the base plus the workers (sampled every 200 us; slack 2), deepest stack 2 items;
      a chain of 500 groups on one worker (150 under ASan: a macOS worker has a 512 KB stack and ASan frames overflow it
      at 500) completes. Mutant: a wait that starts a thread fails the thread bound.
    - I6 property test: 50 groups, widths 0 to 3, three classes, items fan out to random lower groups and wait, an
      outside producer, 1, 2 and 4 workers, width counted over whole items; 400 iterations in CTest, 2,000 twice in each
      of TSan and ASan. Mutants I1, F1, F2 die in it.
    - AL2 `WorkGroup::on_idle(WorkIdle&)` / `cancel_idle`: small (about 60 lines), so built here rather than moved to X2.
      Tests: fires at once on an idle group, once on the idle-making thread otherwise, `cancel_idle` before and after,
      2,000 nodes under load, and a thread standing for an event loop never reaches `block_guard`. Mutants: never
      taken at the end of a turn, and not fired when already idle, both die.
    - AR2: gone with help-anything; the thread-local observation is the I1 test.
    - Probes on this head (`.scratch/l2b/`, built from the originals unchanged except as noted): `inversion` passes at 2,
      4 (2 busy) and 8 (6 busy) workers, twice, plus TSan 2 and 4 and ASan 4 (1 worker is the probe's artefact: g spins
      on h); `width_xwait` passes at threads 1, 2, 4 and widths 0 to 3, plain and ASan; `stress` chain (own-group wait
      removed from the probe), lastitem pass at 1, 2, 4 workers plain, TSan and ASan; `stress deep`, `nested`, `siblings`,
      `dtor`, `postwait` are own-group waits and abort with the message, as ruled; the child-group form `deepchild`
      completes at N = 100,000 (max depth 2 on one worker, plain) and 20,000 (ASan); `rescue` is obsolete: its child-group
      form (`rescue_child.cc`, with the same `pthread_create` interpose) completes with every post-start thread creation
      failing (EAGAIN) and `creates=0` at W 1/4/16, G 1/16, N up to 20,000 (plain and TSan), so the EAGAIN deadlock has no
      path to take; I5 replaces the thread-growth probe.
    - Sanitizers on `threadpool:19a17d0`: TSan (`-O1`) all 7 CTest targets once, 4 repeats of the 4 work_ targets, and
      `work_wait` with 2,000 graphs twice; ASan+UBSan the same with 3 repeats; all clean.
    - `cpp-libraries/tools/preflight.sh` on `threadpool:19a17d0`: mac-hardened, mac-asan-ubsan, linux, freebsd all ok (the VMs ran
      under a light hold beside strata L5's work, after its final preflight ended).
    - Consumers: strata L5 `27c944e` (pin `0244bd3`) builds and passes 61 of 61 with this worktree substituted for
      threadpool (`.scratch/cons_l5.sh`); scheduler, logger, http-log and prism do not use `WorkGroup`.
  - [x] L2-13. K1 to K3 rerun on the ruled code (strict, exclusive on macOS, Mac x86_64, 16 workers, 5 trials, medians (range),
    data in `docs/status/phase7c-l2/k-ruled/`), against L2-1's numbers: K1 1 group 241 (234-247) ns/task against 269
    (250-277); 1,000 groups 143 (136-152) against 138; 10,000 groups 146 (105-152) against 144; 1 us tasks 81 (79-84) against
    84; 4 producers 79 (70-82) against 69 (40-102); worker-local chains 14 (13-18) against 13 (11-16); CPU per task, 1 group
    1,937 against 2,100. K2 one group alone 5.17 ms against 5.1; beside 1,000 busy groups share 1.01 (0.94-1.02) against
    1.01; reserve 0 1.02 against 1.02. K3 query wait, 1,000 busy groups p50 7.6 us / p99 157.5 us / p99.9 312 us against 8.0 /
    176 / 312; 1 hot group p99 167 us against 162; 10,000 groups p99 147 us against 179; background throughput 16.7k tasks/s
    against 16.6k. Deleting help from waits does not move any row outside the earlier ranges (the harness never waits from a
    worker; the hot path changed only by the frame in `run_item` and one `waiters`/`idle` check per turn).
  - [x] L2-14. Re-check 2's items. B5: `cancel()`, `cancel_and_wait()` and `~WorkGroup` share `WorkGroup::cancel_items()`, which
    frees boxed items unrun (before waiters and idle nodes hear the group is idle) and returns the caller-owned ones;
    `test/work_group.cc` case 17 counts live captures after each path; mutant "cancel_and_wait walks the list and never frees" dies
    on `live == 0` (case 17), and "cancel_items frees nothing" dies (case 8 hangs). B6: the design changed rather than the
    count. A lane has at most one ticket on the ring; a post lists one when none is listed, the worker that pops one lists the
    next before it runs its first item, and a turn that ends with items left lists its own (`grant`, `turn`). `spare()`,
    `num_unparked()`, the pool-wide `_extras` budget and the extra-ticket path are gone; the ring needs `states + 1` entries.
    Invariant in `docs/mechanisms/work-pool.md` ("Work conservation"). My first cut (drop `spare()` only) kept `_extras` and
    failed `case 18b` (the extras budget held by items in turns starved a group, 10 s stall), which is why the budget went too.
    Proof: `.scratch/rc2/wc.cc` (64 long items, deadline 300 ms) under 6 CPU burners, 150 runs at each of 1 to 8 workers:
    1,200 runs, `peak < workers` in 0 (the head was 15 to 19 in 300 at 4 workers); `test/work_group.cc` 30 runs beside 8
    burners, 0 failures; case 1 passes on every run. New `test/work_group.cc` case 18 (a: one group of long items as wide as the
    pool at 1 to 8 workers, 20 reps each; b: a group posted while 3 of 4 workers are held widens when they finish). Mutants
    (`.scratch/mutb.py`, all die): old `spare()` counting restored (case 1 and 18); no list at turn start (case 1); turn end
    drops the ticket (hang); post lists only when `pending > tickets` (case 18); cancel_and_wait leak (case 17).
    Re-check 2's I4b test: `test/work_wait.cc` "I4 liveness" (`.scratch/rc2/i4live.cc`) kills mutant I4b ("TIMEOUT: I4 liveness
    hung"). AL2: `test/work_wait.cc` "AL2 lifetime" adds `al2.cc`'s two loops (2,000 each; 16 cancelled and 1,984 fired once in the
    first run). All `.scratch/mutl2.py` mutants (14, with two new cancel_idle ones) die. TSan (`-O1`) and ASan+UBSan builds:
    all 7 targets pass once and the 4 `work_` targets pass 3 more times in each. K1 to K3 once under `quiet strict`
    (`docs/status/phase7c-l2/k-b6/`, 16 workers, 5 trials, medians), against L2-13: K1 1 group 216 against 241 ns/task, 1,000
    groups 54 against 143, 10,000 groups 163 (54-173) against 146, 1 us tasks 85 against 81, 4 producers 40 against 79, chains
    16 against 14. K2 one group alone 5.17 ms (reserve 0: 4.46); share 1.04 (1.02-1.04) against 1.01, reserve 0 1.03 (0.99-1.05);
    Jain 1.000. K3 (reserve 1, the default) p50/p99/p99.9 beside 1,000 busy groups 8.1 / 179 / 376 us against 7.6 / 158 / 312; hot
    group 7.7 / 165 / 330 against 7.9 / 167 / 338; 10,000 groups 7.9 / 169 / 310 against 7.6 / 147 / 285; background 16.6k tasks/s
    against 16.7k. Reserve 2 p99 read higher than L2-13 (151 against 61 us hot group); an interleaved A/B of the old head and
    this one (3 rounds each, `.scratch/kab.sh`) shows 134 against 155 us hot and 147 against 163 busy with overlapping ranges, so
    that gap is mostly run-to-run state; watch it at the gate.
  - [ ] L2-10. CI green on the pushed branch: run 40981849396 (workflow_dispatch; the workflow has no push trigger for this prefix) was queued when I stopped, no job result yet. The lead watches.
- Done: `threadpool:6573176` merge aging, `block_guard`, the wait fix and `test/work_scheduling.cc`; the docs commit after it;
  the review round (`threadpool:0244bd3`, B1, B3, B4); `threadpool:7a1a5c5` the ruled wait, `on_idle` and `test/work_wait.cc`;
  `threadpool:19a17d0` the ASan chain length and ARCHITECTURE; `threadpool:e28a89c` B5, B6 and the two test gaps (L2-14).
- Doing: nothing.
- Next:
  1. The lead: CI. Run 40995760989 (`threadpool:5b482da`) had linux-asan-ubsan fail: LeakSanitizer found a boxed item that
     `work_wait`'s on_idle test cancelled and dropped (a test bug, fixed in `threadpool:5570775`; macOS ASan cannot see
     leaks, so only Linux did). Run 40996635396 (`threadpool:5570775`, dispatched with `gh workflow run ci.yml --ref
     phase7c/l2-scheduler`): at my last look every job was green except macos-x86_64 (still running) and freebsd-x86_64,
     which failed `test/work_group.cc:54` (`peak == 4`, case 1: one group alone on all four workers, a timing
     assertion under `ctest --parallel 2` on the VM); the same library code passed that job in the first run, and the
     FreeBSD preflight passes it here, so it looks like a flaky pre-existing test (not touched by this change; B3 and B4
     were the same kind), not a regression. Not rerun.
  2. The re-check of B2 and N1 with `.scratch/arch/inversion.cc`, `.scratch/rc1/rescue.cc` (obsolete, see L2-12) and
     `stress.cc deep` (a death test now; `.scratch/l2b/stress.cc deepchild` is its child-group form).
  3. strata L5 must change (list for the lead; strata is not edited here): repin `threadpool` in `CMakeLists.txt:30` from
     `0244bd3` to the L2 head; AC1 text in `include/strata/core/executor.hh` (already states that `wait()` and `cancel()` are
     never called from the group's own work); audit that no strata test or chore calls `executor::group::wait()` or
     `cancel()` from an item of the same group, or destroys a `work_group` from its own item (all abort now); no use of
     `help_until` exists. Xapiand: D4 and V2's negative test become a worker waiting on a child group plus the I2 death test
     (AC2); X2 wraps `on_idle` as an awaitable resuming through `asio_scheduler::post()` and sets `block_guard` to
     `engine::check_may_block`; a host that waits often keeps `reserve >= 1`; contract in `docs/API.md` "Waiting".
  4. K6 (FreeBSD kernel time) is still open from `scheduler.md`.
- Blocked on: nothing.
- Decisions to confirm (architect), each a reading of the task:
  1. `block_guard` is a function pointer in `Options`, called with a site name, so the library has no reactor
     knowledge. It fires for any non-worker thread on entry to `wait()` (`help_until` is gone), even when the wait would
     return at once, so a violation is found deterministically; hosts that legitimately wait from outside (shutdown,
     tests) must have the guard allow it.
  2. Aging is per entry and uses a stamp in the merge ring. A pool-level "no merge for T" rule was rejected: it
     promotes one entry per interval (mutant M4 shows the test catching it).
  3. Flushes do not age against queries (the research note's open item stays open; 7c posts few queries per
     flush stream).
  4. `reserve` default stays 1 (C8.3), because 7c posts at `query`.
  5. From `scheduler.md`, still open: extra tickets capped pool-wide; the destructor cancels; `width` per class.
- Last verified: 2026-10-29, Mac x86_64, Apple clang (plain, `-O2 -g`, asserts on) and Homebrew LLVM 23 (TSan,
  ASan+UBSan): all 7 CTest targets pass.

## Log
- 2026-10-29: `threadpool:5caedce` found already on main `394aaea`; worktree `phase7c/l2-scheduler` cut from it.
  Baseline TSan and ASan+UBSan clean. Aging, guard, wait fix and tests in `threadpool:6573176`; first run of
  the nested V2 case hung, which is how the wait deadlock was found. Mutants, sanitizers, K1 to K3 reruns, A/B,
  consumer builds and preflight run the same day.
- 2026-10-29 (review round): `docs/reviews/phase7c-l2-code.md` committed as delivered, then the fixes above. The
  nested probe `chain 4` found the cross-group hang while closing N1; the wait rules changed three times in this round
  (width admission, epoch, cross-group suspended), each with a test that hangs without it.
- 2026-10-29 (architect ruling): `threadpool:7a1a5c5` implemented AL1/AL2 and deleted the rescue threads, the cap, `suspended`,
  the epoch and help-anything. The first run of the new tests passed; the mutants (`.scratch/mutl2.py`) all died except the
  helpable half of the wake, which has no liveness consequence. ASan overflowed the macOS worker stack on a 500-group chain
  (150 there). Linux CI's LeakSanitizer found a test leak mac cannot see. K1 to K3 rerun after the VM preflight.
