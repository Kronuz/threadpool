# Phase 7c L2 code review (WorkPool: merge aging, block_guard, V2 waits)

Reviewer: code-reviewer, 2026-10-29. Subject: `threadpool:2ae90f0` on `phase7c/l2-scheduler` (from main `threadpool:394aaea`,
includes `feature/scheduler`). Read: `work_pool.hh` (all of the L2 paths), the three test files, the ledger, `docs/mechanisms/work-pool.md`.

## Verdict: REJECT (one fix round, then a scoped re-check)

Two real wait defects in the code V2 says is done, plus the two CI failures the lead reported. Everything else holds: aging, the
caps, `block_guard`, shutdown and the API surface check out, and the TSan and ASan stress probes of nested, sibling and last-item
waits are clean. The blockers are narrow and local to `WorkGroup::wait()`.

## Blockers

### L2-B1. A worker waiting on another group with `width` set can hang forever (V2 not closed for cross-group waits)
- `work_pool.hh:1305` (`helpable`): a non-own waiter helps only when `l.tickets + l.helpers < s->width`. `tickets` counts ring
  tickets nobody is serving yet, so a width-1 group with one queued item and its ticket sitting in the ring is "full", and the waiter
  does not run the item. If every worker is inside such a wait, nobody ever takes the ring ticket. The line is unchanged from
  `threadpool:5caedce`, so this predates L2, but V2's claim ("no deadlock in any class pairing") rests on tests that only wait on the
  waiter's own group (`test/work_scheduling.cc` case 6, own group only; `own` skips the width check).
- Probe: `.scratch/review-l2/width_xwait.cc` (`./wx3 <threads> <width>`; each of `threads` items sleeps 50 ms so all workers are busy,
  posts one item to group H, `H.wait()`). Result: threads 1, 2, 4 with width 1 abort in the 5 s watchdog every run; width 0, 2, 3 pass.
  The same defect hangs `.scratch/review-l2/stress_asan chain 2 4` (5-group acyclic chain, odd groups width 1); the TSan and
  plain builds of the chain case passed only because an idle worker took the ticket.
- Fix direction: a helper may serve when `tickets - listed + helpers < width` (running/served tickets only), or the waiter may pop the
  ring ticket itself. Add a case-6-style test: all workers waiting on a width-capped other group.

### L2-B2. Inline help is unbounded recursion; a few thousand self-waiting items overflow the worker stack
- `work_pool.hh:1428-1430` and `help_own` (`:1033`): an own-group waiter runs a queued sibling inline on its own stack, and that sibling
  may wait and run the next one. Depth equals the number of queued items that wait. V2 was tested "ten deep".
- Probe: `.scratch/review-l2/stress.cc deep` (`./stress_plain deep 1 5 N`: N items on one group, each calls `g.wait()`, 1 worker).
  N=1000 completes (maxdepth 1000); N=5000, 20000, 100000 crash (SIGBUS, stack overflow, -O2); under ASan N=2000 overflows.
- Also a latency effect: the outer waiter cannot return until every inlined inner item has finished.
- Fix direction: cap the inline depth (a depth counter in `tls_here`, say 16 to 64); past the cap sleep as a non-helper does and rely
  on other workers, or serve only items that were not queued by a waiter. Add the N-deep test.

### L2-B3. CI: `work_scheduling.cc:300` (`at_refresh.load() < 20`) fails on both macOS runners
- The count is absolute, taken at the refresh item, but the main thread's `sleep_for(5ms)` before the enqueue can oversleep on a
  loaded runner while the workers keep parsing, so `parsed` is already above 20 when the refresh is posted. The assertion measures the
  sleep, not the scheduler. The other timing asserts in the case (`separate < 30ms`, line 306; `shared > 40ms`) are wall-clock too.
- Not reproduced here (x86_64 Mac, 16 cores; 8 runs at 30 busy loops, 0 failures), so this is from reading the code, not from a
  repro. Yes, ordering and counts can replace the time asserts: record `before = parsed.load()` just before the enqueue and assert
  `at_refresh - before <= 2 * threads + 2` (the refresh gets a turn within a couple of parse turns whatever the clock does); in the
  one-FIFO run assert `at_refresh == 200` (every item was queued ahead of it, so the order is strict); drop the two wall-clock bounds
  or keep them only as a printed number.

### L2-B4. CI: `work_group.cc:128` (`ratio > 1.3 && ratio < 3.5`) fails on x86_64 macOS
- New to main: `test/work_group.cc` is not on `394aaea`; it arrives with `feature/scheduler` (`b274039`) and no L2 commit touches it.
  The ledger notes the CI workflow has no push trigger for this prefix, so this is the first CI exposure, not a regression from L2.
- The test samples two counters after a fixed 60 ms window on one worker. DRR charges wall-clock ticks, so a descheduled item charges
  its group for the pause, and a 60 ms sample on a stolen runner holds few items. Not reproduced locally (8 runs at 30 busy loops, 0
  failures).
- Replace the time window with a count sample: run until `light` has finished a fixed count (1000, say), read `heavy`, and assert the
  ratio within [1.5, 3] of that; the per-turn jitter then averages over thousands of turns. Keep the printed ratio.

## Other findings, non-blocking

- L2-N1. Sibling waits are not stable (`.scratch/review-l2/stress.cc postwait`): two items of one group in `wait()` together both see a
  quiet group, but the one that returns first decrements `suspended` while still running, so the other no longer satisfies
  `running == suspended` and stays in `wait()` until the first item finishes. If the first then blocks on the second (a flag, a
  future), it hangs: A: `wait(); spin until flag`, B: `wait(); flag = true` hangs on 2 and 4 workers (on 1 worker it is the
  spin holding the only worker, a probe artefact). The contract text says "other items inside their own wait()" but does not
  say a returned waiter breaks the other. Document it, or keep `suspended` counted until the item leaves (not only until `wait()`
  returns) for a bounded window. 2000 iterations of nested/sibling/chain/last-item stress otherwise passed (below).
- L2-N2. `block_guard` fires on every blocking entry the design names, and only there. Probe `.scratch/review-l2/guard_dtor.cc`:
  external `wait()` (+1, site `WorkGroup::wait`), the `~WorkGroup` wait on a non-worker (+1), external `help_until` (+1), worker
  `wait()` and `help_until` (0 hits). Not guarded: `sleep_until` (blocks the caller for the interval, measured 31 ms with +0 hits),
  `~WorkPool` joins, and a worker that blocks on the futex in `wait()` for a group whose items run on other workers (by design,
  cross-group cycles can deadlock). The destructor hit means hosts must let `~WorkGroup` through (the ledger's decision 1 says so;
  say it in `docs/API.md` too).
- L2-N3. A worker-side `wait()` on another group serves that group's flush and merge items without taking a background or merge slot
  (`helpable`, `turn(..., true)`), so a query worker in such a wait can add background concurrency beyond `bg_cap`/`merge_cap` for the
  duration. It is the price of not deadlocking; note it beside the cap claims in `work-pool.md`.
- L2-N4. Aging only orders merges ahead of flushes: `pick()` pops the query ring first, so aging can never starve queries, and under
  saturating query load merges still wait (they hold `bg` and `merge` slots when they do run, so `merge_cap` and the reserve are
  respected; `test/work_scheduling.cc` cases 2 and 3 plus mutants M2-M4 cover it). `merge_overdue()` is a racy head peek; a wrong
  answer only delays or repeats a promotion.
- L2-N5. K numbers rest on a sound method (strict hold, 5 trials, medians with ranges, interleaved 7-round A/B for the aging change),
  with two soft spots: the 4-producer row spans 40 to 102 ns and its A/B (58 against 70) is dismissed as noise although the ranges
  overlap only partly, and the worker-local chain row for the prototype is an acknowledged outlier. All K data are Mac x86_64; the
  cross-platform rule (Linux, FreeBSD, arm64) is not met for the numbers, only for the sanitizer/preflight matrix. Watch the
  4-producer row at the gate, as the ledger says.
- L2-N6. Lifetime contract: groups must be gone before the pool (`~WorkPool` comment); a group outliving the pool is UB with no check.
  Probe results: group destroyed with 20 queued items that each wait on the group, from an external thread (`stress dtor`,
  300 rounds, TSan and ASan clean); pool destroyed with 3000 queued group items and 3000 pool-own items of all classes: all ran
  (`guard_dtor`). Documented contract, acceptable.
- L2-N7. The ledger's "Last verified" test binaries in `.scratch/plain` predate the last edit of `work_pool.hh` (16:33 against 16:34
  mtime); the reviewer's rebuilt tests pass. Rebuild before quoting counts. (The stale binary failed `flood.peak <= 3` under load; a
  fresh -O1/-O2 build of the same test passed 12/12 under 14 to 30 busy loops, and a cap-counting instrumented copy found no
  violation. Treat as a stale build, not a defect.)

## What was checked and held

1. TSan (Homebrew LLVM 23, `-O1`) on `.scratch/review-l2/stress.cc`: nested waits (random depth 4 over 4 groups, 3 external threads,
   widths 0/1), siblings waiting together, acyclic cross-group chain, last-item wait racing group destruction, `help_until` racing the
   last item, group destroyed while items are suspended: 4 s each at 1, 2 and 4 workers, no race, no hang, except the chain with
   width-1 groups under ASan (B1).
2. ASan+UBSan: nested, siblings, last-item, dtor clean; chain hung (B1); deep overflowed (B2).
3. Additivity: `git diff main..HEAD` touches no existing library header or source (`thread.cc`, `thread.hh`, `threadpool.hh`,
   `task_group.hh` are unchanged); `work_pool.hh` is new, the CMake additions sit under the top-level-project guard, and
   `Options::block_guard` is last in the aggregate, so positional initialisers of the older fields stay valid. The ledger's
   consumer builds (scheduler, logger, http-log, prism, strata) are in `.scratch/consumers.out`; I did not rebuild them.

## Probes kept (all in `.scratch/review-l2/`)
`stress.cc` (cases nested, siblings, chain, lastitem, dtor, deep, postwait; builds `stress_tsan`, `stress_asan`, `stress_plain`),
`width_xwait.cc` and `wx2.cc` (B1; `wx3` binary), `guard_dtor.cc` (N2, N6), `loaded.sh`, `loaded2.sh`, `loaded3.sh`, `loaded4.sh`
(the CI tests beside CPU burners), `dbg/` (a header copy with cap-violation and wait-state prints).

## Re-check scope after the fix round
B1: `width_xwait` at threads 1, 2, 4 and widths 0 to 3, plus `stress_asan chain 2 4`; B2: `stress_plain deep 1 5 100000`;
B3 and B4: the two CI runs green, and no wall-clock bound left in cases 5 and the weight case; N1 documented or fixed.

Not run: Linux, FreeBSD and arm64 builds (left to CI); any benchmark or K rerun; consumer rebuilds (relied on the ledger's
record); a full Xapiand build; a TSan run of the full CTest suite on the final head (the tests were exercised by the builder; I ran
my own stress probes under TSan and ASan instead).

## Re-check 1

Re-checker, 2026-10-29. Subject: fix head `threadpool:0244bd3` (the review is `threadpool:d623d80`). Read `work_pool.hh` and the tests in that
commit only. Probes and harnesses in `.scratch/rc1/` (rebuilt against the fix head; the original `stress.cc` and `width_xwait.cc` are reused unchanged).

### Verdicts

- **L2-B1 CLOSED.** `work_pool.hh:938-948` (`helpable`: `tickets - listed + helpers < width`) and `:925-932` (`serve()` gives its ticket up when a helper
  holds the width). `width_xwait` (`wx3`, plain and ASan+UBSan) passes at threads 1, 2, 4 and widths 0 to 3 (24 of 24; the review had 1, 2, 4 at width 1
  aborting in the watchdog). `stress_asan chain` at 1, 2 and 4 workers passes (84k, 63k, 115k iterations). Revert (mutants run in a copy of the tree,
  `.scratch/rc1/mut`, so the worktree was not touched): F1 (old rule) dies on case 6d ("width cross wait hung"), F2 (no ticket yield) dies on case 6e
  (`peak.load() == 1`, `work_scheduling.cc:508`). The width also holds for work served on a rescue thread: `.scratch/rc1/widthres.cc` (query-class waiters
  nest past 64 on other groups, so rescue threads serve a width-capped group as helpers) measured max concurrent 1, 2 and 3 for widths 1, 2 and 3
  with 3 to 37 rescue threads created (plain and TSan).

- **L2-B2 OPEN (the reported overflow is fixed, the mechanism is not sound).** The original probe passes: `stress_plain deep 1 20 N` for N = 1000, 5000,
  20000, 100000 completes (maxdepth 2283 at 100000, no crash); `stress_asan deep` at N = 5000 and 20000 on 1, 2 and 4 workers passes; `max_inline_depth = 64`
  at `work_pool.hh:669`, `spill`/`rescue` at `:950`/`:961`, the call site at `:1543-1552`. Revert: F3 (no rescue) and F3b (no depth bound) both die in case 6f.
  Rescue threads do exit and `~WorkPool` waits for them (`:1245-1251`; `after ~WorkPool threads=` is back to the baseline in every run), and TSan and ASan are clean
  on `stress` (nested, siblings, chain, lastitem, dtor, deep at 1, 2, 4 workers) and on the rescue probes. Two defects:
  1. **A failed thread creation deadlocks the pool.** `work_pool.hh:1548-1552`: when `spill()` returns false (`std::thread` threw, EAGAIN from a thread or
     pid limit), the deep waiter drops its reference and falls through to `futex_wait` at `:1577`, with nothing left to serve the group and no retry until some
     unrelated wake. Probe `.scratch/rc1/rescue.cc` with `libinterpose.dylib` (a dyld interpose of `pthread_create` that returns EAGAIN after K creations;
     `./rescue_plain W G N K 0 0`): `1 1 200 0` (every rescue creation fails) hangs at 0 of 200 progress; `4 1 2000 0` hangs; `4 1 2000 3` hangs at 779 of 2000;
     `4 1 20000 10` hangs at 2373 of 20000. The same N complete with no injection (`4 1 2000 -1`). Not a crash, but a silent permanent hang under a thread limit,
     and no test injects it.
  2. **Thread count is unbounded, linear in the nested waiters.** There is no pool-wide cap on rescue threads: a new one starts whenever a state has none that is
     idle (`s->rescuers == s->rescue_busy`, `:1543`), and every rescue thread can nest 64 more. Probe `rescue_plain <W> <G> <N> -1 0 1500` (each group has a gate item
     that keeps it busy while N items per group wait on it, as a group of long chores waiting on their index would): 20,000 waiting items on 4 workers hold 315 threads
     (729 created); 4 groups x 20,000 hold 648 (1228 created); 16 groups x 5,000 hold 634; 100 groups x 500 hold 80. Each group with a deep waiter adds at least
     one. That is the stack cost the old design paid in one stack, now paid in threads; with a pid or `ulimit -u` limit it runs into defect 1. Acceptable only if the limit is
     stated and defect 1 degrades gracefully. Fix direction: on `spill()` failure keep the waiter retrying (timed `futex_wait`, or serve inline past the cap as the
     last resort) instead of sleeping on a word nobody will bump; add a pool cap or at least state the bound in `API.md`/`work-pool.md` (threads <= groups with deep
     waiters + nested waiters / 64); add an injection test (a hook or a `std::thread` factory option). A rescue thread also runs 64 nested waits on the default
     secondary-thread stack (512 KB on macOS), so 64 times the item's own frame must fit; worth one line in `API.md`.

- **L2-B3 CLOSED** (test fix). `test/work_scheduling.cc:273-312`: one worker; the refresh is posted after `parsed >= 3` and asserts `at_refresh - before <= 2*threads + 2`
  (own group) and `at_refresh >= 200 - threads` (one FIFO); the two wall-clock bounds are gone (only 60 s loop timeouts remain). Passes 6 of 6 beside 4 CPU burners, and
  the mutant that drains a lane in one turn still dies on it. Residual: `before` is read a few instructions ahead of the `enqueue`, so a preemption there can add parse
  items to the difference; a window of nanoseconds, not a sleep. Not verifiable here: the CI run itself (L2-10 is open in the ledger).

- **L2-B4 CLOSED** (test fix). `test/work_group.cc:122-130`: sampled when `light` has run 1,500 items (6000 queued), ratio in [1.5, 3.0]; measured 2.01x beside 4 burners,
  6 of 6 runs pass. No wall-clock bound left. CI run not seen.

- **L2-N1 PARTIAL, as the ledger says.** The epoch fix works for the review's case 6c (F4 dies "post-wait siblings hung") and the cross-group `suspended` change fixes the
  `chain` hang at 4 workers (F5 dies). `stress_tsan postwait` still hangs at 2 and 4 workers (1 worker passes), as the review found. `docs/API.md:138-140` now states the
  contract ("an item that runs there must not block on the progress of the waiter below it").

### N1 as a consumer contract (for L5 and X2, not an L2 blocker)

The pattern is plausible. `docs/design/phase7c.md` D4 has chores that wait for their own index's work call `WorkGroup::wait`, and a sibling chore of the same group is
then served inline on that waiter's stack. The realistic form is not a flag: it is a **lock or turn held across `wait()`** (a commit or close chore holding a strata
writer lock, or a lease, while it drains the group; a sibling flush or merge chore served inline on its stack that needs the same lock deadlocks with itself, even on one
worker). D5 says the turn is held by the awaiter coroutine, not by the worker, so the async turn itself is probably safe; strata's own internal mutexes are the thing to audit.
Condition for the consumers, not for L2: L5 and X2 must state "no lock held across `wait()` that a chore of the same group takes", assert it in a test of a commit chore waiting
on a merge chore of the same index, and consider a `wait()` variant that does not serve inline (parks, or serves only other groups) for chores that hold locks.

### New findings

None in the fix beyond B2's two defects above. One line for the lead: the CI run (L2-10) was still queued in the ledger, so B3 and B4 are closed on code and local load runs,
not on CI.

Not run: Linux, FreeBSD and arm64 builds; any benchmark; consumer rebuilds; the full CTest suite under TSan/ASan on the fix head (the six mutants ran the plain build of
`work_scheduling_test` in a copy; the sanitizers ran the review's stress probes plus the new rescue probes); the other mutants (M1 to M9, unchanged by this commit);
an EAGAIN test on Linux (the injection is a macOS dyld interpose); a run of `work_pool_test`/`work_group_test` beyond the load runs above.
