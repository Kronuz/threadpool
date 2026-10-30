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
