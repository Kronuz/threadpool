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

## Architect ruling on B2 and N1

- Date: 2026-10-29.
- Reviewed: `threadpool:b1fe0c3` (`work_pool.hh:442-444`, `:661-671`, `:938-990`, `:1101-1140`, `:1299-1310`, `:1378`, `:1463-1577`), this file's Re-check 1,
  `docs/API.md:100-160`, `docs/mechanisms/work-pool.md`; strata `strata-wt/phase7c-l5` at `strata:0bc8969` (`include/strata/core/executor.hh:27-40`,
  `work_pool_executor.hh:73-118`, `pool_executor.hh:60-100`, `index.hh:1721-1734`, `:1880-1888`, `:2841`, `:3103-3105`, `:3392-3410`, `owner.hh:1-40`);
  Xapiand `docs/design/phase7c.md` D4, D5 (`:188-245`); task `task:9a85e89` (`task.hh:687-740`, `event`).
- Checked against: Lucene `search/TaskExecutor.java:83-115` (parity-src), Rayon `rayon-core-1.13.0/src/registry.rs:780-817`, Tantivy 0.26
  `core/executor.rs:44-80`, `indexer/segment_updater.rs:262-341,516`. oneTBB from memory (not on this machine).
- Checked by: Homebrew LLVM, `-O2` and `-O1 -fsanitize=thread`; probe `.scratch/arch/inversion.cc` (build `.scratch/arch/build.sh`, binaries
  `inversion`, `inversion_tsan`).
  - Not run: an implementation of the ruling (none exists yet); Linux, FreeBSD, arm64; any benchmark; the EAGAIN probe (Re-check 1's numbers are taken as given).

### Decision

**(b), completed by the awaitable half of (c). (a) is rejected.** A wait serves only the group it waits on, a wait on a group already on the calling
stack (its own group included) is a programming error that aborts, no thread is ever created by a wait, and a thread that must not block (an event
loop, a coroutine) joins a group through an idle notification instead of `wait()`. The own-group "quiet" semantics, the epoch, `suspended`,
`max_inline_depth`, the rescue threads, and serving other groups' queries from a wait are deleted, not tuned.

### Why: the current model is unsound for acyclic programs, independent of depth

AR1. **Help-anything deadlocks an acyclic group graph** (new; it applies to the head and to (a), which keeps the mechanism). A waiter that finds
nothing to serve in the awaited group runs other groups' queries (`work_pool.hh:1553-1559`, `help_query`). The query lands on top of the waiter's
own item, so if the query waits on that item's group, neither can finish. Probe `.scratch/arch/inversion.cc` (`./inversion <threads> <timeout_s>
<busy>`): groups Q -> G -> H, nothing waits on Q. `g` (G, flush) posts `h` (H, 1.5 s) and calls `H.wait()` while `q` (Q, query, calls `G.wait()`) is
queued. Results: 2 workers 3 of 3 hang, 4 workers with 2 busy 1 of 2, 8 workers with 6 busy 1 of 1, TSan 2 workers 1 of 1 (no race reported, hang).
In every hang `h` had finished (`h_running=1`) and `q` ran on `g`'s thread (`q_saw_lease=1`). The 1-worker run is a probe artefact (`g` spins on `h`,
which cannot start). The graph has no cycle; the deadlock is made by the scheduler.

AR2. **Help-anything leaks thread identity.** In the same run, `q` read `g`'s thread-local (`q_saw_lease=1`). strata counts lease scopes per thread
(`owner.hh:31-38`, `owner_thread().leases`), so a chore of index B inlined above a commit chore of index A passes `owner_only()` for B in debug builds.
Serving only the awaited group keeps the inlined work within what strata already marks (`work_pool_executor.hh:64`, `off_owner_scope`).

AR3. **The depth and thread costs come from self-waits, which no consumer needs.** B2's shape is N items of G each calling `G.wait()`; the waiters
are members of what they wait for, so each queued waiter nests on the next (B2) and the "quiet" rule needs `suspended` and an epoch, which breaks
when a sibling returns early (N1). The rescue threads move that recursion into threads (Re-check 1: 315 threads for 20,000 waiters) and add a
failure path (EAGAIN deadlock). Any pool-wide cap on rescue threads moves the hang to the cap; no cap is unbounded memory under the
many-indexes workload. Neither is a stated, load-bearing limit, so (a) cannot be made sound.

### The consumers' actual use (all of it is (b)-shaped)

- strata waits only from the index's owner on the index's own child groups: `publish_merges` on the merge group (`index.hh:3105`),
  `finish_deletes`/`end_deletes` on the delete group (`:3395`, `:3405`), and `cancel()`/destruction of each (`work_pool_executor.hh:94-113`).
  The posted work does not wait: `run_merge` (`:2841`), `run_delete` (`:3432`), and the run builds (`:1887`, polled by `take_runs`, never joined).
  The groups are opened per index (`:1728-1731`, width 1 by default), so the owner is never a member of what it waits on. Under Xapiand the owner
  is a chore of the index's group X (D5), so the chain is X chore -> M or D item: inline depth 2, whatever the number of indexes or items.
- strata's own `inline_executor` already runs the awaited work on the waiter's thread (`executor.hh:107`), so inline service of the awaited group
  is the model strata was written against.
- Xapiand D4 (`phase7c.md:203-207`) is the one own-group use: "a chore that waits for its own index's work calls `WorkGroup::wait`", and the
  carried V2 negative test is an own-group wait. Under the ruling that wait moves to the task that holds the turn (it awaits the group's idle
  notification) or the chore posts what it waits for to a child group.
- No other repository under `~/code/Kronuz` uses `WorkGroup` or `help_until` (grep; only strata's binding).

### Practice elsewhere

- Lucene `TaskExecutor.invokeAll` (`:83-115`): the caller runs unstarted tasks of its own batch only (a shared index counter), then blocks on the
  futures of the ones others claimed; the comment at `:100-104` names deadlock avoidance when the caller is one of the executor's threads. That is
  (b). Lucene merges run on `ConcurrentMergeScheduler`'s own threads, never joined from a pool worker.
- Tantivy merges and segment updates run on dedicated pools that the caller joins from outside (`segment_updater.rs:281-312`, `:341`, `:516`);
  search fan-out is a Rayon scope (`executor.rs:65`).
- Rayon's waiter steals anything (`registry.rs:788-808`: local, then other deques and the injector). That is help-anything; it is sound for
  pure compute that never blocks or holds locks, and Rayon documents that a job blocking inside the pool can deadlock. strata merges and Xapiand
  chores do I/O and hold leases, so the Rayon model does not transfer.
- oneTBB (from memory, not read here): a `wait` also takes any arena task, and `this_task_arena::isolate` was added so a waiting thread takes only
  tasks spawned inside the region, which is (b) as an opt-in; it also stops stealing past a stack threshold. We make isolation the only mode.

### Progress argument the implementation must make true

Obligation on the program: the wait-for graph is acyclic (an edge P -> G when an item of P may wait on G or block on something an item of G
produces), and an item blocks only through `wait()` on a lower group or on a bounded external event.

Claim: every `wait()` returns. Take a non-returning `wait(G)` with G of least height. (1) Every running item of G finishes: above it on its stack sit
only its own wait on a lower group and that group's items (I1, I2), and those waits return by minimality. (2) Queued items of G get served: a worker
waiter is admitted whenever fewer than `width` turns of G are being served, ring tickets not counted (I3, the B1 rule at `:938-948`), and the
served turns end by (1); waking it then is I4. So a worker's wait never needs another free worker. (3) Hence every worker returns to its loop, so a
non-worker waiter's ring ticket is popped (bg and merge slots are held only during turns, which end). Contradiction. AR1's counterexample is exactly
step (1) failing: an unrelated item above a running item of G.

### Invariants (each with a test and a mutant that must die)

- **I1 Help scope.** A wait on G runs only items of G, inline, as a width-bounded helper; it never runs another group's work or a query.
  Test: the AR1 shape (`inversion.cc`, made a CTest case with a watchdog) at 1, 2 and 4 workers passes; a recorder shows no item of another group
  runs on the waiter's thread inside the wait window. Mutant: restore `help_query()` in `wait()` -> the AR1 case hangs.
- **I2 No wait on the calling stack.** `here` becomes a chain (a parent pointer per inline frame); `wait()`, `cancel_and_wait()` and `~WorkGroup`
  on any group present in the chain call `std::abort` after naming the site (all builds; a hang is the alternative). Tests (death tests): self-wait,
  `~WorkGroup` from its own item, and a G -> H -> G chain on one stack. Mutants: drop the chain walk (self check only) -> the G -> H -> G test hangs
  instead of aborting; drop the self check -> the self-wait test hangs.
- **I3 Self-sufficient helpers.** Admission is `tickets - listed + helpers < width`; a ring turn that finds a helper over the width gives its ticket
  up (`:925-932`). Keep 6d and 6e, rewritten so the waiter is never a member. Mutants F1 and F2 of Re-check 1 must still die.
- **I4 No lost wake.** Item end, turn end, helper leave, post and cancel wake G's parked waiters when G becomes idle or helpable. Test: a width-1
  group with its width held by a 50 ms item and three worker waiters parked; all finish. Mutant: drop the wake on helper leave or turn end -> hangs.
- **I5 Bounded resources.** A wait never creates a thread and inline depth on a stack is at most the number of distinct groups on its wait chain.
  Tests: 20,000 items on G, each fanning out to its own child group and waiting on it, 1 and 4 workers: completes, the process thread count stays
  `threads + test threads` throughout, maximum depth observed is 2 (a debug counter); a recursive chain of 500 child groups on 1 worker completes
  under ASan. Mutant: reintroduce `spill()` above any depth -> the thread-count assertion fails.
- **I6 Progress under random acyclic graphs.** A property test: random DAGs of 50 groups, widths 0 to 3, all three classes, items waiting on
  random lower groups, external and worker waiters, 1, 2 and 4 workers; TSan and ASan builds separately, a watchdog, at least 2,000 iterations each.
  Mutants I1 and I3 must make it fail.

Delete with the change: `GroupState::suspended`, `quiet`, `rescuers`, `rescue_busy` (`:441-444`), `max_inline_depth` and `Here::depth`
(`:665-669`), `spill`/`rescue` and `_rescuers` (`:950-990`, `:733`, `:1248`), the `own` branch of `helpable`, `help_own` as a wait path, the help
of other groups' queries from `wait()` (`:1553-1559`), tests 6, 6b, 6c, 6c2 and 6f in their own-group form (rewritten as child-group cases), and
`help_until` unless a consumer is named (none today; it has the same help-anything hazard at `:1305`).

### Consumer contract (goes into `docs/API.md` and `docs/mechanisms/work-pool.md`)

1. `G.wait()` returns when nothing of G is queued or running. Call it only from outside G: never from an item of G or from an item of a group
   whose wait is on this stack. That aborts. To join work from inside a group, post it to a child group and wait on that.
2. The wait-for graph must be acyclic: an item of G blocks only by waiting on lower groups or on bounded external events, never on a lock, flag or
   future that its waiter, its waiter's caller, or another item of G provides. Such a block hangs in any scheduler; it is not specific to inline
   serving.
3. Hold no lock across `wait()` that the group's items take.
4. Items of G may run on the waiter's thread: they must not rely on a thread identity distinct from the waiter's (recursive mutexes, thread-keyed
   ownership) except through scopes they set themselves (strata's `off_owner_scope` does).
5. A worker inside a wait serves G or sleeps; it serves no queries. Workers lost to waits are at most the number of concurrent waits; a host keeps
   `reserve >= 1` for queries if waits are frequent.
6. Event loops and coroutines never call `wait()` (`block_guard`); they join through `WorkGroup`'s idle notification (an intrusive node with a
   `noexcept` callback run by the thread that makes the group idle, which only posts; the threadpool does not depend on the task library). Xapiand
   wraps it as an awaitable resuming through `asio_scheduler::post()`, like `task::event`.
7. Stack: inline depth equals the program's own nesting of group waits; each level costs the waiting item's frame plus the served item's. For
   strata under Xapiand it is 2.

### B2 and N1

- **L2-B2 stays OPEN** until I1 to I6 are implemented and their tests and mutants recorded; the rescue-thread defects (EAGAIN, thread growth) close
  by deletion. Re-check with `.scratch/arch/inversion.cc`, Re-check 1's `rescue.cc` (must now show zero threads created), and the review's
  `stress.cc deep` (now a death test, since it is a self-wait; its child-group form must complete at N = 100,000).
- **L2-N1 closes with the ruling.** Its shape needs two members of G waiting on G, which I2 forbids. What remains, an item that blocks on its
  waiter's later progress, is a cycle in the wait-for graph and is covered by contract item 2. Re-check 1's lock-held-across-wait concern is item 3.

### Blocking items

- AL1. Implement the ruling in `work_pool.hh` with I1 to I6 and their mutants (above); update `docs/API.md` and `work-pool.md` with the contract.
- AL2. The idle notification (contract item 6) lands in threadpool before Xapiand X2 uses it; it is not needed for strata and does not block L2 if
  X2's first step carries it.

### Conditions

- AC1 (L5, strata): `executor.hh:27-33` states that `group::wait()` and `cancel()` are never called from that group's own work, and drops the line
  about "running an index's owner on the only worker its work needs" (a worker owner now serves the work). No code change in
  `work_pool_executor.hh`.
- AC2 (Xapiand design, before X2): `phase7c.md` D4 replaces the own-group wait with the turn holder awaiting the group's idle notification or a
  child group, and V2's negative test becomes "a worker waits on a child group whose work is queued only there" plus the I2 death test.

Verdict: **REJECT stands for L2 (B2 open), with the design decided.** Blocking: AL1 (AL2 may move to X2's first step). Conditions: AC1, AC2.

Not run: the ruling's implementation and its tests (nothing to run yet); Linux, FreeBSD and arm64; any benchmark; consumer rebuilds; the
EAGAIN injection (taken from Re-check 1).

Probes kept: `.scratch/arch/inversion.cc`, `.scratch/arch/build.sh`, `.scratch/arch/config.h` (copy of `.scratch/rc1/config.h`), binaries
`inversion`, `inversion_tsan`.

## Re-check 2

Re-checker, 2026-10-29. Subject: `threadpool:7a1a5c5` through `threadpool:2947834` (the ruling is `threadpool:6c3491d`). Read `work_pool.hh` at that head
(wait/on_idle `:1286-1460`, `turn`/`serve`/`helpable` `:893-990`, `grant` `:795-815`), `test/work_wait.cc`, and the two items the lead added (B5, B6). Probes in
`.scratch/rc2/`: `i4live.cc`, `al2.cc`, `deep.cc`, `wc.cc` (+ `wc_inst.cc`), `b5.cc` (strata's probe, unchanged); `.scratch/arch/inversion` rebuilt; the developer's
`.scratch/mutl2.py` rerun. Builds under `quiet light`: TSan (`-O1`) and ASan+UBSan (Homebrew LLVM 23), all 7 CTest targets pass on the head in both
(`.scratch/rc2/tsan.ctest.log`, `asan.ctest.log`).

### Verdicts

- **L2-B2 CLOSED.** The mechanism is deleted: grep over `work_pool.hh`, the docs and `test/` finds no `rescu*`, `spill`, `max_inline`, `suspended`, `quiet`, `help_query`,
  `help_own`, `help_until`; the quiet-epoch field is gone (`GroupState::epoch` at `:457` is only `cancel()`'s stop token). Only `WorkPool`'s constructor creates threads
  (`:1186`); `wait()` (`:1306-1334`) has no thread creation, and mutant "a wait creates a thread" dies on I5. Child-group form of the review's `deep`: `deep.cc`
  (every item posts one item to its own child group and waits) completes at N = 100,000 on 1, 2, 4 workers, plain, and 1, 4 under ASan+UBSan. The own-group form is the
  I2 death test.
- **L2-N1 CLOSED** with the ruling: two members of one group waiting on it abort (`check_stack`, `:1289-1295`); what remains is contract items 2 and 3 in `docs/API.md`.
- **AL1 CLOSED**, with one test weakness (I3c below).
  - I1 `test/work_wait.cc:318-383` (inversion shape at 1, 2, 4 workers, `foreign == 0`, `q_saw_lease == 0`). Mutant "wait serves a query": killed, and killed on I6.
  - I2 `:265-312`: self wait, `~WorkGroup`, `cancel_and_wait`, G -> H -> G, plus a control. `here.top` is a parent chain (`Frame`, `:668-692`) and `check_stack` walks it.
    Mutants "top frame only" and "parents only": both killed by the death tests.
  - I3 `:388-426`, `:687-723` (I6). Mutant F1 (`tickets + helpers < width`): killed ("I3 width hung") and by I6 (timeout). Mutant F2 (ticket does not yield, `:925-932`):
    killed on `peak == 1` (`:425`) 3 of 3 reruns and by I6 (`peak_over == 0`), but it SURVIVED once in the full mutant pass beside other light holds, so the I3 test is
    probabilistic on that mutant; I6 backs it up.
  - I4 `:432-458`. Mutant "no wake at turn end": killed ("I4 wake hung"). Mutant I4b: see the separate item below.
  - I5 `:464-545`: 20,000 fan-out waits at 1 and 4 workers with a thread-count sampler and depth peak 2; a 500-group chain on one worker (150 under ASan). Mutant
    "a wait creates a thread": killed (`:506`).
  - I6 `:687-723`: random DAGs, 50 groups, widths 0 to 3, 1, 2, 4 workers; mutants I1 and I3 (F1, F2) all killed. Full mutant table: `.scratch/rc2/mut.out`.
- **AL2 CLOSED.** `on_idle`/`cancel_idle` (`:1438-1462`). Lists are detached whole under `mu` (`take_idle`, `:841`), then fired unlocked (`fire_idle`, `:850`, `next` read before `fn`).
  Lifetime against group destruction: `~WorkGroup` cancels and waits until idle; every path that makes the group idle (`turn`, `cancel`) takes the list under the lock first, so a
  node linked at destruction fires before `wait()` can return idle, and `fire_idle` touches only the nodes (not `s`), so the state can be freed under it. Against pool shutdown:
  groups must be gone first, so no node outlives its group. Callback racing `cancel_idle`: `cancel_idle` returns true only when it unlinked the node, so a false means the
  list was already detached and `fn` has run or will run, exactly once. Probe `al2.cc` (20,000 iterations each: a thread calling `cancel_idle` against the firing; a group destroyed with a
  node linked on a busy group, then the node freed after `fn`; pool shutdown last): TSan and ASan+UBSan clean, never fired twice (cancelled 3 / 615, fired 19,997 / 19,385). The suite's AL2 test
  (`:575-683`) does not cover the destruction or the cancel race; worth adding `al2.cc`'s two loops. Note for X2: `~WorkGroup` does not wait for an `fn` that is mid-call, so a node must stay valid until its `fn` returns (the doc says `fn` may free it).
- **AR1 CLOSED.** `.scratch/arch/inversion` rebuilt against the head: `ok` at 2 workers (busy 0), 4 (0 and 2), 8 (0 and 6), all with `q_saw_lease=0`; TSan at 2 and 4 clean. The 1-worker run
  hangs in the probe's own spin (the ruling calls it a probe artefact; the test's 1-worker case drops the spin and passes).
- **AR2 CLOSED.** `q_saw_lease=0` in every inversion run and in I1 (`foreign == 0`): no item of another group runs on a thread inside a wait.
- **I4b: I disagree with the developer, the wake has a liveness effect.** The mutant (no wake when a lane becomes helpable, idle only) survives the suite, but a legal acyclic graph hangs
  without it. `.scratch/rc2/i4live.cc`: 3 workers, reserve 0, query class. G (width 1): g1 runs 300 ms, g2 queued. Y (width 1): y1 calls `G.wait()` (parks, G's width is held). W: b1 calls `G.wait()` (parks). Then Z: z2 calls
  `Y.wait()` (Y's width is held by y1). Z -> Y -> G, no cycle. g1's turn ends with g2 pending (budget spent), relists G's ticket, and its worker picks z2 and parks on Y: all three workers are parked, a ticket sits on the ring.
  The real code wakes y1 and b1 at turn end (`waiters_due`, `helpable`), one serves g2, everything finishes (3 of 3 runs `ok`). With the I4b mutant it hangs (3 of 3, the probe's 10 s watchdog, `g2_done=0`).
  The turn-ending worker is not always free to pick the ticket again, so the wake is needed. Ask: add `i4live` as an I4 test case (it kills I4b).
- **L2-B5 OPEN (new, from the lead; blocks landing).** `WorkGroup::cancel_and_wait()` (`work_pool.hh:1466-1473`) walks `cancel()`'s list only to count it; it never frees the boxed items, while `~WorkGroup`
  (`:1323-1331`) does. strata's probe `workgroup_cancel_and_wait_leak.cc` (copy `.scratch/rc2/b5.cc`) against the head, ASan+LSan: `queued live=5`, `dropped=5 live after cancel_and_wait=5 (0 expected if freed)`, then LeakSanitizer reports the
  boxed items (a direct leak of 48 bytes, indirect 192, from `enqueue`'s `new (std::nothrow)`). Not introduced here (the code is as at `threadpool:2ae90f0`) but `API.md` does not say the caller must free them and cannot: the call returns only a count.
  Fix: free the boxed items as the destructor does (`item_boxed` -> `box->destroy`), and document that caller-owned items are dropped unrun (or take `cancel()` first); add a test that counts live captures (strata's probe is the test).
- **L2-B6 OPEN (new, from the lead; blocks landing): the pool is not work-conserving for a group whose tickets were refused at post time.**
  Reproduced locally on the 16-CPU Mac at load 6 to 9: `.scratch/rc2/wc.cc` (4 workers, reserve 0, 64 items in one group, each holds its worker until 4 run at once or a 300 ms deadline, 300 repetitions):
  head `peak < 4` in 16, 19 and 15 of 300 (peak 3 every time, so one worker stayed away for the whole 300 ms with about 60 items queued); `threadpool:0244bd3`'s header 150 of 300 (49 to 64 percent); `threadpool:5caedce` 143 of 300. So it predates
  this branch's changes and the head is better, but not fixed. Mechanism (`wc_inst.cc`, counter on the refusal): `grant()` (`:795-815`) refuses an extra ticket when `spare(c)` is false, and `spare` is `num_unparked() < n` (`:781-790`), where a worker the
  chain has just woken (the searching worker, `Idle::from_searching` / `notify_one`) counts as unparked. While the main thread is still enqueuing, the chain wakes all four workers; the searchers find an empty ring (the extras were refused while they were counted),
  and park; nothing grants again because the only other grant points are `post()` (all 64 are posted) and the end of a turn (the running items are inside their turns). In the instrumented runs the refusals per repetition are 61 to 63 of 63 in every failing
  repetition, against 41 on average in passing ones. The extras are re-granted only when a turn ends (`turn`, `:945-952`), so an under-ticketed group runs narrower than its width for as long as its running items run. For the usual short item the gap
  closes in one quantum; for a long chore or merge it stays open for the item's duration (not work-conserving), and the rewritten `work_group.cc` case 1 (items that never end their turn) turns that gap into a 10 s failure, which is what CI shows on slow runners.
  Not a deadlock and not a race: a stall of parallelism with idle workers. Direction (the developer's call, not a prescription): make the refusal not depend on a transient count of woken-but-searching workers, or have a searcher that finds the ring empty re-check
  for groups with `pending > tickets` before parking. I did not run the CI log (not reachable here) or an enqueue-before-start variant (the pool starts its workers in the constructor; load did it).

### Other checks

- `quiet check` exit 2 (other light holds); all builds and probes ran under `quiet light`, at most 3 jobs.
- New findings beyond B5 and B6: none in the ruled-wait implementation. One-liner for the lead: the I3 test kills the F2 mutant about 90 percent of the time under load; I6 kills it reliably.

### Verdict

**Do not land.** L2-B2, L2-N1, AL1, AL2, AR1, AR2 are closed; L2-B5 (cancel_and_wait leaks boxed items) and L2-B6 (work-conservation gap that fails `work_group` case 1 on slow runners) are open, and I4b needs a test.

Not run: Linux, FreeBSD and arm64 builds; CI logs; any benchmark or K rerun; consumer rebuilds; the `stress.cc` probes of the first review (superseded by I1 to I6 and `deep.cc`); a revert check for B5/B6 (no fix exists); an enqueue-before-start reproduction of B6.

## Re-check 3

Re-checker, 2026-10-29. Subject: `threadpool:e28a89c` (B5, B6) and `threadpool:3e6e585` (ledger L2-14); worktree `l2-scheduler`, clean at `3e6e585`. Read `work_pool.hh` `grant`/`drop`/`turn`/`serve`/`helpable`
(`:778-960`), `cancel_items`, `cancel()`, `cancel_and_wait()`, `~WorkGroup`, `test/work_group.cc` cases 17 and 18, and the K data in `docs/status/phase7c-l2/k-b6/` and `k-ruled/`.
Probes in `.scratch/rc3/` (`b5.cc` is strata's probe unchanged, `c17.cc`/`rv2.py`, `p.cc` modes width/widthwait/fair/lost/herd, `ws_inst.cc`, `ctl.cc`, `wc` from `.scratch/rc2/wc.cc`).
Builds under `quiet light` (at most 3 jobs): TSan (`-O1`) and ASan+UBSan (Homebrew LLVM 23), both from `.scratch/rc2/san.sh`; all 7 CTest targets pass in each on this head.

### Verdicts

- **L2-B5 CLOSED.** `cancel()`, `cancel_and_wait()` and `~WorkGroup` all call `cancel_items()` (`work_pool.hh:1260-1306`), which frees boxed items unrun (`box->destroy`) before the group's waiters and idle nodes are
  told it is idle, and returns caller-owned ones through `next`. Strata's probe (`workgroup_cancel_and_wait_leak.cc`, byte-identical to `.scratch/rc2/b5.cc`) under ASan+UBSan with `detect_leaks=1`: `queued live=5`,
  `dropped=5 live after cancel_and_wait=0`, no LeakSanitizer report (Re-check 2 showed `live=5` and a leak). Revert check of case 17: `.scratch/rc3/rv2.py` runs case 17 alone (`c17.cc`, extracted from `work_group.cc:482-523`) against three copies of the
  header that each stop freeing boxes on one path: "cancel_and_wait keeps boxes" dies on `live == 0` (c17.cc:50), "cancel keeps boxes" dies on the returned-list order (:53), "destructor keeps boxes" dies on `live == 0` (:60); the unmutated control passes.
  (A first attempt ran the whole `work_group` binary under ASan on a loaded host and timed out at 300 s on the first mutant; that was the host, not the mutant, and is why the case is run alone.)
- **L2-B6 CLOSED.** The design is sound and I could not break it.
  - **Work conservation.** `.scratch/rc2/wc.cc` rebuilt (`-O2`), 14 CPU burners (load 10 to 12 on 16 CPUs), 80 repetitions at each of 1 to 8 workers (640 runs): `peak < workers` in 0 of 640; plus TSan at 2, 4 and 8 workers, 40 each (120): 0 of 120. Re-check 2 read 15 to 19 of 300 on the previous head at 4 workers.
    Mutant "relay removed" (`bool relay = false && first && grant(s, c);`, `.scratch/rc3/mrelay/`): 98 of 100 at 4 workers and 100 of 100 at 8 fail, so the pop-then-list rule is what carries the property and `wc` exercises it.
  - **Width.** By construction: `grant` lists only when `tickets + helpers < cap(s)` and `listed == 0` (`:782-789`), the relay is the same call, and a pop moves a ticket from listed to running without changing `tickets`, so `tickets + helpers <= cap` always. Measured (`p.cc width`, 165 runs: 2/4/8 workers x widths 1,2,3,4,8, 64 items): peak above width 0, below width 0 (plain and TSan).
    `p.cc widthwait` (120 runs, widths 1 to 3: four worker waiters plus a non-worker waiter plus the owner all helping): over width 0, plain and TSan.
  - **Fairness.** `p.cc fair` (4 workers, two saturated groups of self-reposting 50 us items, 3 s): weights 1:1 share 0.998, weights 1:3 share 2.979 (expect 3). The DRR path (`budget`, `deficit`) is unchanged by `e28a89c`; `work_scheduling` passes under both sanitizers. K2's 1.04 is the ledger's number; not rerun.
  - **No lost wakeup.** `p.cc lost` (a post races the end of a turn on its last item): external serial post-then-wait 20,000, an item that posts its successor from inside itself 20,000, two posters 40,000, each at 1/2/4 workers and widths 0 and 1 (6 configs), plain 20,000 iterations and TSan 3,000: all items ran, no watchdog hang.
    By reading: `post` and the end of `turn` take `s.mu`, so a post either lands before the turn's final `pop` (the turn takes it) or after the turn's `drop` (the post sees `tickets` lowered and lists). The relay's `release` pushes after `unlock`, but the group is held alive by `tickets` and `running`.
  - **No herd.** One `grant` per `pop` (the relay) plus one relist at a turn's end, and `listed <= 1` per lane, so a ring holds at most one entry per lane and `reserve(states + 1)` is enough. Counters in an instrumented copy (`.scratch/rc3/inst2`, `p_cnt herd`):
    one post to 8 parked workers wakes 2 (the post and the Tokio ramp wake), 8 long items on 8 workers wake 8, `pushes == serves` in every row, and `wakes < pushes` throughout (e.g. 64 long items, 8 workers: 4,657 wakes for 15,974 pushes).
    The high `serves` for long items (about 250 per item) is DRR debt being paid by empty turns; it is identical on the previous head (`threadpool:2947834`: 16,400 against 16,301 here for 64 x 20 ms items on 4 workers), so not new.
  - **I1 to I6.** `.scratch/mutl2.py` rerun against this head: all 15 mutants killed (I1, I1b, I2a, I2b, I3a, I3b, I3c on `peak == 1` this time, I3d, I4, I4b on the new "I4 liveness" case, I5, AL2a to AL2d). TSan and ASan suites pass.
- **K3 at reserve 2: noise or environment, not a B6 regression; no change needed, watch it at the gate.** The k-b6 reserve-2 p99 is higher than `k-ruled` in all three scenarios (hot group 151 against 61 us medians, busy 1,000 184 against 84, idle 10,000 139 against 55), ranges disjoint in two of them, so it is real in those runs.
  It is not B6: (1) reserve 1 (`workpool`, the default) is unchanged run to run (hot 165 against 167, busy 179 against 158, idle 169 against 147 us), while only the reserve-2 rows moved; (2) in `.scratch/kab/` (old head and B6 interleaved, 6 samples each) the old head's reserve-2 p99 reads 134 us hot and 146 busy,
  so the old binary no longer reproduces its own 61 and 84 on this host state; B6 reads 155 and 163; an exact permutation test on the medians gives p = 0.78 (hot) and 0.39 (busy), reserve 1 p = 0.99 and 0.31; (3) `k-ruled` was taken `exclusive` on macOS and `k-b6` under `strict`, which on macOS cannot pin cores. A B6 effect of up to about +15 to +20 us at reserve 2 cannot be excluded with 6 samples; it is within the spread (55 to 204 us) of a single configuration.
  Mechanism check: B6 changes nothing for a query lane with one item per group; reserve is applied in `pick`/`ready_for_me`, which `e28a89c` did not touch.
  Not rerun, per the brief.

### Item 4 (CI): linux-tsan `work_scheduling.cc:150 assert(!in_time)`. Not a priority bug. The control is timing-dependent and fails on the previous head too; it needs a deterministic replacement, but it is not B6's.

- Reproduced locally under TSan with 14 CPU burners: new head 2 of 20, 6 of 40 (instrumented copy), 7 of 40; previous head `threadpool:2947834` (before B6) 5 of 40 and 3 of 30, all `Assertion failed: (!in_time)` at the same line. Without load, 0 of 30.
- Mechanism (`.scratch/rc3/ws_inst.cc` with a hook at the merge pick): `pick()` takes a merge without aging only after `_ready[flush].pop()` returned null (`work_pool.hh:1046-1050`; `merge_overdue()` is false with `merge_age = 0`). At those moments the flush ring held 30 entries (`size()` 30) and every pending flood group was listed
  (`pending-unlisted=0`, 30 to 32 groups pending, 1 to 3 running), so the ring was not empty: `Ring::pop` returned null because the head cell's producer had claimed the slot (`enq` CAS) and not yet published `seq` (`:566`, "the next entry is still being published"). A producer descheduled in that window makes every consumer see an empty ring.
  The control floods with self-reposting items, so pushes are continuous and one is in flight almost always; on a starved runner the window is wide enough to hit. B6 does not cause it (previous head fails equally), though B6 pushes somewhat more often (a post lists whenever `listed == 0`, including a self-repost during its own turn), which is why it can read slightly higher.
- Verdict on the priority question: a merge can run ahead of listed flushes only while a flush push is mid-publish, which is a property of the Vyukov ring and bounded by that producer resuming; no path lets a merge jump over a pop that returned an entry. It does mean "flush before merge" is strict only for published entries; say so in `docs/mechanisms/work-pool.md` or close the window if strictness matters (pop could retry while `enq > deq`, at the cost of spinning on a descheduled producer; I would not).
- Deterministic replacement for the control (probe `.scratch/rc3/ctl.cc`): 32 flush groups each post one item that waits on a gate, wait until 4 have started, post the merge, open the gate. No flush item reposts, so no push is in flight when the workers pick. Assert the merge starts after all but at most `threads - 1` flushes (a flush can be popped and not yet counted; the probe's strict "merge is 33rd" failed 2 of 300 and 1 of 1,500 for exactly that reason, `merge_at=32`), and `merges_aged() == 0`. A broken priority would put the merge near 5th.
  The aging-on half (`in_time` true, `waited >= 15ms`, `merges_aged() >= 1`) is a positive wait with a 400 ms bound and is fine.

### New findings

- None against `e28a89c`/`3e6e585`. One line for the lead: `work_scheduling.cc` case 1's control (`:150`) needs the replacement above; it fails about 10 to 15 percent of the time on a starved host on the head before B6 as well.

### Verdict

**Land** after the case-1 control is replaced (or marked as not blocking: it is a test flake with a known cause, not a scheduler defect). L2-B5 and L2-B6 are CLOSED; the K3 reserve-2 reading is environment/noise; I1 to I6 hold.

Not run: Linux, FreeBSD and arm64 builds; the CI logs themselves (the failure reproduced locally instead); any benchmark or K rerun; consumer rebuilds (strata, Xapiand); LSan on the full suite (only on strata's probe); an enqueue-before-start variant of B6.
