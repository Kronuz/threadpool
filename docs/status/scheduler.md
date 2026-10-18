# scheduler

- Design: the research note `docs/research/scheduler.md` on `research/scheduler`
  (`~/code/Kronuz/threadpool-wt/research`), section 6: R1, R2, R3 and the additive API
  sketch. Reviews: none yet.
- Branch / worktree: `feature/scheduler` at `~/code/Kronuz/threadpool-wt/scheduler`, from
  `origin/main` at `628679a`.
- State: implementing R1
- Done criteria (set by the lead; contributors tick, never edit):
  - [ ] K1. R1 is in. Idle workers use no measurable CPU. Tiny-task throughput is at least
    today's pool and at least the research prototype's, within noise (interleaved A/B, five
    or more rounds, minutes long).
  - [ ] K2. R2 is in. One group alone gets the whole pool. Beside 1,000 busy groups, each
    group's share is fair within the note's bound.
  - [ ] K3. R3 is in. A query-class piece's p99 on a saturated pool matches the note's
    prototype result.
  - [ ] K4. Strictly additive. Every existing API and test is unchanged, and reactor and
    every Kronuz library that links threadpool still builds and passes (check the
    cpp-libraries index for consumers). Detent links these libraries: nothing it calls
    changes behavior.
  - [ ] K5. TSan and ASan+UBSan clean. CI green on all jobs.
  - [ ] K6. FreeBSD: no kernel-time regression against today's pool. A condvar handoff
    cost about 3x kernel time on FreeBSD 14.4 in strata, so measure CPU and sys time there.
  - [ ] K7. The README and notes document the new API in the repo's style. Comments stay
    short.
- Done: nothing yet.
- Doing: R1, `work_pool.hh`: the futex layer, Tokio's idle protocol, `WorkItem`, `WorkPool`.
- Next:
  1. R1 tests, CI green; A/B against today's pool and the prototype.
  2. R2: `WorkGroup`, per-group lanes on a ready ring, deficit round robin.
  3. R3: classes, the reserve, the merge cap.
- Blocked on: nothing.
- Findings folded in: none.
- Consumers (for K4): `scheduler` (and through it `logger`, `http-log`, `prism`, Xapiand),
  `strata`; reactor does not link threadpool (it runs its own `asio::thread_pool`).
- Last verified: not yet built (a strict host hold, `si-m2b2`, at the start).
