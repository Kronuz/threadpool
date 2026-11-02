> Status: CURRENT (2026-11-02) workstream: one pool for the Kronuz libraries (developer).

# unify-pool

- Task: `WorkPool` is the only pool; the scheduler library moves onto it, then `ThreadPool` and its family are deleted here.
- Branch: `phase7c/unify-pool`, from main `threadpool:65bc0bc`. Scheduler branch: `phase7c/unify-pool` in the scheduler repo.
- State: step 1 (additive `WorkPool` features) done; step 2 (scheduler) in its repo's ledger.

## Step 1: what the scheduler needed

| Need | `WorkPool` today | Change |
|---|---|---|
| Per-pool thread names | `Options::name_format` | none |
| All workers run tasks | `Options::reserve = 0` | none |
| Thread policy | `ThreadPolicyType` is ignored at run time by `setup_thread`/`run_thread`; `WorkPoolWorker` is `regular` | none (the scheduler's `Thread` keeps its policy parameter) |
| Delayed or timed post | the scheduler's wheel times; the pool only runs | none |
| `size()` and `running_size()` for Xapiand's metrics | not available per group | `WorkGroup::pending()` and `running()` (new) |
| Bounded `join()` | `on_idle` + a condition variable in the scheduler | none |

## Done

- `WorkGroup::pending()` / `running()` with `test/work_counts.cc`; mutants in `test/mutants.sh`:
  "pending() counts only the query lane" and "running() reports the queued count" both fail the test
  (`work_counts.cc:42`, `:41`).

## Next

Steps 3 to 5 after the scheduler review: delete the `ThreadPool` family, repin consumers, note for Detent.

## Blocked on

Nothing.
