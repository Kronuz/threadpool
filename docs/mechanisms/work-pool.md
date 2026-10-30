> Status: CURRENT (2026-10-29) `WorkPool` as built on `phase7c/l2-scheduler`; the design is the research note `docs/research/scheduler.md` (R1 to R3) and Xapiand `docs/design/phase7c.md` D4.

# WorkPool

One set of named workers serves many owners. Each owner (an index, a restore, an upload) is a
`WorkGroup`; its work is caller-owned `WorkItem`s, so a post allocates nothing. Groups take
fair turns, queries run ahead of flushes and merges, merges cannot be starved, and an idle
pool uses no CPU.

## How it works

```
 post(item) --> group lane[class] (FIFO, group's spinlock)
                  | first ticket free; more while idle workers exist
                  v
        ready ring[class] (MPMC ring of tickets)  <-- query, flush, merge
                  |  pick(): query, [overdue merge], flush, merge
                  v
        worker: turn = quantum x weight of service, then requeue the ticket or drop it
```

- **Lanes and rings.** A group has a lane per class. A lane with queued work holds a ticket,
  its place on that class's ring; a worker pops a ticket, serves the lane for a turn (100 us of
  service by default, measured with the cycle counter), and pushes the ticket back at the tail
  if work remains. Debt from an overrun is carried (deficit round robin). Extra tickets go to
  a lane only while workers idle, at most one per worker per class, and retire when other
  groups wait, so one group alone takes every worker and many busy groups converge on equal
  shares. Rings are chained and grow when a group opens, so a post never blocks.
- **Classes and the reserve.** `pick()` takes the query ring first. A flush or merge turn
  holds one of `threads - reserve` background slots (an RMW before the pop, so the cap is
  exact), merges also one of `merge_cap`. The reserve workers wait for queries only: a query
  waits for a wake, not for an item to end. The reserve stays 1 because the host posts
  read-your-writes refreshes, opens and blob reads at `query`; a host that posts nothing there
  sets `reserve = 0`.
- **Merge aging.** A merge ring entry is stamped when it is pushed. When the oldest has
  waited longer than `merge_age` (100 ms), a background pick takes it before the flush ring,
  under the same slots, so every overdue entry is promoted in turn and a flood of flushes
  cannot starve merges, while young merges still yield to flushes. `merge_waiting()`,
  `merge_oldest()` and `merges_aged()` report the backlog. Flushes do not age against
  queries.
- **Parse-ahead and other bulk work get their own group.** Groups take turns; one group's lane
  is a FIFO. A restore's parse-ahead in the index's own group would delay the index's refresh
  behind the whole backlog, in its own group it waits one turn.
- **Waiting.** `WorkGroup::wait()` on a worker serves the group (inline from one of its items,
  else as a helper within the width) and other groups' queries; off the pool it parks on a
  futex. The group is quiet when nothing is queued and every running item is itself waiting in
  `wait()` (`GroupState::suspended`), so two items of one group that wait together, or a wait
  nested in an item run inline, end instead of waiting on each other. A thread that is not a
  worker first calls `Options::block_guard(site)` (also `help_until`), where a host aborts for
  an event loop: a loop that waits for a pool whose work needs the loop waits for itself.
- **Idle.** Tokio's protocol over one untimed futex per worker (`futex(2)`,
  `os_sync_wait_on_address`, `_umtx_op`): a post wakes a sleeper only when nobody searches, the
  last searcher wakes the next, the last to park checks the rings again.
- **Lifetime.** A group's state outlives its `WorkGroup` until its last ticket leaves the
  ring; the destructor cancels, frees queued boxed items and waits for running ones.

## Why this mechanism

Measured in the research note: per-group FIFOs with deficit round robin are the only candidate
that is fair at 1,000 and 10,000 groups (Jain's index 0.99) while one group alone still takes
every core; per-worker deques steal fast but give no fairness or classes. A futex per worker
with Tokio's wake rule cut an external post from 7.0 us to under 150 ns and idle CPU from 0.4%
to 0.001% of a core. Aging is stamped per entry and not per pool: a pool-level "no merge served
for T" rule serves one entry per interval and leaves a backlog of overdue merges waiting.

## Invariants

- Every ring entry is one listed ticket of one lane; a lane with queued work holds one (the
  churn and ping-pong cases in `test/work_group.cc`, `test/work_pool.cc`).
- Background turns never exceed `threads - reserve`, merges never `merge_cap`, aged ones
  included (`test/work_scheduling.cc` cases 2 and 3; mutants that bypass either die).
- A merge older than `merge_age` runs under a flood of flushes (case 1, with its control).
- `wait()` ends when only waiters run (cases 6 and 6b); a non-worker's `wait()` and
  `help_until()` reach the guard before blocking (case 7).

## Numbers

K1 to K3 reruns, the sanitizer runs and the consumer builds are in
`docs/status/phase7c-l2.md`; the gate rows are measured on `main` by the lead.

## Code

`work_pool.hh`: `ReadyRing` (stamps), `WorkPool::pick`, `merge_overdue`, `WorkGroup::wait`.
Tests: `test/work_pool.cc`, `test/work_group.cc`, `test/work_scheduling.cc`. Reviews: none yet.
