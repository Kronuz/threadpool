# One scheduler for tens of thousands of indexes

- Date: 2026-10-18. Branch `research/scheduler` (worktree `~/code/Kronuz/threadpool-wt/research`).
- Ledger: `docs/status/research-scheduler.md`. Prototypes: `research/` (built only with
  `-DTHREADPOOL_BUILD_RESEARCH=ON`; nothing in the library changes).
- Machines: the Mac (Intel i9-9980HK, 8 cores / 16 threads, macOS 15, Apple clang 17) and
  strata's gate VM (Ubuntu 24.04 under QEMU/HVF, 8 vCPUs of the same host).
- Raw outputs: `docs/research/scheduler/*.txt`.

## 1. The problem

strata's plan (`docs/PLAN.md`, "The workload is many indexes") wants one thread pool per
host, shared by every open index (tens of thousands open, thousands to millions on disk), with
no thread setting: one index alone on an idle laptop uses every core; 1,000 busy indexes each
get about one. The pool must also stay out of the way of the reactor's event loops, which
post latency-critical query pieces into it.

Per index the work is: flush partitions (tens to hundreds of tasks of 0.1 to 10 ms), merges
(long, background, sliced by strata's budget), query fan-out pieces (microseconds, latency
critical), and parse-ahead batches. Across a host: up to tens of thousands of groups, most
idle, a few hot. Needs: fairness across groups, priority (queries over flushes over merges),
cache locality (an index's tasks on the core that has its data), cheap wakeups, no allocation
per task, nested waits that cannot deadlock, and about zero CPU when idle.

### Today

`Kronuz/threadpool` (`threadpool.hh`, `task_group.hh`): one `std::deque` behind a mutex and a
condition variable, `std::function` tasks, workers that wake every 100 ms to re-check
shutdown, and `TaskGroup`: a per-group FIFO served round robin through "tickets" on the pool
queue (one ticket per running or queued task up to the group's cap). Each `TaskGroup::enqueue`
boxes the callable (`make_unique<std::function>`), posts a ticket lambda to the pool queue
(another `std::function`), and the pool's deque node is a third allocation. strata binds it
through `include/strata/pool_executor.hh` (one `TaskGroup` per index, `per_group = 1` for
merges); bisection (`phase3/bisection-pool`) runs its halves on it with a rule that the caller
never waits for a helper that has not started. reactor offloads blocking work to an
`asio::thread_pool` of its own (`reactor.h`, `Reactor::pool_`), so a host today runs two
pools that know nothing of each other.

What this design cannot do, whatever its tuning: give a query piece a bounded wait on a
saturated pool (every worker may be inside a 10 ms flush task, and the pool has no notion of
class); keep one index's tasks on one core (the shared queue scatters them); or make the
single-index-on-an-idle-host case use every core while 1,000 busy indexes share equally
(tickets are per task, so the group with the most pending tasks takes the most workers).

## 2. The bound

What the hardware and the kernels charge for the pieces a scheduler is made of, measured by
`research_bound` (single-threaded except the two-thread handoffs; light hold).

(Measured below; see "Results".)

## 3. Survey

Read freely (all permissive licenses: Tokio MIT, Go BSD, oneTBB Apache-2.0, Taskflow MIT, folly
Apache-2.0, Rayon MIT/Apache-2.0, Seastar Apache-2.0, Eigen MPL-2.0, Linux documentation);
nothing is ported, so no notices travel.

### 3.1 The local queue: Chase-Lev and its memory orders

Chase and Lev's deque (SPAA 2005) is the standard: the owner pushes and pops at the bottom with
no CAS, thieves take from the top with one CAS, and the owner needs a CAS only for the last
element. Lê, Pop, Cohen, Zappa Nardelli, "Correct and Efficient Work-Stealing for Weak Memory
Models" (PPoPP 2013, `research/.scratch/papers`) prove the C11 version and give the minimal
orders, which `research/queues.hh` uses verbatim: `push` loads `bottom` relaxed and `top`
acquire, stores the slot relaxed, then a release fence before the relaxed store of
`bottom + 1`; `take` stores `bottom - 1` relaxed, a seq_cst fence, loads `top` relaxed, and
races with a seq_cst CAS on `top` only when `t == b`; `steal` loads `top` acquire, a seq_cst
fence, `bottom` acquire, then the CAS. Their measurement: the hand-fenced version keeps at
least 1.5× the throughput of the all-seq_cst version on both x86 and ARM, and the first
serializing instruction on the owner's path is what costs on x86 (Table 2: 4.88 × 10^8 against
3.66 × 10^8 push+take per second on a 4-thread Core i7).

Tokio (`tokio/src/runtime/scheduler/multi_thread/queue.rs`) keeps a fixed ring of
`LOCAL_QUEUE_CAPACITY = 256`, packs a real head and a steal head into one atomic (so a thief
claims a batch with one CAS and signals completion with a second), steals half rounded up
(`n - n / 2`), and on overflow moves the newest half (`NUM_TASKS_TAKEN = 128`, plus the
incoming task) to the inject queue so tasks taken from it are not pushed straight back. The
2019 redesign ("Making the Tokio scheduler 10x faster") reports `chained_spawn` 2,019,796 →
168,854 ns and `ping_pong` 1,279,948 → 562,659 ns per iteration against the crossbeam deque,
and the Hyper hello-world at 113,923 → 152,258 requests/s (+34%).

oneTBB (`src/tbb/arena_slot.h`) does not use Chase-Lev at all: each slot has an array task
pool published through an atomic pointer and locked (`acquire_task_pool`, a backoff loop)
when the owner and a thief may touch the same end. Taskflow (`taskflow/core/wsq.hpp`) is the
PPoPP 2013 code with a growable array (`UnboundedWSQ`), plus a `BoundedWSQ`.

### 3.2 Sleeping and waking: the protocols

Tokio (`idle.rs`, `worker.rs`): one word holds the number of searching workers (low 16 bits)
and of unparked workers. A post notifies one parked worker only when **no** worker is
searching (`notify_should_wakeup`: zero searchers and fewer unparked than workers); a worker
may enter searching only while `2 * num_searching < num_workers`; the last searcher that
finds work must wake another ("gradual ramp-up"); the last searcher that parks does a final
check of the inject queue first. All with SeqCst RMWs, which the code comment explains: the
RMW "is what makes the caller's preceding inject-queue push visible to a parking worker's
subsequent unlocked queue-emptiness check". Other knobs: the LIFO slot runs at most
`MAX_LIFO_POLLS_PER_TICK = 3` tasks in a row ("picked out of thin-air"), the inject queue is
checked first every `global_queue_interval` ticks (self-tuned to about 200 µs of task time,
clamped to [2, 127]; the fixed default was 61, Go's number), and `event_interval` (61) says
how often the driver is polled with a zero-duration park.

Go (`src/runtime/proc.go`): `findRunnable` checks the global queue when
`pp.schedtick%61 == 0` ("to ensure fairness. Otherwise two goroutines can completely occupy
the local runqueue by constantly respawning each other"), steals in `stealTries = 4` passes
over a random permutation of Ps, taking `runnext` only on the last pass ("stealing from the
other P's runnext should be the last resort"), and limits spinning Ms to half the busy Ps:
`2*sched.nmspinning.Load() < gomaxprocs-sched.npidle.Load()`. `wakep` starts a spinning M
only if none exists. The `runnext` slot inherits the current time slice (so a ping-pong pair
cannot starve others without `sysmon` preempting them). The last M to go idle blocks in
`netpoll(delay)`, so the network poller needs no thread of its own.

Rayon (`rayon-core/src/sleep`): a worker yields for `ROUNDS_UNTIL_SLEEPY = 32` rounds, then
announces itself sleepy (reads the jobs event counter), searches once more, and sleeps on its
own condvar at round 33 unless the counter changed. A new job wakes
`min(num_jobs, num_sleepers)` threads when the queue was not empty, or
`min(num_jobs - num_awake_but_idle, num_sleepers)` when it was. Two seq_cst fences and a last
look at the injector close the lost-wakeup window.

Eigen's `EventCount` (MPL-2.0; Taskflow ships it as `NonblockingNotifier`): a 64-bit word with
a waiter stack (14 bits), a pre-wait count, a pending-signal count, and an ABA epoch (22 bits);
`Prewait` increments the count with seq_cst, the waiter re-checks its predicate, `CommitWait`
either consumes a pending signal or pushes itself and parks; `Notify` fences seq_cst and
leaves a signal for a pre-waiter or pops one waiter. Dekker-style: the notifier's
`predicate = true; fence; read state` against the waiter's `count++ (seq_cst); re-check`, so
one side always sees the other. Taskflow 3.x replaced it with `AtomicNotifier`: a 32-bit
epoch plus a waiter count, and C++20 `atomic::wait` on the word ("starting at v3.5 we do not
use any complicated notification mechanism"); workers try `MAX_STEALS = 2 * (victims + 1)`
steals, then yield, and give up after `150 + MAX_STEALS` attempts, with a two-phase guard
(`prepare_wait`, re-check every queue, `commit_wait`) before sleeping.

folly: `LifoSem` wakes waiters in LIFO order ("to maximize performance rather than
fairness"): the most recently blocked thread has the warmest cache and the others stay
asleep; its state is one 64-bit word (value or waiter-node index, flags, a sequence number
against ABA) over an `IndexedMemPool` of 16-byte nodes. `ThrottledLifoSem` adds
`wakeUpInterval`: a woken waiter waits that long before the next waiter is woken, so a burst
of short tasks is drained by the threads already awake instead of waking the pool ("it may be
beneficial to have a single thread process all the tasks sequentially"); default 0.
`CPUThreadPoolExecutor` serves priorities strictly, one queue per priority, and documents the
consequence: "a run of long low-priority tasks can still occupy every worker".

### 3.3 Fairness across groups and classes

Seastar (`include/seastar/core/scheduling.hh`, `src/core/reactor.cc`): shared-nothing, one
reactor per core, and within it `scheduling_group`s with `shares` (1 to 1000). Each group's
task queue carries a virtual runtime, `_vruntime += runtime * (2^32 / shares) >> 32`, the
active queues are kept sorted by vruntime (`insert_active_entity`), the front runs for a task
quota (`--task-quota-ms`, default 0.5 ms), and a queue that wakes from idle has its vruntime
clamped to `max(_last_vruntime, _vruntime)` "so it doesn't dominate scheduling for a long
time" (no credit accrues while idle). The number of groups is a compile-time constant,
`Seastar_SCHEDULING_GROUPS_COUNT = 16`: Seastar does proportional sharing between a handful of
classes, never between thousands of tenants.

Linux EEVDF (kernel 6.6+, Stoica and Abdel-Wahab 1995): every runnable entity has a lag
(service owed, weight-scaled); only entities with lag ≥ 0 are eligible; each has a virtual
deadline = eligible time + slice / weight; the eligible entity with the earliest deadline
runs. A shorter slice gives earlier deadlines, so a latency-sensitive entity runs sooner
without receiving more CPU over time. That separation of **share** (weight) from **latency**
(slice) is exactly what a merge-versus-query policy needs, and it costs an ordered set (a tree
or heap) of eligible entities.

Deficit round robin (Shreedhar and Varghese, SIGCOMM 1995): each flow gets a quantum per
round and a deficit counter; a flow is served while its deficit covers the next packet; the
leftover carries; an idle flow's deficit resets. O(1) per packet when the quantum is at
least the largest packet. Stride scheduling (Waldspurger and Weihl, 1995): each client has
`stride = stride1 / tickets` and a `pass`; the client with the smallest pass runs and
advances by its stride; O(log n) with a heap, and a client that was idle re-enters at the
global pass (the same no-burst rule as Seastar's clamp).

oneTBB: a `task_arena` has a concurrency limit and reserved slots, and the market assigns
workers to arenas by demand, so an arena is the natural "group with a worker budget"; arenas
carry one of 3 priority levels (low, normal, high); affinity is a per-slot mailbox a task can
be addressed to, and `execution_data` records `original_slot` and `affinity_slot`. Workers
leave an arena when it has no work (`FAST_LEAVE` by default; `DELAYED_LEAVE` keeps them
around on non-hybrid CPUs).

### 3.4 Locality

Seastar's answer is total: a core owns its data and nothing migrates; cross-core work is a
message. Tokio's LIFO slot and Go's `runnext` run the task a running task just made runnable
while its data is in cache (Tokio's message-passing benchmark halved). TBB's mailboxes send a
task to the slot that touched its data last. Acar, Charguéraud, Rainey, "Scheduling Parallel
Programs by Work Stealing with Private Deques" (PPoPP 2013) go further: no concurrent deque at
all; each worker's deque is private, and thieves send steal requests the owner answers between
tasks, which costs a polling point in the owner and a response delay of one task.

### 3.5 Completion sources

Linux: `IORING_SETUP_SINGLE_ISSUER` + `IORING_SETUP_DEFER_TASKRUN` (6.1+) make a ring per
thread whose completions are processed only when that thread waits, with no IPIs
(`IORING_SETUP_COOP_TASKRUN` avoids the forced reschedule); `io_uring_prep_msg_ring` (5.18+)
posts a CQE into another thread's ring, which wakes it if it is waiting, so a worker whose
only sleep point is `io_uring_enter` can be woken by a message instead of a futex;
`IORING_OP_FUTEX_WAIT` / `FUTEX_WAKE` (6.7) put futexes on the ring too. macOS and FreeBSD
have `kqueue` with `EVFILT_USER` for the same user-triggered wakeup. The wake cost is the
same syscall-plus-schedule floor as a futex (measured in section 2); what the ring buys is
one sleep point for I/O completions and tasks, which is reactor's concern, not the pool's.

### 3.6 What the survey says about this problem

1. The idle protocol is solved: Tokio's (notify one when nobody searches; at most half the
   workers search; the last searcher wakes the next; a final check before parking) with a
   futex per worker. Rayon and Taskflow spend 32 to 150 rounds of yielding before sleeping,
   which is CPU when idle; Tokio and Go spend none.
2. Nobody does per-group fairness at ten thousand groups: Seastar has 16 groups per core,
   Tokio, Go, Rayon, and Taskflow have no groups, TBB arenas are a few, folly's priorities are
   strict. The per-group ready-list below is a composition of DRR (per-group quanta of
   service time) with Seastar's and stride's no-credit-while-idle rule.
3. For tasks of 0.1 to 10 ms the queue's cost is noise (50 against 500 ns per task is 0.005%
   against 0.05% at 1 ms); what decides the latency tail is **who can run a query when every
   worker is busy**, so the design question is a reservation or preemption point, not a
   deque. For microsecond query pieces the deque and the LIFO slot do matter.
4. Locality for an index comes for free from serving a group for a quantum (one worker runs
   many of its tasks in a row), or from a home core, and costs nothing per task in either.

## 4. The designs

All four share: caller-owned task records (a function pointer, the group, an intrusive link,
a class byte; no allocation), the Tokio idle protocol over one futex per worker
(`research/idle.hh`, `research/futex.hh`), and a stop flag. They differ in where a task goes
and who may take it.

### D-A. Per-worker Chase-Lev deques with stealing (`StealPool`)

Tokio's shape: a worker posts to its own deque (through a LIFO slot, at most 3 in a row),
external threads post to a bounded MPMC inject ring (Vyukov's), workers check the inject ring
every 61 tasks and when idle, and steal half from a random peer when their own deque is empty.
Groups are tags. What it gives: the lowest per-task cost and the best microsecond-task
locality. What it cannot give: fairness (a group with 1,000 queued tasks is served before a
group with one, in some order), class priority, or an index's tasks on one core.

### D-B. Per-group FIFOs, a ready-list, deficit round robin (`GroupPool`)

Each group is an intrusive FIFO under a spinlock plus a deficit counter; a group with work is
on a global ready-list (an MPMC ring of group pointers) once per **ticket**. A worker pops a
group, adds a quantum of service time (100 µs) to its deficit, runs its tasks until the
deficit is spent or the group is empty, and re-appends the group at the tail if work remains
(the deficit carries, so a group whose 10 ms task overran skips rounds until it is even; an
idle group's deficit resets to zero, Seastar's clamp). Tickets implement the plan's rule
directly: a group's first task always earns a ticket; more tickets (up to its width) are
granted only while workers are idle, one per quantum, so an index alone doubles its share of
cores every quantum (16 cores in four quanta, 400 µs) and 1,000 busy indexes hold one ticket
each and share equally. Locality: a worker runs one group for a quantum at a time.

### D-C. Priority classes on D-B with a reservation (`PrioPool`)

Three ready-lists (query, flush, merge) picked in order. Strict priority alone leaves the
folly problem (long background tasks on every worker), so the pool reserves `R` workers that
refuse background work when `N - R` are already in it, and caps the workers merges may hold.
A query then waits for a wake (tens of µs), not for a flush task to end on some core. The
cost is `R` idle cores while no queries arrive; the alternative, EEVDF-style weights with a
short slice for queries, cannot preempt a running task either, so a reservation (or shorter
background tasks) is the only lever against the tail. Aging for starved flushes is a design
note, not prototyped.

### D-D. Groups pinned to a home worker (`HomePool`): the assumption breaker

The assumption everyone else makes is that a task's placement is the poster's business (own
deque or a global queue). Here a group has a **home** worker, and every task of the group goes
to the home's inbox (an MPMC ring) whoever posts it, so an index's flushes, merges, and query
pieces run on the core that has its data, in order, with no stealing in the common path.
Workers steal only when idle, half of the fullest inbox, and may re-home the stolen group to
themselves (so 1,000 busy groups redistribute by load, and one hot group's tasks spread across
idle cores). Fairness between groups on one home is FIFO by arrival, not round robin.

### Not prototyped, recorded

- **Help-first inline execution on saturation**: a poster that finds every worker busy runs
  the task itself. It is what bisection does by hand and the natural answer to nested waits;
  it belongs in the API (a `try_post` that returns false) rather than in the scheduler.
- **io_uring as the worker's sleep point**: Linux only, and the win is reactor's (one sleep
  point for I/O and tasks), so it is noted for reactor's design, not for the pool.
- **A full EEVDF over groups**: a heap of eligible groups keyed by virtual deadline gives
  weighted shares and latency-by-slice in one mechanism, at O(log G) per pick. With a
  ready-list of equal quanta the same effect at O(1) is DRR; weights would be class-based
  quanta. Worth building only if D-B's equal quanta prove insufficient.

## 5. Results

(Filled from `docs/research/scheduler/*.txt`.)

## 6. Recommendations

(Written from the results.)

## 7. Reproduce

```sh
cmake -S . -B .scratch/build -DCMAKE_BUILD_TYPE=Release -DTHREADPOOL_BUILD_RESEARCH=ON
cmake --build .scratch/build -j4
.scratch/build/research/research_bound                       # the floors (light hold)
.scratch/build/research/research_workloads tiny pools=baseline,raw,steal,groups,prio,home tasks=2000000 groups=1000
.scratch/build/research/research_workloads tail groups=1000 merges=4 seconds=6
.scratch/build/research/research_workloads makespan groups=1000
.scratch/build/research/research_workloads idle seconds=3
.scratch/build/research/research_workloads memory groups=10000
```
