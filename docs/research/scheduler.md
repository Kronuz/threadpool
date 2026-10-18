# One scheduler for tens of thousands of indexes

- Date: 2026-10-18. Branch `research/scheduler` (worktree `~/code/Kronuz/threadpool-wt/research`).
- Ledger: `docs/status/research-scheduler.md`. Prototypes: `research/` (built only with
  `-DTHREADPOOL_BUILD_RESEARCH=ON`; nothing in the library changes).
- Machines: the Mac (Intel i9-9980HK, 8 cores / 16 threads, macOS 15, Apple clang 17) and
  strata's gate VM (Ubuntu 24.04 under QEMU/HVF on the same host, 8 vCPUs without SMT,
  g++ 13.3). Other agents ran single-threaded jobs on the VM throughout (light holds), so the
  Mac's pool had 70 to 88% of its 16 hardware threads; trials are interleaved per pool and
  the tables show medians of three, so that load shifts every pool alike.
- Raw outputs: `docs/research/scheduler/*-mac.txt` and `*-vm.txt`;
  `research/summarize.py FILE...` prints the median tables below.

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

What this design cannot do, whatever its tuning (section 5 has the numbers): give a query
piece a bounded wait on a saturated pool (at 1,000 busy groups a query's ticket waits a whole
round, 70 ms); keep one index's tasks on one core (the shared queue scatters them); keep a
bursting index from taking 13 to 16 times its share (tickets are per task); or wake cheaply
(a condvar wake per enqueue costs 7 µs of wall and 20 µs of CPU per tiny task, and the idle
pool burns 0.35 to 0.47% of a core on 100 ms timeouts).

## 2. The bound

What the hardware and the kernels charge for the pieces a scheduler is made of
(`research_bound`, light hold; `bound-mac.txt`, `bound-vm.txt`).

| Primitive | Mac (macOS 15, x86_64) | gate VM (Linux 6.8, QEMU/HVF) |
|---|---|---|
| `steady_clock::now()` | 47 ns | **31.0 µs** (clocksource `hpet`; see below) |
| `std::mutex` lock + unlock, uncontended | 20 ns | 21 ns |
| atomic `fetch_add` relaxed / seq_cst | 4.9 / 4.9 ns | 10.6 / 12.0 ns |
| atomic CAS, success | 8.7 ns | 19.6 ns |
| mutex + `std::deque` push + pop (today's queue) | 41 ns | 47 ns |
| Chase-Lev push + pop (owner) / push + steal | 23 / 23 ns | 35 / 33 ns |
| bounded MPMC ring push + pop | 20 ns | 33 ns |
| `std::function` assign (24-byte capture) + call | 17 ns | 63 ns |
| syscall floor (`getppid`) / `sched_yield` | 436 / 896 ns | 878 / 1,242 ns |
| cross-core cache-line round trip (two spinning threads) | 139 ns | 328 ns |
| SPSC handoff per item: mutex+deque / MPMC ring / Chase-Lev steal | 85 / 12 / 59 ns | 88 / 24 / 72 ns |
| MPMC 4 producers × 4 consumers per item: mutex+deque / ring | 186 / 124 ns | 126 / 105 ns |
| futex ping-pong round trip, both threads sleeping | 11.4 µs | 959 µs |
| wake a sleeping thread, condvar: p50 / p99 / p99.9 | 10.7 / 77 / 303 µs | 397 / 1,762 / 2,727 µs |
| wake, `std::atomic::wait` + `notify_one` | 10.0 / 63 / 259 µs | 444 / 2,265 / 3,924 µs |
| wake, raw futex (`os_sync_wait_on_address`; `futex(2)`) | 9.9 / 64 / 316 µs | 439 / 1,976 / 3,434 µs |

What it says: every queue operation is 12 to 88 ns and the fastest (the ring) is 4 to 7×
cheaper than today's mutex-and-deque; but **waking a thread is 10 µs at the median and 60 to
80 µs at p99 on the Mac, whatever the primitive**: the condvar, the C++20 atomic wait, and the
raw futex all end in the same kernel path, and the cost is the scheduler's, not the API's. So a
pool that wakes a worker per task pays 100× the queue's cost in CPU, and a query that needs a
*sleeping* worker cannot see a p99 under ~70 µs on this host. The design levers are therefore:
wake rarely (a worker that is awake and searching takes the task; a sleeper is woken only
when none is), and keep a worker awake for the latency class when it is worth a core.

**The gate VM is not a machine for latency floors.** HVF exposes no stable TSC, so the guest
kernel runs on `hpet` (`/sys/.../current_clocksource`; `available_clocksource` lists only
`hpet acpi_pm`) and every `clock_gettime` is a trapped MMIO read: 31 µs. A vCPU that halts is
woken through the hypervisor: 0.4 ms at the median, 2 ms at p99, with three other agents'
single-threaded jobs live on the same 8 vCPUs during the run (load average 9.5). Both numbers
are the platform's, not the pool's; the VM tables in 5.5 compare pools against each other
under the same handicap. Two consequences for the project: any harness that timestamps per
task in the VM pays 62 µs per pair (strata's per-query timing included), and a scheduler's
wake latency should be gated on bare metal (the Mac, the FreeBSD box, or the M1's successor).

## 3. Survey

Read freely (all permissive licenses: Tokio MIT, Go BSD, oneTBB Apache-2.0, Taskflow MIT, folly
Apache-2.0, Rayon MIT/Apache-2.0, Seastar Apache-2.0, Eigen MPL-2.0, Linux documentation);
nothing is ported, so no notices travel.

### 3.1 The local queue: Chase-Lev and its memory orders

Chase and Lev's deque (SPAA 2005) is the standard: the owner pushes and pops at the bottom with
no CAS, thieves take from the top with one CAS, and the owner needs a CAS only for the last
element. Lê, Pop, Cohen, Zappa Nardelli, "Correct and Efficient Work-Stealing for Weak Memory
Models" (PPoPP 2013) prove the C11 version and give the minimal orders, which
`research/queues.hh` uses verbatim: `push` loads `bottom` relaxed and `top` acquire, stores
the slot relaxed, then a release fence before the relaxed store of `bottom + 1`; `take` stores
`bottom - 1` relaxed, a seq_cst fence, loads `top` relaxed, and races with a seq_cst CAS on
`top` only when `t == b`; `steal` loads `top` acquire, a seq_cst fence, `bottom` acquire, then
the CAS. Their measurement: the hand-fenced version keeps at least 1.5× the throughput of the
all-seq_cst version on both x86 and ARM, and the first serializing instruction on the owner's
path is what costs on x86 (Table 2: 4.88 × 10^8 against 3.66 × 10^8 push+take per second on a
4-thread Core i7).

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
`netpoll(delay)`, so the network poller needs no thread of its own. Go's local run queue is
FIFO; only `runnext` is LIFO, and section 5 shows why that matters.

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
same syscall-plus-schedule floor as a futex (section 2); what the ring buys is one sleep point
for I/O completions and tasks, which is reactor's concern, not the pool's.

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
and who may take it. `research/pools.hh` holds them all behind one interface; TSan (Homebrew
LLVM 23) is clean on every one over every workload.

### D-A. Per-worker Chase-Lev deques with stealing (`steal`)

Tokio's shape: a worker posts to its own deque (through a LIFO slot, at most 3 in a row),
external threads post to a bounded MPMC inject ring (Vyukov's), workers give the inject ring a
turn every 61 tasks or 200 µs of task time (the cycle counter, read per task) and when idle,
and steal half from a random peer when their own deque is empty. The owner pops its deque
**FIFO** (Go's run queue), not LIFO (Tokio's): the first prototype popped LIFO and a
self-reposting flush held its worker forever (fairness index 0.31, query p99.9 154 ms); the
`steal-lifo` variant keeps that behavior for the record. Groups are tags. What it gives: the
lowest per-task cost and the best microsecond-task locality. What it cannot give: fairness
(the inject queue, checked first on its turn, takes about half of every worker, so external
posts get 700× their share), class priority, or an index's tasks on one core.

### D-B. Per-group FIFOs, a ready-list, deficit round robin (`groups`)

Each group is an intrusive FIFO under a spinlock plus a deficit counter; a group with work is
on a global ready-list (an MPMC ring of group pointers) once per **ticket**. A worker pops a
group, adds a quantum of service time (100 µs) to its deficit, runs its tasks until the
deficit is spent or the group is empty, and re-appends the group at the tail if work remains
(the deficit carries, so a group whose 10 ms task overran skips rounds until it is even; an
idle group's deficit resets to zero, Seastar's clamp). Tickets implement the plan's rule
directly: a group's first task always earns a ticket; more tickets (up to its width) are
granted only while workers are idle, one per quantum, so an index alone doubles its share of
cores every quantum (16 cores in four quanta, 400 µs) and 1,000 busy indexes hold one ticket
each and share equally. Locality: a worker runs one group for a quantum at a time. A quantum
below the task size turned out to matter: groups that overran are in debt and skipped in
50 ns each, so a group that just became active (a query) reaches a worker without waiting
for anyone's task (EEVDF's eligibility, by accident of DRR); with a 1 ms quantum
(`groups-q1000us`) the query waits behind whole tasks (p99.9 5 ms against 0.5 ms).

### D-C. Priority classes on D-B with a reservation (`prio-r1`, `prio-r2`)

Three ready-lists (query, flush, merge) picked in order. Strict priority alone leaves the
folly problem (long background tasks on every worker), so the pool reserves `R` workers that
refuse background work when `N - R` are already in it, and caps the workers merges may hold.
A query then waits for a wake (tens of µs), not for a flush task to end on some core. The
cost is `R` idle cores while no queries arrive; the alternative, EEVDF-style weights with a
short slice for queries, cannot preempt a running task either, so a reservation (or shorter
background tasks) is the only lever against the tail. Fan-out counts only the workers beyond
the reserve as spare capacity (the first version counted the reserved worker as idle and let
one group take 61% of a saturated pool). Aging for starved flushes is a design note, not
prototyped.

### D-D. Groups pinned to a home worker (`home`): the assumption breaker

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
  it belongs in the API (`help_until`, section 6) rather than in the scheduler.
- **io_uring as the worker's sleep point**: Linux only, and the win is reactor's (one sleep
  point for I/O and tasks), so it is noted for reactor's design, not for the pool.
- **A full EEVDF over groups**: a heap of eligible groups keyed by virtual deadline gives
  weighted shares and latency-by-slice in one mechanism, at O(log G) per pick. D-B's small
  quantum gets the eligibility effect at O(1); weights would be class-based quanta. Worth
  building only if D-B's equal quanta prove insufficient.

## 5. Results

`research_workloads`, 16 workers on the Mac (strict holds `sched-mac-tiny`, `sched-mac-tail`),
medians of three interleaved trials; `baseline` is today's `ThreadPool<>` + `TaskGroup`, `raw`
today's pool without groups, `taskflow` and `moodycamel` the open references (Taskflow 3.10's
executor; moodycamel's `BlockingConcurrentQueue` 1.0.4 with plain workers). Per-task CPU is
`getrusage` time over the run divided by tasks, so it includes the workers' searching and
parking.

### 5.1 Tiny tasks (K4: throughput)

One external producer posting 500,000 empty tasks round robin over the groups:

| pool | 1 group: ns/task wall, CPU | 1,000 groups | 10,000 groups |
|---|---|---|---|
| baseline | 1,978 / 24,032 | 7,009 / 20,200 | 8,225 / 22,603 |
| raw | 6,827 / 19,590 | 7,040 / 20,388 | 7,831 / 22,397 |
| steal (D-A) | 124 / 1,695 | 138 / 1,856 | 130 / 1,793 |
| groups (D-B) | 545 / 6,188 | 46 / 659 | 135 / 1,509 |
| prio-r1 (D-C) | 517 / 6,144 | 110 / 1,375 | 154 / 1,800 |
| home (D-D) | 111 / 1,228 | 495 / 1,688 | 616 / 2,379 |
| taskflow | 221 / 2,828 | 228 / 2,532 | 222 / 2,600 |
| moodycamel | 1,341 / 9,838 | 1,357 / 8,728 | 1,440 / 9,383 |

Four producers, 1,000 groups, empty tasks and 1 µs tasks (wall ns/task; the 1 µs column's
floor is 1,000/16 = 62 ns):

| pool | 4 producers, empty | 4 producers, 1 µs | chained (worker-local re-post) ns/task, CPU |
|---|---|---|---|
| baseline | 8,419 | 8,692 | |
| raw | 5,010 | 1,777 | |
| steal | 153 | 114 | 2.7 / 35 |
| steal-lifo | | | 3.1 / 41 |
| groups | 47 | 96 | 12.6 / 163 |
| prio-r1 | 67 | 108 | 12.4 / 159 |
| home | 535 | 94 | 10.1 / 127 |
| taskflow | 127 | 166 | 67.7 / 859 |
| moodycamel | 145 | 146 | 166 / 1,789 |

Reading: today's pool costs 7 to 9 µs of wall and 20 to 60 µs of CPU per external tiny task,
all of it the condvar wake per enqueue (the queue itself is 41 ns); every candidate is 50 to
150× cheaper and all beat both references. `groups` leads with many groups (46 ns at 1,000:
the per-group spinlock spreads contention; at one group every worker hits one lock and it
falls to 545 ns, where `steal` and `home` are 4 to 5× faster). Worker-local continuations
through the LIFO slot cost 2.7 ns. At 1 µs tasks every candidate is within 90 to 120 ns of
each other.

### 5.2 The latency tail of a query piece on a saturated pool (K4)

A reactor-like thread posts one 20 µs query every 1 ms and waits for it; the background keeps
the pool full with 1 ms flush tasks (one hot group with 64 in flight; 1,000 groups with one
each; 10,000 groups of which 100 are busy) plus four merge groups running 10 ms slices. Wait
= post to start; done p99 = post to finish; `bg` = background tasks per second completed
(the pool's throughput while probed); fairness = Jain's index over the busy groups' completions.

One hot group:

| pool | wait p50 | p99 | p99.9 | done p99 | bg | fairness |
|---|---|---|---|---|---|---|
| baseline | 476 µs | 869 µs | 999 µs | 952 µs | 11.7k | 1.00 |
| steal | 78 µs | 409 µs | 569 µs | 460 µs | 12.7k | 1.00 |
| groups | 70 µs | 470 µs | 1.54 ms | 718 µs | 14.2k | 1.00 |
| prio-r1 | 30 µs | 359 µs | 1.67 ms | 413 µs | 14.6k | 1.00 |
| prio-r2 | 27 µs | 386 µs | 794 µs | 446 µs | 14.2k | 1.00 |
| home | 4.28 ms | 11.5 ms | 16.1 ms | 11.6 ms | 12.2k | 1.00 |

1,000 busy groups:

| pool | wait p50 | p99 | p99.9 | done p99 | bg | fairness |
|---|---|---|---|---|---|---|
| baseline | **70.5 ms** | 83.4 ms | 83.4 ms | 83.4 ms | 15.4k | 0.99 |
| steal | 71 µs | 365 µs | 504 µs | 418 µs | 15.6k | 0.99 |
| groups | 68 µs | 386 µs | 1.84 ms | 465 µs | 15.2k | 0.99 |
| prio-r1 | 40 µs | 358 µs | 1.05 ms | 412 µs | 15.2k | 1.00 |
| prio-r2 | 28 µs | 362 µs | 1.70 ms | 449 µs | 14.7k | 1.00 |
| home | 67.8 ms | 91.7 ms | 91.7 ms | 91.7 ms | 15.3k | 1.00 |

10,000 groups, 100 busy:

| pool | wait p50 | p99 | p99.9 | done p99 | bg | fairness |
|---|---|---|---|---|---|---|
| baseline | 1.14 ms | 7.78 ms | 9.08 ms | 7.80 ms | 13.4k | 1.00 |
| steal | 84 µs | 440 µs | 546 µs | 487 µs | 13.5k | 0.92 |
| groups | 66 µs | 380 µs | 1.34 ms | 476 µs | 15.6k | 1.00 |
| prio-r1 | 30 µs | 320 µs | 1.04 ms | 380 µs | 14.9k | 1.00 |
| prio-r2 | 12 µs | 299 µs | 712 µs | 391 µs | 14.6k | 1.00 |
| home | 16.0 ms | 22.3 ms | 24.1 ms | 22.3 ms | 13.6k | 0.92 |

Reading: today's `TaskGroup` round robin has no classes, so at 1,000 busy groups a query's
ticket waits one full round, 70 ms (1,000 × 1 ms / 16 workers), and at 100 busy groups 1 to
8 ms. Every candidate that lets a query reach *any* worker promptly lands at p50 ≈ 65 to
85 µs (a worker frees up every 1 ms / 16 on average) and p99 ≈ 300 to 450 µs. The reserved
worker (D-C) cuts p50 to 12 to 40 µs, the wake latency of section 2, and keeps background
throughput within 3 to 5%; its p99 stays at ≈ 300 µs because on this oversubscribed host a
woken thread still waits for a hardware thread (the Mac has 16 for 16 busy workers plus the
VM): the pool controls the queueing part of the tail, the OS the rest. `home` is the worst by
three orders of magnitude: a query lands in its home's FIFO behind that core's share of the
flushes (1,000 / 16 groups × 1 ms = 62 ms), so pinning without classes is wrong for latency.

### 5.3 One index's flush: alone and beside 1,000 others (K4)

A flush of 64 tasks of 1 ms posted at once. Alone: the ideal on 16 workers is 4.0 ms. Beside
1,000 busy groups (one 1 ms task in flight each, re-posting): `share` is the probe's measured
fraction of the pool's completions times the number of busy groups, so 1.0 means exactly its
equal share whatever the host's load; the nominal fair-share makespan is 4.0 s (64 ms of work
at 16/1001 of a core).

| pool | alone | beside 1,000 | share | beside 100 | share |
|---|---|---|---|---|---|
| baseline | 5.06 ms | 277 ms | 15.6× | 37 ms | 13.0× |
| steal | 5.04 ms | 5.5 ms | 781× | 8.2 ms | 53× |
| groups | 5.06 ms | 5.42 s | 0.82× | 396 ms | 1.08× |
| groups-q1000us | | | | 542 ms | 0.87× |
| prio-r1 | 5.11 ms | 5.39 s | 0.82× | 515 ms | 0.88× |
| home | 5.03 ms | 136 ms | 31.7× | 78 ms | 5.8× |

Reading: alone, every pool uses every core (4.3 to 5.1 ms against 4.0 ideal; the rest is the
ramp of waking 16 workers). Beside others the pools disagree on policy, and the plan decides:
"many busy indexes each get about one" means equal shares, which only D-B and D-C deliver
(0.8 to 1.1 of a fair share; the probe's 20% shortfall at 1,000 groups is consistent across
trials and configurations, and the hypothesis for the performance engineer is that a group
re-enters the ready-list a task later than a skipping group does). Today's tickets are per
task, so a bursting index takes 13 to 16× its share; the deque pool's inject queue lets
external posts take the pool (700×); home pinning gives 32× and starves the groups that share
the probe's core. `groups`' background fairness index stays at 0.99 to 1.00 throughout.

### 5.4 CPU when idle and memory per group (K4)

| pool | idle CPU (of one core) | context switches / s | bytes per group (RSS delta over 10,000; sizeof) |
|---|---|---|---|
| baseline | 0.354% | 283 | 284 (232 + `TaskGroup::State`) |
| raw | 0.468% | 329 | |
| steal | 0.0008% | 0.3 | 16 (16) |
| groups | 0.0008% | 0.3 | 199 (128) |
| prio-r1 | 0.0008% | 0.3 | 128 |
| home | 0.0006% | 0.3 | 8 (64) |
| taskflow / moodycamel | 0.0008% / 0.0009% | 0.3 | |

Today's workers wake every 100 ms to re-check shutdown (16 × 10 × 2 switches a second); a
futex-parked worker costs nothing until woken. Memory is 128 to 284 bytes per group in every
design: 10,000 groups are 1.3 to 2.8 MB, not a factor.

### 5.5 The gate VM

(`*-vm.txt`; 8 vCPUs, Linux 6.8, g++ 13; filled from the VM runs.)

## 6. Recommendations

The three, in the order to build them; each is additive to `threadpool` (Detent links the
library through `scheduler` and keeps `ThreadPool<TaskType, thread_policy>`, which does not
change).

### R1. The wake protocol: a futex per worker, wake one only when nobody searches

Tokio's idle protocol (`research/idle.hh`: searching and unparked counts in one word, a
sleeper list, at most half the workers searching, the last searcher wakes the next, a final
check before parking, LIFO pick of the warmest sleeper) over `std::atomic<uint32_t>::wait`
(the platform futex: `futex(2)`, `os_sync_wait_on_address`, `_umtx_op`). No timeouts, no
condvar, no spinning (spinning before parking bought nothing in 5.1 and 5.2 and costs CPU
when the first version registered the spinner as a sleeper).

Numbers: per external tiny task 7.0 µs → 46 to 140 ns of wall and 20 µs → 0.7 to 1.9 µs of
CPU (5.1); idle CPU 0.35 to 0.47% of a core and ~300 context switches per second per pool →
0.0008% and 0.3 (5.4). This is independent of the queue, so it is also the one change that
could go into today's `ThreadPool<>` behind an option as a first step; the rest of this list
needs the new pool.

### R2. The scheduling core: per-group FIFOs on a ready-list with deficit round robin (D-B)

A group is an intrusive FIFO plus a 100 µs quantum deficit; the ready-list is an MPMC ring of
groups; tickets give a group one place on the list and more only while workers are idle
(one per quantum, so an index alone takes every core within 400 µs and 1,000 busy indexes
hold one each). This is the plan's rule as a mechanism, and it is the only design that is
fair: Jain's index 0.99 to 1.00 at 1,000 and 10,000 groups, a bursting index gets 0.8 to 1.1
of its equal share (today 13 to 16×, deques 700×), a query piece waits 65 to 70 µs at the
median at 1,000 busy groups (today 70 ms), one index alone finishes a 64 × 1 ms flush in
4.3 to 5.1 ms on 16 workers (ideal 4.0), 128 bytes per group, 46 to 135 ns per external post
with many groups. Keep the quantum below the common task size (100 µs against 1 ms tasks): it
is what gives new arrivals EEVDF's eligibility advantage (p99.9 0.5 ms against 5 ms at a 1 ms
quantum) at O(1).

Open items for the performance engineer: the 0.8 share of a bursting group at 1,000 peers
(5.3); the one-group tiny-task case (545 ns against `steal`'s 124 ns, from 16 workers on one
spinlock), which a per-worker LIFO slot for same-group continuations would fix if query
fan-out ever posts from workers at that rate (at 1 µs tasks the gap is gone); and locality,
which this harness cannot measure (its tasks touch no data), so the index-on-one-core claim
needs strata's real flush with a `last_worker` hint in `pick()` before it is believed.

### R3. Classes with a reservation (D-C)

Three ready-lists in strict order (query, flush, merge), `R` workers that refuse background
work while `N - R` are in it, and a cap on workers in merges. Numbers (5.2): query wait p50
12 to 40 µs with R = 1 or 2 (against 65 to 70 µs without), p99.9 0.5 to 1.7 ms, background
throughput within 3 to 5% of D-B. The reserve is the only lever on the tail that does not
require shorter tasks, because nothing preempts a running task; strata's budget (merge slices
≤ 10 ms, flush partitions ≤ 1 ms) bounds the rest. Starvation of flushes by a flood of
queries is possible in a strict order; an aging rule (a flush waiting more than T is promoted)
is the designed answer and was not needed in these runs.

### Not recommended as the policy, kept as mechanisms

- **D-A, per-worker deques with stealing**: 2.7 ns per worker-local continuation and the
  best one-group tiny-task rate, but no fairness (0.92), no classes, and external posts take
  the pool. Its LIFO slot is the right mechanism for same-group continuations inside R2.
- **D-D, home pinning**: the best memory (64 bytes) and one-group throughput, but p50 of 4 to
  68 ms for a query behind its core's FIFO and 6 to 32× over-share. Keep the *hint*: a group
  remembers the worker that served it last, and an idle worker prefers such groups; measure
  with real index data before making it policy.

### The additive API for threadpool (sketch)

New header `work_pool.hh`; `threadpool.hh`, `task_group.hh`, and `thread.hh` are untouched,
so Detent (through `scheduler`) keeps compiling. Names avoid the catalog's `scheduler`
library.

```cpp
enum class WorkClass : std::uint8_t { query = 0, flush = 1, merge = 2 };

struct WorkItem {                        // caller-owned, intrusive: no allocation per post
	void (*run)(WorkItem*) noexcept;
	WorkGroup* group;
	WorkClass cls = WorkClass::flush;
	WorkItem* next = nullptr;            // the group's FIFO link; the pool's while queued
};

class WorkPool {
public:
	struct Options {
		unsigned threads = 0;            // 0: hardware concurrency
		unsigned reserve = 1;            // workers kept free of flush and merge work
		unsigned merge_cap = 0;          // workers merges may hold; 0: threads - reserve
		std::chrono::nanoseconds quantum = std::chrono::microseconds(100);
		const char* name_format = "W{:02}";   // through Thread/setup_thread, so crash registration keeps working
	};
	explicit WorkPool(Options = {});
	~WorkPool();                         // end(), join()
	unsigned threads() const noexcept;
	bool on_worker() const noexcept;
	void post(WorkItem&) noexcept;       // any thread; never blocks, never allocates
	// Nested waits: runs queued items (the caller's group first) until pred() holds;
	// a worker never blocks in it, so bisection and flush waits cannot deadlock.
	template <typename Pred> void help_until(Pred&& pred);
};

class WorkGroup {                        // one per index, 128 bytes, lives with the index
public:
	WorkGroup(WorkPool&, unsigned width = 0, unsigned weight = 1);
	~WorkGroup();                        // cancel_and_wait()
	void post(WorkItem&) noexcept;
	template <typename F> bool enqueue(F&&);      // boxed convenience: one allocation, TaskGroup's shape
	std::size_t cancel() noexcept;       // drops queued items (returned through item->run with stop set), requests stop
	void wait();                         // helps when called on a worker, parks otherwise
	bool stop_requested() const noexcept;         // the epoch rule of TaskGroupStopToken
	void set_width(unsigned) noexcept;
	void set_weight(unsigned) noexcept;  // quanta per round; 1 for every index by default
};
```

What strata changes: `pool_executor.hh` becomes `work_executor.hh`, `executor::group` wraps a
`WorkGroup` (`open_bounded(width)` is the width), `post(work)` boxes the `std::function`
into a pooled item (merges and flushes are few per second; query pieces embed a `WorkItem` in
their coroutine frame through Kronuz/task's frame pool, so the hot path allocates nothing),
`executor` grows `post(work, WorkClass)` with merges as `merge`, flush partitions and
parse-ahead as `flush`, query fan-out as `query`; bisection's "never wait on an unstarted
helper" becomes `group.wait()` helping, so the caller runs the queued halves itself instead of
idling. What reactor changes: `Reactor::pool_` (its own `asio::thread_pool`) becomes an
optional `WorkPool*` shared with the engine; an offload is a `WorkItem` of class `flush` whose
completion `asio::post`s back to the loop, the `OffloadGate` stays as admission, and the
loop's threads never become workers. The host (Xapiand, prism) owns the one `WorkPool` and
hands it to both.

## 7. Reproduce

```sh
cmake -S . -B .scratch/build -DCMAKE_BUILD_TYPE=Release -DTHREADPOOL_BUILD_RESEARCH=ON
cmake --build .scratch/build -j4
.scratch/build/research/research_bound 2000000                  # the floors (light hold)
research/measure.sh .scratch/build docs/research/scheduler mac 3 # the matrix (strict hold, ~10 min)
python3 research/summarize.py docs/research/scheduler/*-mac.txt  # the median tables
# TSan: Homebrew LLVM, -DCMAKE_CXX_FLAGS="-fsanitize=thread -O1 -g", THREADPOOL_RESEARCH_{TASKFLOW,MOODYCAMEL}=OFF
```
