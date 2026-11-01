# enqueue against a mutex pool and Asio

`bench/enqueue_bench.cc` (`enqueue_bench`, built with `-DTHREADPOOL_BUILD_BENCH=ON`) submits a trivial
task through `ThreadPool::enqueue`, through a pool of the kind most people write first, and through
Asio, and times them on the same machine in turn.

## Method

- Pools. `threadpool`: `ThreadPool<>::enqueue`. `naive`: `NaivePool` in the bench, one `std::mutex`,
  one `std::condition_variable` and a `std::deque<std::function<void()>>`, a `notify_one` per task.
  `asio`: `asio::thread_pool` fed with `asio::post`, from
  [standalone Asio 1.30.2](https://github.com/chriskohlhoff/asio/tree/asio-1-30-2) (the tag the bench
  fetches). The standard library has no thread pool. The bench also times the pool as it was before
  its current queue; that column is not in these tables.
- Workloads. `shared`: each task bumps one shared atomic, and the last one wakes the waiter. `cap40`:
  each task bumps a counter of its worker's own and captures 40 bytes, past the inline buffer of
  libc++'s `std::function`. The figure is wall-clock nanoseconds per task, from the first submit until
  the last task has run, for P producers each submitting 100,000 tasks into W workers; the pools take
  turns inside each of 7 reps (after a warm-up rep) and the figure is the fastest rep. The number in
  brackets is the slowest rep over the fastest.
- Agreement. After every timed run the bench checks that the pool ran exactly the tasks it was
  given and aborts otherwise.
- `latency`: the median over 7 batches of 2,000 `async(f).get()` round trips (`post` plus a promise for
  the pools that have no future-returning call), with 4 workers and the worker asleep as the
  caller waits. The bench runs the block twice because the order of the pools is a neighbor effect.
  `trickle`: one producer submits a task every gap, into 4 workers, and the table gives the
  submit-to-start delay (median and p99) and the CPU the pool used per task.
  `idle`: the CPU of idle workers over 4 s, as a share of one core.
- Machines: Apple M4 Pro (macOS 26) and Intel Core i9-9980HK (macOS 15), Apple clang 17, CMake
  Release (`-O3 -DNDEBUG`), each held exclusively while the bench ran, built from the tree of
  `threadpool:318a95a`.

## Throughput, `shared`

Nanoseconds per task, fastest rep (slowest over fastest):

| P x W | M4 Pro threadpool | naive | asio | i9 threadpool | naive | asio |
|---|---|---|---|---|---|---|
| 1 x 1 | 11.7 (1.75) | 34.8 (1.10) | 48.9 (1.06) | 30.0 (1.05) | 122.1 (1.05) | 228.5 (1.08) |
| 1 x 4 | 38.7 (1.07) | 126.7 (1.22) | 217.3 (1.15) | 124.2 (1.21) | 1116.6 (1.09) | 849.7 (1.05) |
| 1 x 8 | 34.7 (1.07) | 272.3 (5.91) | 841.8 (1.57) | 197.6 (1.77) | 1429.7 (1.12) | 2058.2 (1.14) |
| 4 x 1 | 11.3 (1.89) | 55.9 (1.10) | 71.1 (1.16) | 37.7 (1.09) | 141.1 (1.16) | 233.4 (1.06) |
| 4 x 4 | 37.6 (1.19) | 75.6 (1.08) | 124.4 (1.08) | 135.5 (1.04) | 180.2 (1.22) | 336.2 (1.28) |
| 4 x 8 | 32.3 (1.08) | 144.9 (1.19) | 183.6 (1.61) | 178.7 (1.23) | 449.0 (1.05) | 648.5 (1.05) |

## Throughput, `cap40`

| P x W | M4 Pro threadpool | naive | asio | i9 threadpool | naive | asio |
|---|---|---|---|---|---|---|
| 1 x 1 | 18.6 (1.18) | 53.3 (1.10) | 42.5 (1.09) | 27.0 (1.17) | 191.4 (1.10) | 248.1 (1.09) |
| 1 x 4 | 35.5 (1.24) | 204.8 (1.28) | 182.9 (1.18) | 88.8 (1.54) | 1131.6 (1.16) | 838.8 (1.09) |
| 1 x 8 | 32.1 (1.60) | 1382.9 (1.54) | 664.9 (1.73) | 168.8 (1.70) | 1813.3 (1.14) | 2059.4 (1.09) |
| 4 x 1 | 13.4 (3.39) | 92.4 (1.16) | 74.2 (1.12) | 46.9 (1.45) | 250.8 (1.25) | 270.4 (1.24) |
| 4 x 4 | 39.2 (1.07) | 139.0 (1.20) | 120.1 (1.10) | 126.0 (1.07) | 381.1 (1.24) | 398.7 (1.18) |
| 4 x 8 | 31.4 (1.17) | 168.3 (1.27) | 118.8 (1.52) | 177.9 (1.24) | 640.9 (1.07) | 610.4 (1.15) |

## Future round trip

Median nanoseconds per round trip, over a batch of 2,000; the two blocks of a run:

| | M4 Pro threadpool `async` | threadpool `post` + promise | naive + promise | asio `post` + promise | i9 threadpool `async` | threadpool `post` + promise | naive + promise | asio `post` + promise |
|---|---|---|---|---|---|---|---|---|
| block 1 | 5,446 | 5,620 | 5,203 | 5,478 | 8,776 | 9,088 | 8,306 | 8,700 |
| block 2 | 5,517 | 5,326 | 5,281 | 5,317 | 8,803 | 9,153 | 8,242 | 8,752 |

## Start delay

One producer, a task every gap, 4 workers, 100,000 tasks: the submit-to-start delay, median and p99,
in microseconds, and the CPU the pool used per task, in nanoseconds (the producer's own spin on the
clock is taken out). Each cell is the two blocks of a run.

| Gap | Pool | M4 Pro delay | p99 | CPU per task | i9 delay | p99 | CPU per task |
|---|---|---|---|---|---|---|---|
| 1 µs | threadpool | 3.0, 2.9 | 24.9, 17.4 | 1,726, 1,774 | 5.8, 6.5 | 81.5, 81.8 | 1,942, 1,943 |
| | naive | 6.0, 5.7 | 36.5, 30.2 | 1,966, 1,970 | 5.2, 5.2 | 38.0, 32.4 | 4,839, 4,744 |
| | asio | 3.5, 3.5 | 23.3, 25.0 | 2,073, 2,094 | 3.1, 3.1 | 28.2, 20.8 | 3,346, 3,291 |
| 5 µs | threadpool | 3.3, 3.2 | 18.8, 19.6 | 4,093, 3,851 | 2.5, 2.5 | 26.2, 25.7 | 7,167, 7,143 |
| | naive | 3.1, 3.1 | 17.5, 14.9 | 2,873, 2,783 | 3.8, 3.8 | 37.2, 35.5 | 4,100, 4,017 |
| | asio | 3.2, 3.1 | 15.8, 15.8 | 4,805, 4,165 | 3.9, 3.9 | 30.2, 29.3 | 4,171, 3,967 |
| 50 µs | threadpool | 7.2, 7.3 | 33.4, 29.2 | 3,788, 3,690 | 4.4, 4.5 | 46.2, 45.9 | 5,380, 5,408 |
| | naive | 6.9, 6.6 | 35.0, 25.8 | 3,170, 2,964 | 4.2, 4.2 | 45.6, 45.6 | 4,155, 4,199 |
| | asio | 7.0, 6.7 | 26.0, 27.8 | 3,068, 3,054 | 4.3, 4.2 | 46.9, 45.7 | 4,415, 4,344 |

## Idle workers

The CPU of idle workers as a share of one core over 4 s: four workers 0.040% on the M4 Pro and
0.138% on the i9; eight workers 0.073% and 0.299%.

## Reading it

The pool takes 1.3 to 7.8 times less time per task than the better of the other two in every `shared`
row on both machines, and 2.3 to 21 times less in `cap40`. The hand-offs are closer: the `async`
round trip is 5% to 7% above the naive pool's, which is the fastest on both machines, and at one
task per microsecond the pool starts a task later than Asio and the naive pool on the i9, with a p99
of 82 µs against 21 to 38 µs. At a 5 µs gap on the i9 it uses 7.2 µs of CPU per task against about 4
for the others.
