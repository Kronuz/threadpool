# threadpool

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/threadpool/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/threadpool/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)

Fixed-size thread pool for C++20: named workers, futures, cancelable task groups and a
fair multi-owner scheduler. `ThreadPool` runs a fixed set of named threads that pull from
one queue of 64-slot chunks, with a lock at each end, so a producer and a worker never
wait for each other. `WorkPool` shares one pool among thousands of owners, with fair
turns, latency classes and caller-owned items that cost no allocation. Reach for it when
a C++ service needs background workers without pulling in a framework.

## ✨ Highlights

- **Four ways to submit:** `enqueue` (fire and forget), `post` (the same with
  arguments), `enqueue_bulk`, and `async`, which returns a `std::future`.
- **No allocation for small tasks:** a callable of up to 48 bytes is stored in its queue
  slot, and an enqueue signals a worker only when one sleeps.
- **Task groups:** `TaskGroup` cancels what has not started, signals running tasks
  through a stop token, and waits for its own tasks only, on a pool other owners share.
- **Many owners on one pool:** `WorkPool` serves per-group FIFOs by deficit round robin,
  runs `query` items ahead of `flush` and `merge` on workers kept for queries, and parks
  idle workers on a futex.
- **Named workers** (`"W00"`, `"W01"`, ...) that show up in debuggers and `top`.
- **Two shutdowns:** `end()` drains the queue first, `finish()` stops as soon as
  possible, and `join()` waits with a timeout.
- **Lock-free counters:** `size()`, `running_size()` and `threadpool_workers()` read
  atomics; tracing and thread registration hooks are no-ops by default.

## 🚀 Quick start

```cpp
#include "task_group.hh"
#include "threadpool.hh"

#include <atomic>
#include <cstdio>
#include <future>

int main() {
	ThreadPool<> pool("W{:02}", 4);  // four workers, named W00..W03

	pool.enqueue([] { std::puts("fire and forget"); });

	std::future<int> sum = pool.async([](int a, int b) { return a + b; }, 2, 3);
	std::printf("2 + 3 = %d\n", sum.get());  // 2 + 3 = 5

	std::atomic<int> chunks{0};
	{
		TaskGroup group(pool);  // one owner's tasks on the shared pool
		for (int i = 0; i < 8; ++i) {
			group.enqueue([&](const TaskGroupStopToken& stop) {
				if (!stop.stop_requested()) {
					++chunks;
				}
			});
		}
		group.wait();  // waits for this group's tasks only
	}  // leaving the scope cancels what has not started
	std::printf("%d chunks\n", chunks.load());  // 8 chunks

	pool.end();   // let the workers drain the queue
	pool.join();  // and wait for them
}
```

Many owners on one `WorkPool`, with an item the caller owns:

```cpp
#include "work_pool.hh"

#include <cstdio>

struct Piece : WorkItem {
	int shard = 0;
};

int main() {
	WorkPool pool;          // a worker per hardware thread, one kept for queries
	WorkGroup index(pool);  // one group per owner

	Piece piece;
	piece.shard = 7;
	piece.cls = WorkClass::query;
	piece.run = [](WorkItem* w) noexcept { std::printf("shard %d\n", static_cast<Piece*>(w)->shard); };
	index.post(piece);  // the caller owns the item: no allocation

	index.enqueue([] { std::puts("a boxed flush"); });
	index.wait();
	// shard 7
	// a boxed flush     (the two lines may come in either order)
}
```

[`examples/demo.cc`](examples/demo.cc) runs a fuller tour: futures, load spread across
workers, the counters, both shutdowns, and `TaskQueue`.

## 📦 Installation

Requires C++20 (the library uses `std::format`), CMake 3.14 or newer, and pthreads; no
other dependencies. CI builds and tests it on Linux (x86_64 with GCC and Clang, arm64),
macOS (arm64, x86_64) and FreeBSD, plus ASan, UBSan and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(threadpool
  GIT_REPOSITORY https://github.com/Kronuz/threadpool.git
  GIT_TAG        f533782bf12dad200d4daf8c221ab1140ee91c47)  # or main, to track the tip
FetchContent_MakeAvailable(threadpool)

target_link_libraries(my_app PRIVATE threadpool::threadpool)
```

`threadpool::threadpool` is a small static library (it compiles `thread.cc`) that brings
`Threads::Threads` with it. Include `threadpool.hh`, `task_group.hh` or `work_pool.hh`.

## 🎯 When to use it

- A fixed pool of long-lived workers for request handling, indexing or other background
  work, with futures or group-scoped cancellation.
- Floods of tiny tasks, from one producer or many: tens of nanoseconds per task on the
  M4 Pro (see Performance).
- One pool shared by many owners (`WorkPool`), with fairness between them, latency
  classes and high rates of tiny items.

Not for:

- Fine-grained fork-join work that needs work stealing: neither pool steals.
- Coroutine scheduling: see [task](https://github.com/Kronuz/task).
- Bounded queues with back-pressure: `ThreadPool`'s queue is unbounded and `enqueue`
  always returns `true`.

## ⚡ Performance

`ThreadPool::enqueue` of a trivial task (one increment of a shared atomic), from P producer
threads into W workers, in nanoseconds per task: wall clock from the first submit until the
last task has run, the fastest of 7 runs. `naive` is a mutex and condition variable pool and
`asio` is `asio::thread_pool` from standalone Asio 1.30.2. The spread (slowest run over
fastest) is 1.04 to 1.4 except where noted.

| P x W | threadpool, M4 Pro | naive | asio | threadpool, i9 | naive | asio |
|---|---|---|---|---|---|---|
| 1 x 1 | **17.6** | 34.3 | 49.6 | **29.6** | 123.8 | 221.7 |
| 1 x 4 | **41.3** | 115.0 | 219.9 | **128.1** | 1106.5 | 882.1 |
| 1 x 8 | **34.5** (spread 2.8) | 2505 | 1351 | **191.9** (spread 2.0) | 1405 | 2052 |
| 4 x 1 | **22.6** | 63.7 | 72.3 | **39.4** | 140.8 | 237.0 |
| 4 x 4 | **46.8** | 78.9 | 124.1 | **139.4** | 180.8 | 337.3 |
| 4 x 8 | **32.6** | 133.6 (spread 1.6) | 123.1 (spread 1.6) | **163.9** (spread 1.7) | 419.0 | 650.9 |

A task with a 40-byte closure (past `std::function`'s inline buffer in libc++) is no more than
15% slower than a task that captures nothing in 11 of the 12 configurations; the i9's 4 x 1
is 34% slower (47 against 35 ns). Four idle workers use 0.04% of a core on the M4 Pro and
0.13% on the i9.

The pool does not win a single hand-off. A future round trip (`async(f).get()`, with a worker
that sleeps as the caller wakes) and the delay before a task starts, with one task submitted
every microsecond into 4 workers (median), in microseconds:

| | threadpool, M4 Pro | naive | asio | threadpool, i9 | naive | asio |
|---|---|---|---|---|---|---|
| future round trip | 6.9 | 6.1 | 6.2 | 8.8 | 8.2 | 8.2 |
| start delay, 1 task per µs | 2.4 | 2.0 to 4.7 | 3.8 to 4.2 | 6.4 | 5.0 | 3.1 |

Machines: Apple M4 Pro (arm64) and Intel Core i9-9980HK (x86_64), both on macOS with Apple
clang 17, `-O3 -DNDEBUG`. CMake fetches Asio 1.30.2 when the system has none; pass
`-DASIO_INCLUDE_DIR=<asio>/asio/include` to choose one.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DTHREADPOOL_BUILD_BENCH=ON
cmake --build build --target enqueue_bench
./build/enqueue_bench 7 100000 shared    # the first table; also cap40, local, idle, latency, trickle
```

[ARCHITECTURE.md](ARCHITECTURE.md) explains the queue, the wake protocol and the
trade-offs.

## 📚 Documentation

- [docs/API.md](docs/API.md): `ThreadPool`, `TaskGroup`, `WorkPool`, `WorkGroup`,
  `Thread`, `TaskQueue` and the free functions.
- [ARCHITECTURE.md](ARCHITECTURE.md): the worker loop, shutdown, task wrappers,
  `TaskGroup`'s scheduling, `WorkPool`'s rings, turns, classes and idle protocol, and the
  concurrency model.
- [docs/mechanisms/work-pool.md](docs/mechanisms/work-pool.md): how `WorkPool` schedules.
- [CHANGELOG.md](CHANGELOG.md): notable changes, with their measurements.
- [AGENTS.md](AGENTS.md): repository map, invariants, traps, and how to plug in tracing.
- [Catalog entry](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#threadpool)
  in Kronuz C++ Libraries.

## 🛠️ Building and testing

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

`./build/threadpool_demo` runs the tour. Add `-DTHREADPOOL_BUILD_BENCH=ON` to build the
benchmarks.

## 🤝 Contributing

Issues and pull requests are welcome; read [AGENTS.md](AGENTS.md) and the catalog's
[rules of engagement](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#rules-of-engagement)
first.

## 📄 License

MIT, see [LICENSE](LICENSE). Extracted from [Xapiand](https://github.com/Kronuz/Xapiand),
where it runs the servers, clients, indexers, committers and replication workers.
