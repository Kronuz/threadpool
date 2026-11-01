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

`ThreadPool::enqueue` of a trivial task (one increment), from P producer threads into W
workers, in nanoseconds per task: wall clock from the first submit until the last task
has run, best of 7 runs. `naive` is a mutex and condition variable pool, `asio` is
standalone Asio's `asio::thread_pool`. The spread (slowest over fastest of the 7 runs) is
1.04 to 1.4 except where noted.

| P x W | threadpool, M4 Pro | naive | asio | threadpool, i9 | naive | asio |
|---|---|---|---|---|---|---|
| 1 x 1 | **19** | 32 | 49 | **32** | 132 | 225 |
| 1 x 4 | **34** | 157 | 227 | **126** | 967 | 840 |
| 1 x 8 | **35** (spread 3.0) | 1643 | 1045 | **223** (spread 1.8) | 1690 | 2289 |
| 4 x 1 | **11** | 56 | 79 | **40** | 148 | 244 |
| 4 x 4 | **34** | 73 | 110 | **146** | 226 | 389 |
| 4 x 8 | **32** | 135 | 177 | **221** | 525 | 708 |

A task with a 40-byte closure (past `std::function`'s inline buffer in libc++) costs the
same as a small one. Four idle workers use 0.04% of a core on the M4 Pro and 0.1% on the
i9. A future round trip (`async(f).get()`, with a worker that sleeps as the caller wakes)
takes 9 to 10 microseconds on both machines. One task submitted every microsecond starts
after 3 microseconds on the M4 Pro and 5 on the i9.

Machines: Apple M4 Pro (arm64) and Intel Core i9-9980HK (x86_64), both on macOS with
Apple clang, `-O2`. Build with `-DTHREADPOOL_BUILD_BENCH=ON` or the line at the top of
[`bench/enqueue_bench.cc`](bench/enqueue_bench.cc), then run `./build/enqueue_bench`.
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
