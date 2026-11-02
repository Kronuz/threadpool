# threadpool

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/threadpool/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/threadpool/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)

Fixed-size thread pool for C++20: named workers, caller-owned work items and a fair
multi-owner scheduler. `WorkPool` shares one set of named threads among thousands of
owners, each a `WorkGroup`, with fair turns, latency classes, cancellation and waits
scoped to one owner, and items that cost no allocation. Reach for it when a C++ service
needs background workers without pulling in a framework.

## ✨ Highlights

- **Many owners on one pool:** `WorkPool` serves per-group FIFOs by deficit round robin,
  runs `query` items ahead of `flush` and `merge` on workers kept for queries, and parks
  idle workers on a futex.
- **No allocation for caller-owned items:** `WorkGroup::post` links a `WorkItem` the
  caller owns; `enqueue` boxes a callable with one allocation.
- **Group-scoped control:** `cancel()` drops what has not started and signals running
  items through `stop_requested()`; `wait()` waits for the group's own items only, and
  `on_idle()` lets an event loop join a group without blocking.
- **Named workers** (`"W00"`, `"W01"`, ...) that show up in debuggers and `top`.
- **Live counters:** `WorkGroup::pending()` and `running()` report a group's backlog and
  load; tracing and thread registration hooks are no-ops by default.

## 🚀 Quick start

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
workers, results through promises, the counters, and `wait()` against
`cancel_and_wait()`.

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
`Threads::Threads` with it. Include `work_pool.hh` (and `thread.hh` for the thread names).

## 🎯 When to use it

- A fixed pool of long-lived workers for request handling, indexing or other background
  work, with group-scoped cancellation and waits.
- Floods of tiny items, from one producer or many.
- One pool shared by many owners, with fairness between them, latency classes and high
  rates of tiny items.

Not for:

- Fine-grained fork-join work that needs work stealing: the pool does not steal.
- Coroutine scheduling: see [task](https://github.com/Kronuz/task).
- Bounded queues with back-pressure: a post never blocks and never fails.
- Futures: `WorkGroup::enqueue` returns no future; pass a `std::promise` or post a
  `WorkItem` that carries the result.

## ⚡ Performance

The design and its measurements are in
[docs/research/scheduler.md](docs/research/scheduler.md) and
[docs/mechanisms/work-pool.md](docs/mechanisms/work-pool.md). `bench/kernel_time.cc`
(`-DTHREADPOOL_BUILD_BENCH=ON`) reports user time, kernel time and context switches per
operation for a handoff, a burst, an idle pool and a timed sleep.

[ARCHITECTURE.md](ARCHITECTURE.md) explains the thread primitive, the rings, the turns
and the wake protocol.

## 📚 Documentation

- [docs/API.md](docs/API.md): `WorkPool`, `WorkGroup`, `WorkItem`, `Thread` and the free
  functions.
- [ARCHITECTURE.md](ARCHITECTURE.md): the thread primitive, `WorkPool`'s rings, turns,
  classes and idle protocol, and the concurrency model.
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
