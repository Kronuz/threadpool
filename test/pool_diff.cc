// Differential test: the pool against its frozen predecessor (test/threadpool_old.hh).
//
// A random script of enqueue / async / post / enqueue_bulk calls is queued behind a gate on a
// one-worker pool, then released; both pools must run the tasks in the same order and resolve
// every future the same way (value, or exception type and message), and report the same state
// (size(), running_size(), the thread counts) while gated and after. The shutdown paths (end(),
// finish(), clear(), the destructor) and the async call shapes are compared the same way, and a
// many-worker run compares the totals.
//
// Uses CHECK, not assert: a Release build must verify too.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "threadpool.hh"
#include "threadpool_old.hh"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

using namespace std::chrono_literals;

namespace {

// Whether a pool has post(): the pool under test does, its frozen predecessor does not.
template <typename P>
concept HasPost = requires(P& p, void (*f)()) { p.post(f); };

void wait_until(const std::function<bool()>& pred) {
	auto deadline = std::chrono::steady_clock::now() + 20s;
	while (!pred()) {
		CHECK(std::chrono::steady_clock::now() < deadline);
		std::this_thread::sleep_for(100us);
	}
}

// What a run observed; the two pools must produce equal ones.
struct Observed {
	std::vector<int> order;            // ids, in execution order
	std::vector<std::string> results;  // one line per future, in script order
	std::size_t size_gated = 0;
	std::size_t running_gated = 0;
	std::size_t size_after = 0;
	std::size_t running_after = 0;
	std::size_t threads = 0;
	std::size_t workers = 0;

	bool operator==(const Observed&) const = default;
};

struct Op {
	enum Kind { enqueue, async_value, async_throw, async_void, post, bulk } kind;
	int id;
	int count;  // bulk
};

std::vector<Op> make_script(unsigned seed, int n) {
	std::mt19937 rng(seed);
	std::vector<Op> ops;
	int id = 0;
	for (int i = 0; i < n; ++i) {
		Op op{};
		op.kind = static_cast<Op::Kind>(rng() % 6);
		op.id = id;
		op.count = op.kind == Op::bulk ? static_cast<int>(rng() % 20) : 1;
		id += op.count;
		ops.push_back(op);
	}
	return ops;
}

template <typename Pool>
Observed run_script(const std::vector<Op>& ops) {
	Observed obs;
	std::mutex m;
	std::atomic<bool> gate{false};
	std::atomic<bool> gate_running{false};
	Pool pool("D{:02}", 1);

	auto record = [&](int id) {
		std::lock_guard<std::mutex> lk(m);
		obs.order.push_back(id);
	};
	// The gate holds the only worker, so everything after it queues behind it, in order.
	pool.enqueue([&] {
		gate_running = true;
		while (!gate.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
	});
	wait_until([&] { return gate_running.load(); });

	std::vector<std::function<std::string()>> futures;
	std::size_t queued = 0;
	for (const auto& op : ops) {
		int id = op.id;
		switch (op.kind) {
			case Op::enqueue:
				pool.enqueue([&record, id] { record(id); });
				++queued;
				break;
			case Op::post:
				if constexpr (HasPost<Pool>) {
					pool.post([&record](int i) { record(i); }, id);
				} else {
					pool.enqueue([&record, id] { record(id); });
				}
				++queued;
				break;
			case Op::async_value: {
				auto f = std::make_shared<std::future<int>>(pool.async([&record](int i, int k) { record(i); return i * k; }, id, 3));
				futures.push_back([f] { return "value " + std::to_string(f->get()); });
				++queued;
				break;
			}
			case Op::async_throw: {
				auto f = std::make_shared<std::future<int>>(pool.async([&record](int i) -> int {
					record(i);
					if (i % 2) {
						throw std::invalid_argument("odd " + std::to_string(i));
					}
					throw std::out_of_range("even " + std::to_string(i));
				}, id));
				futures.push_back([f] {
					try {
						return "value " + std::to_string(f->get());
					} catch (const std::invalid_argument& e) {
						return std::string("invalid_argument ") + e.what();
					} catch (const std::out_of_range& e) {
						return std::string("out_of_range ") + e.what();
					}
				});
				++queued;
				break;
			}
			case Op::async_void: {
				auto f = std::make_shared<std::future<void>>(pool.async([&record](int i) { record(i); }, id));
				futures.push_back([f] { f->get(); return std::string("void"); });
				++queued;
				break;
			}
			case Op::bulk: {
				std::vector<std::function<void()>> batch;
				for (int k = 0; k < op.count; ++k) {
					batch.push_back([&record, id, k] { record(id + k); });
				}
				CHECK(pool.enqueue_bulk(batch.begin(), batch.size()));
				queued += batch.size();
				break;
			}
		}
	}

	obs.size_gated = pool.size();
	obs.running_gated = pool.running_size();
	CHECK(obs.size_gated == queued);
	CHECK(obs.running_gated == 1);
	obs.threads = pool.threadpool_size();

	gate.store(true, std::memory_order_release);
	pool.end();
	CHECK(pool.join());
	for (auto& f : futures) {
		obs.results.push_back(f());
	}
	obs.size_after = pool.size();
	obs.running_after = pool.running_size();
	obs.workers = pool.threadpool_workers();
	return obs;
}

// The future a dropped task leaves: its error code, or "ok".
std::string future_state(std::future<int>& f) {
	try {
		return "value " + std::to_string(f.get());
	} catch (const std::future_error& e) {
		return e.code() == std::make_error_code(std::future_errc::broken_promise) ? "broken_promise" : "future_error";
	}
}

struct ShutdownObserved {
	int ran_before_end = 0;
	int ran_after_end = 0;
	std::size_t size_after_end = 0;
	std::string cleared_future;
	std::size_t size_after_clear = 0;
	std::string finished_future;
	int survived = 0;

	bool operator==(const ShutdownObserved&) const = default;
};

template <typename Pool>
ShutdownObserved run_shutdown() {
	ShutdownObserved obs;

	// end(): queued tasks run first; a task enqueued after it never does.
	{
		Pool pool("E{:02}", 2);
		std::atomic<int> ran{0};
		for (int i = 0; i < 500; ++i) {
			pool.enqueue([&ran] { ran.fetch_add(1); });
		}
		pool.end();
		CHECK(pool.join());
		obs.ran_before_end = ran.load();
		pool.enqueue([&ran] { ran.fetch_add(1000); });
		obs.size_after_end = pool.size();
		obs.ran_after_end = ran.load();
	}

	// clear(): queued tasks are dropped and their futures break; the running one finishes.
	{
		Pool pool("C{:02}", 1);
		std::atomic<bool> gate{false}, in{false};
		pool.enqueue([&] {
			in = true;
			while (!gate.load()) {
				std::this_thread::yield();
			}
		});
		wait_until([&] { return in.load(); });
		auto f = pool.async([] { return 1; });
		pool.enqueue([] {});
		pool.clear();
		obs.size_after_clear = pool.size();
		obs.cleared_future = future_state(f);
		gate = true;
		pool.end();
		CHECK(pool.join());
	}

	// finish(): the destructor drops what is queued; its futures break.
	{
		std::future<int> f;
		{
			Pool pool("F{:02}", 1);
			std::atomic<bool> gate{false}, in{false};
			pool.enqueue([&] {
				in = true;
				while (!gate.load()) {
					std::this_thread::yield();
				}
			});
			wait_until([&] { return in.load(); });
			f = pool.async([] { return 2; });
			pool.finish();
			CHECK(pool.finished());
			gate = true;
			CHECK(pool.join());
		}
		obs.finished_future = future_state(f);
	}

	// A task that throws does not kill its worker.
	{
		Pool pool("X{:02}", 1);
		std::atomic<int> ran{0};
		for (int i = 0; i < 10; ++i) {
			pool.enqueue([] { throw std::runtime_error("ignored"); });
			pool.enqueue([&ran] { ran.fetch_add(1); });
		}
		pool.end();
		CHECK(pool.join());
		obs.survived = ran.load();
	}
	return obs;
}

// async call shapes: references through std::ref, move-only arguments, results and callables,
// member functions.
struct Calc {
	int base;
	int add(int x) const { return base + x; }
};

template <typename Pool>
std::vector<std::string> run_shapes() {
	std::vector<std::string> out;
	Pool pool("S{:02}", 3);

	int counter = 0;
	pool.async([](int& c) { c += 5; }, std::ref(counter)).get();  // make_tuple unwraps reference_wrapper
	out.push_back("ref " + std::to_string(counter));

	auto up = std::make_unique<int>(41);
	auto f = pool.async([](std::unique_ptr<int> p) { return std::make_unique<int>(*p + 1); }, std::move(up));
	out.push_back("unique " + std::to_string(*f.get()));

	Calc calc{10};
	out.push_back("member " + std::to_string(pool.async(&Calc::add, &calc, 5).get()));
	out.push_back("memberobj " + std::to_string(pool.async(&Calc::add, calc, 6).get()));

	std::string s = "abc";
	out.push_back("string " + pool.async([](std::string x) { return x + "!"; }, s).get());
	out.push_back("lvalue " + s);

	auto thrower = pool.async([]() -> std::string { throw std::logic_error("shape"); });

	// A move-only callable through async.
	auto mo = [p = std::make_unique<int>(9)] { return *p; };
	out.push_back("moveonly " + std::to_string(pool.async(std::move(mo)).get()));

	// Joined before the exception is read: see test_exceptions in pool_stress.cc.
	pool.end();
	CHECK(pool.join());
	try {
		thrower.get();
		out.push_back("no throw");
	} catch (const std::logic_error& e) {
		out.push_back(std::string("logic_error ") + e.what());
	}
	return out;
}

template <typename Pool>
std::pair<long, std::size_t> run_many(int workers, int producers, int per) {
	Pool pool("M{:02}", static_cast<std::size_t>(workers));
	std::atomic<long> sum{0};
	std::vector<std::thread> ps;
	for (int p = 0; p < producers; ++p) {
		ps.emplace_back([&, p] {
			for (int i = 0; i < per; ++i) {
				long v = p * per + i;
				pool.enqueue([&sum, v] { sum.fetch_add(v, std::memory_order_relaxed); });
			}
		});
	}
	for (auto& t : ps) {
		t.join();
	}
	pool.end();
	CHECK(pool.join());
	return {sum.load(), pool.size() + pool.running_size()};
}

}  // namespace

int main() {
	using OldPool = threadpool_old::ThreadPool<>;
	using NewPool = ThreadPool<>;

	// ---- random scripts: same order, same futures, same counters ----
	for (unsigned seed = 1; seed <= 40; ++seed) {
		auto script = make_script(seed, 200);
		auto before = run_script<OldPool>(script);
		auto after = run_script<NewPool>(script);
		CHECK(before.order == after.order);
		CHECK(before.results == after.results);
		CHECK(before == after);
		CHECK(!after.order.empty());
	}
	std::printf("pool_diff OK: 40 random scripts ran in the same order with the same futures and counters\n");

	// ---- shutdown paths ----
	{
		auto before = run_shutdown<OldPool>();
		auto after = run_shutdown<NewPool>();
		CHECK(before == after);
		CHECK(after.ran_before_end == 500);
		CHECK(after.ran_after_end == 500);  // the task enqueued after end() never ran
		CHECK(after.size_after_end == 1);
		CHECK(after.cleared_future == "broken_promise");
		CHECK(after.finished_future == "broken_promise");
		CHECK(after.survived == 10);
		std::printf("pool_diff OK: end/clear/finish/throwing tasks behave as before\n");
	}

	// ---- async call shapes ----
	{
		auto before = run_shapes<OldPool>();
		auto after = run_shapes<NewPool>();
		CHECK(before.size() == after.size());
		for (std::size_t i = 0; i < before.size(); ++i) {
			CHECK(before[i] == after[i]);
		}
		std::printf("pool_diff OK: %zu async shapes resolve the same\n", after.size());
	}

	// ---- many workers: same totals, nothing left over ----
	for (int workers : {1, 2, 4, 8}) {
		for (int producers : {1, 3}) {
			auto before = run_many<OldPool>(workers, producers, 20000);
			auto after = run_many<NewPool>(workers, producers, 20000);
			CHECK(before == after);
			CHECK(after.second == 0);
		}
	}
	std::printf("pool_diff OK: 1-8 workers, 1-3 producers: same totals, nothing left over\n");

	std::printf("all pool differential tests passed\n");
	return 0;
}
