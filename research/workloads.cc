// The workloads: tiny-task throughput, the latency tail of a query-class task on a saturated
// pool, one index's flush makespan alone and beside 1,000 busy indexes, CPU when idle, and
// memory per group, with one hot group, 1,000 busy groups, or 10,000 groups mostly idle.
// Pools run interleaved, trial by trial. Research code.
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <time.h>

#include "common.hh"
#include "futex.hh"
#include "pools.hh"

using namespace research;

namespace {

struct Latch {
	alignas(64) std::atomic<std::uint32_t> remaining{0};
	void add(std::uint32_t n) noexcept { remaining.fetch_add(n, std::memory_order_relaxed); }
	void done_one() noexcept {
		if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) { futex_wake_all(&remaining); }
	}
	void wait() noexcept {
		for (;;) {
			auto v = remaining.load(std::memory_order_acquire);
			if (v == 0) { return; }
			futex_wait(&remaining, v);
		}
	}
};

inline void burn(std::uint64_t ns) noexcept {
	if (ns == 0) { return; }
	if (ns >= 2000) { burn_ns_clock(ns); } else { burn_ns(ns); }
}

struct Job final : Task {
	std::uint64_t work_ns = 0;
	std::uint64_t posted = 0;
	std::uint64_t started = 0;
	std::uint64_t finished = 0;
	Latch* latch = nullptr;
	// Self-reposting background work:
	Pool* pool = nullptr;
	const std::atomic<bool>* running = nullptr;
	std::uint64_t count = 0;
	std::uint64_t reposts_left = 0;
};

void run_once(Task* t) {
	auto* j = static_cast<Job*>(t);
	j->started = now_ns();
	burn(j->work_ns);
	j->finished = now_ns();
	if (j->latch) { j->latch->done_one(); }
}

// Runs, then posts itself again while `running` (or while reposts remain).
void run_again(Task* t) {
	auto* j = static_cast<Job*>(t);
	burn(j->work_ns);
	++j->count;
	bool more = j->running ? j->running->load(std::memory_order_relaxed) : j->reposts_left-- > 0;
	if (more) {
		j->pool->post(j);
	} else if (j->latch) {
		j->latch->done_one();
	}
}

struct Args {
	std::string workload = "tiny";
	std::vector<std::string> pools{"baseline", "raw", "steal", "groups", "prio", "home"};
	unsigned threads = hardware_threads();
	unsigned trials = 3;
	unsigned groups = 1;
	unsigned busy = 0;          // busy groups among `groups` (0: all)
	std::uint64_t tasks = 2'000'000;
	std::uint64_t work_ns = 0;
	unsigned producers = 1;     // 0: tasks repost themselves from the workers
	double seconds = 5;
	unsigned flush_ms = 1;
	unsigned merge_ms = 10;
	unsigned merges = 0;
	unsigned outstanding = 1;   // flush tasks each busy group keeps in flight
	unsigned query_us = 20;
	unsigned query_interval_us = 1000;
	unsigned flush_tasks = 64;
};

Args parse(int argc, char** argv) {
	Args a;
	for (int i = 1; i < argc; ++i) {
		std::string s = argv[i];
		auto eq = s.find('=');
		if (eq == std::string::npos) { a.workload = s; continue; }
		auto k = s.substr(0, eq), v = s.substr(eq + 1);
		if (k == "pools") {
			a.pools.clear();
			std::size_t p = 0;
			while (p <= v.size()) {
				auto c = v.find(',', p);
				if (c == std::string::npos) { c = v.size(); }
				if (c > p) { a.pools.push_back(v.substr(p, c - p)); }
				p = c + 1;
			}
		} else if (k == "threads") { a.threads = std::stoul(v); }
		else if (k == "trials") { a.trials = std::stoul(v); }
		else if (k == "groups") { a.groups = std::stoul(v); }
		else if (k == "busy") { a.busy = std::stoul(v); }
		else if (k == "tasks") { a.tasks = std::stoull(v); }
		else if (k == "work_ns") { a.work_ns = std::stoull(v); }
		else if (k == "producers") { a.producers = std::stoul(v); }
		else if (k == "seconds") { a.seconds = std::stod(v); }
		else if (k == "flush_ms") { a.flush_ms = std::stoul(v); }
		else if (k == "merge_ms") { a.merge_ms = std::stoul(v); }
		else if (k == "merges") { a.merges = std::stoul(v); }
		else if (k == "outstanding") { a.outstanding = std::stoul(v); }
		else if (k == "query_us") { a.query_us = std::stoul(v); }
		else if (k == "query_interval_us") { a.query_interval_us = std::stoul(v); }
		else if (k == "flush_tasks") { a.flush_tasks = std::stoul(v); }
		else { std::fprintf(stderr, "unknown arg %s\n", k.c_str()); std::exit(2); }
	}
	return a;
}

void sleep_ns(std::uint64_t ns) {
	timespec ts{static_cast<time_t>(ns / 1'000'000'000ULL), static_cast<long>(ns % 1'000'000'000ULL)};
	while (nanosleep(&ts, &ts) != 0) {}
}

struct Groups {
	Pool& pool;
	std::vector<Group*> gs;
	Groups(Pool& p, unsigned n, unsigned width = 0) : pool(p) {
		gs.reserve(n);
		for (unsigned i = 0; i < n; ++i) { gs.push_back(p.open(width)); }
	}
	~Groups() { for (auto* g : gs) { pool.close(g); } }
	Group* operator[](std::size_t i) const { return gs[i % gs.size()]; }
};

////////////////////////////////////////////////////////////////////////////////

// Throughput of tiny tasks: external producers posting round robin over the groups, or
// (producers=0) tasks that repost themselves from the workers.
void tiny(const Args& a, Pool& pool) {
	Groups groups(pool, a.groups);
	Latch latch;
	if (a.producers == 0) {
		const unsigned chains = pool.workers() * 4;
		std::vector<Job> jobs(chains);
		const std::uint64_t per_chain = a.tasks / chains;
		for (unsigned i = 0; i < chains; ++i) {
			jobs[i].fn = run_again;
			jobs[i].group = groups[i];
			jobs[i].work_ns = a.work_ns;
			jobs[i].pool = &pool;
			jobs[i].latch = &latch;
			jobs[i].reposts_left = per_chain - 1;
		}
		latch.add(chains);
		auto t0 = now_ns();
		for (auto& j : jobs) { pool.post(&j); }
		latch.wait();
		auto t1 = now_ns();
		const double total = double(per_chain) * chains;
		std::printf("tiny pool=%-14s groups=%-6u producers=0 work_ns=%-5llu tasks=%.0f wall=%.3fs rate=%.2fM/s ns_per_task=%.1f cpu_ns_per_task=%.1f\n",
			pool.name(), a.groups, static_cast<unsigned long long>(a.work_ns), total, double(t1 - t0) / 1e9, total / double(t1 - t0) * 1e3,
			double(t1 - t0) / total, double(t1 - t0) * pool.workers() / total);
		return;
	}
	std::vector<Job> jobs(a.tasks);
	for (std::uint64_t i = 0; i < a.tasks; ++i) {
		jobs[i].fn = run_once;
		jobs[i].group = groups[i];
		jobs[i].work_ns = a.work_ns;
		jobs[i].latch = &latch;
	}
	latch.add(static_cast<std::uint32_t>(a.tasks));
	std::vector<std::thread> producers;
	const std::uint64_t per = a.tasks / a.producers;
	auto t0 = now_ns();
	for (unsigned p = 0; p < a.producers; ++p) {
		producers.emplace_back([&, p] {
			std::uint64_t lo = p * per, hi = p + 1 == a.producers ? a.tasks : lo + per;
			for (std::uint64_t i = lo; i < hi; ++i) { pool.post(&jobs[i]); }
		});
	}
	for (auto& t : producers) { t.join(); }
	auto t_posted = now_ns();
	latch.wait();
	auto t1 = now_ns();
	std::printf("tiny pool=%-14s groups=%-6u producers=%u work_ns=%-5llu tasks=%llu wall=%.3fs rate=%.2fM/s ns_per_task=%.1f post_ns=%.1f cpu_ns_per_task=%.1f\n",
		pool.name(), a.groups, a.producers, static_cast<unsigned long long>(a.work_ns), static_cast<unsigned long long>(a.tasks), double(t1 - t0) / 1e9,
		double(a.tasks) / double(t1 - t0) * 1e3, double(t1 - t0) / double(a.tasks), double(t_posted - t0) * a.producers / double(a.tasks),
		double(t1 - t0) * pool.workers() / double(a.tasks));
}

////////////////////////////////////////////////////////////////////////////////

// Background load: `busy` groups each keep `outstanding` flush tasks of flush_ms in flight,
// `merges` groups each one merge slice of merge_ms, all reposting themselves.
struct Background {
	std::atomic<bool> running{true};
	Latch latch;
	std::vector<Job> flushes;
	std::vector<Job> merges;

	Background(Pool& pool, const Groups& groups, const Args& a) {
		unsigned busy = a.busy ? a.busy : a.groups;
		flushes.resize(std::size_t(busy) * a.outstanding);
		for (unsigned g = 0; g < busy; ++g) {
			for (unsigned k = 0; k < a.outstanding; ++k) {
				Job& j = flushes[std::size_t(g) * a.outstanding + k];
				j.fn = run_again;
				j.group = groups[g];
				j.prio = PRIO_FLUSH;
				j.work_ns = std::uint64_t(a.flush_ms) * 1'000'000;
				j.pool = &pool;
				j.running = &running;
				j.latch = &latch;
			}
		}
		merges.resize(a.merges);
		for (unsigned m = 0; m < a.merges; ++m) {
			Job& j = merges[m];
			j.fn = run_again;
			j.group = groups[busy + m];
			j.prio = PRIO_MERGE;
			j.work_ns = std::uint64_t(a.merge_ms) * 1'000'000;
			j.pool = &pool;
			j.running = &running;
			j.latch = &latch;
		}
	}
	void start(Pool& pool) {
		latch.add(static_cast<std::uint32_t>(flushes.size() + merges.size()));
		for (auto& j : flushes) { pool.post(&j); }
		for (auto& j : merges) { pool.post(&j); }
	}
	void stop() {
		running.store(false, std::memory_order_relaxed);
		latch.wait();
	}
	std::uint64_t completed() const {
		std::uint64_t n = 0;
		for (auto& j : flushes) { n += j.count; }
		for (auto& j : merges) { n += j.count; }
		return n;
	}
	// Jain's fairness index over the busy groups' completed flush counts.
	double fairness(unsigned outstanding) const {
		std::size_t busy = flushes.size() / outstanding;
		if (busy < 2) { return 1.0; }
		double sum = 0, sq = 0;
		for (std::size_t g = 0; g < busy; ++g) {
			double c = 0;
			for (unsigned k = 0; k < outstanding; ++k) { c += double(flushes[g * outstanding + k].count); }
			sum += c;
			sq += c * c;
		}
		return sq > 0 ? (sum * sum) / (double(busy) * sq) : 1.0;
	}
};

// The latency tail of a query-class task while the background saturates the pool. A
// reactor-like thread posts one query at a time and waits for it.
void tail(const Args& a, Pool& pool) {
	Groups groups(pool, a.groups + a.merges + 1);
	Background bg(pool, groups, a);
	Group* qg = groups[a.groups + a.merges];
	bg.start(pool);
	sleep_ns(300'000'000);  // steady state
	std::vector<std::uint64_t> to_start, to_done;
	to_start.reserve(std::size_t(a.seconds * 1e6 / a.query_interval_us) + 16);
	to_done.reserve(to_start.capacity());
	Job q;
	q.fn = run_once;
	q.group = qg;
	q.prio = PRIO_QUERY;
	q.work_ns = std::uint64_t(a.query_us) * 1000;
	auto cpu0 = cpu_time_ns();
	auto end = now_ns() + std::uint64_t(a.seconds * 1e9);
	auto next = now_ns();
	while (now_ns() < end) {
		Latch l;
		l.add(1);
		q.latch = &l;
		q.posted = now_ns();
		pool.post(&q);
		l.wait();
		to_start.push_back(q.started - q.posted);
		to_done.push_back(q.finished - q.posted);
		next += std::uint64_t(a.query_interval_us) * 1000;
		auto now = now_ns();
		if (next > now) { sleep_ns(next - now); } else { next = now; }
	}
	auto cpu1 = cpu_time_ns();
	bg.stop();
	auto p1 = percentiles(to_start);
	auto p2 = percentiles(to_done);
	std::printf("tail pool=%-14s groups=%-6u busy=%-5u outstanding=%u merges=%u flush_ms=%u queries=%llu wait_p50=%s wait_p99=%s wait_p999=%s wait_max=%s done_p50=%s done_p99=%s done_p999=%s bg_tasks_per_s=%.0f cpu=%.1f%% fairness=%.3f\n",
		pool.name(), a.groups, a.busy ? a.busy : a.groups, a.outstanding, a.merges, a.flush_ms, static_cast<unsigned long long>(p1.n),
		fmt_ns(p1.p50).c_str(), fmt_ns(p1.p99).c_str(), fmt_ns(p1.p999).c_str(), fmt_ns(p1.max).c_str(),
		fmt_ns(p2.p50).c_str(), fmt_ns(p2.p99).c_str(), fmt_ns(p2.p999).c_str(),
		double(bg.completed()) / a.seconds, 100.0 * double(cpu1 - cpu0) / (a.seconds * 1e9 * pool.workers()), bg.fairness(a.outstanding));
}

////////////////////////////////////////////////////////////////////////////////

// One index's flush: flush_tasks tasks of flush_ms each, alone on the pool and then beside
// `groups` busy groups. Prints both makespans and the fair-share ideal of the second.
void makespan(const Args& a, Pool& pool) {
	Groups groups(pool, a.groups + 1);
	Group* probe = groups[a.groups];
	std::vector<Job> flush(a.flush_tasks);
	auto run_flush = [&] {
		Latch l;
		l.add(a.flush_tasks);
		for (auto& j : flush) {
			j.fn = run_once;
			j.group = probe;
			j.prio = PRIO_FLUSH;
			j.work_ns = std::uint64_t(a.flush_ms) * 1'000'000;
			j.latch = &l;
		}
		auto t0 = now_ns();
		for (auto& j : flush) { pool.post(&j); }
		l.wait();
		return now_ns() - t0;
	};
	std::vector<std::uint64_t> alone;
	for (unsigned i = 0; i < 5; ++i) { alone.push_back(run_flush()); sleep_ns(20'000'000); }
	std::sort(alone.begin(), alone.end());
	const double ideal_alone = double(a.flush_tasks) * a.flush_ms * 1e6 / std::min<unsigned>(pool.workers(), a.flush_tasks);

	Background bg(pool, groups, a);
	bg.start(pool);
	sleep_ns(300'000'000);
	auto before = bg.completed();
	auto t0 = now_ns();
	auto beside = run_flush();
	auto t1 = now_ns();
	auto during = bg.completed() - before;
	bg.stop();
	// Fair share: the probe is one of groups+1 busy groups sharing `workers` cores.
	unsigned busy = (a.busy ? a.busy : a.groups) + 1;
	const double share = double(pool.workers()) / double(busy);
	const double ideal_beside = double(a.flush_tasks) * a.flush_ms * 1e6 / std::min<double>(share, double(std::min<unsigned>(pool.workers(), a.flush_tasks)));
	std::printf("makespan pool=%-14s groups=%-6u flush=%ux%ums alone=%s (ideal %s) beside=%s (fair-share ideal %s, x%.2f) bg_tasks_per_s_during=%.0f fairness=%.3f\n",
		pool.name(), a.groups, a.flush_tasks, a.flush_ms, fmt_ns(alone[alone.size() / 2]).c_str(), fmt_ns(static_cast<std::uint64_t>(ideal_alone)).c_str(),
		fmt_ns(beside).c_str(), fmt_ns(static_cast<std::uint64_t>(ideal_beside)).c_str(), double(beside) / ideal_beside,
		double(during) / (double(t1 - t0) / 1e9), bg.fairness(a.outstanding));
}

////////////////////////////////////////////////////////////////////////////////

void idle(const Args& a, Pool& pool) {
	Groups groups(pool, a.groups);
	{
		Latch l;
		std::vector<Job> jobs(10'000);
		l.add(static_cast<std::uint32_t>(jobs.size()));
		for (std::size_t i = 0; i < jobs.size(); ++i) {
			jobs[i].fn = run_once;
			jobs[i].group = groups[i];
			jobs[i].latch = &l;
		}
		for (auto& j : jobs) { pool.post(&j); }
		l.wait();
	}
	sleep_ns(200'000'000);
	auto cpu0 = cpu_time_ns();
	auto cs0 = context_switches();
	auto t0 = now_ns();
	sleep_ns(std::uint64_t(a.seconds * 1e9));
	auto t1 = now_ns();
	auto cpu1 = cpu_time_ns();
	auto cs1 = context_switches();
	std::printf("idle pool=%-14s workers=%u seconds=%.1f cpu=%.4f%% (of one core) context_switches_per_s=%.1f\n",
		pool.name(), pool.workers(), double(t1 - t0) / 1e9, 100.0 * double(cpu1 - cpu0) / double(t1 - t0), double(cs1 - cs0) / (double(t1 - t0) / 1e9));
}

void memory(const Args& a, Pool& pool) {
	// Touch the allocator first, so the delta is the groups' and not the heap's warm-up.
	{ Groups warm(pool, 100); }
	auto rss0 = rss_bytes();
	auto* groups = new Groups(pool, a.groups);
	auto rss1 = rss_bytes();
	std::printf("memory pool=%-14s groups=%u sizeof_group=%zu rss_delta_per_group=%.0f bytes rss_delta=%.2f MiB\n",
		pool.name(), a.groups, pool.group_bytes(), double(rss1 - rss0) / a.groups, double(rss1 - rss0) / 1048576.0);
	delete groups;
}

}  // namespace

int main(int argc, char** argv) {
	Args a = parse(argc, argv);
	churn_per_ns();
	std::printf("workloads: %s threads=%u trials=%u pools=", a.workload.c_str(), a.threads, a.trials);
	for (auto& p : a.pools) { std::printf("%s,", p.c_str()); }
	std::printf("\n");
	for (unsigned trial = 0; trial < a.trials; ++trial) {
		for (auto& name : a.pools) {
			auto pool = make_pool(name, a.threads);
			if (a.workload == "tiny") { tiny(a, *pool); }
			else if (a.workload == "tail") { tail(a, *pool); }
			else if (a.workload == "makespan") { makespan(a, *pool); }
			else if (a.workload == "idle") { idle(a, *pool); }
			else if (a.workload == "memory") { memory(a, *pool); }
			else { std::fprintf(stderr, "unknown workload %s\n", a.workload.c_str()); return 2; }
			std::fflush(stdout);
		}
	}
	return 0;
}
