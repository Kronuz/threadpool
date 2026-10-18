// WorkPool behind the research harness's Pool interface (research/pools.hh on the
// research/scheduler branch), so bench/research_ab.sh runs it interleaved with today's
// pool and the prototypes over the same workloads. Bench code only.
#pragma once

#include "work_pool.hh"

namespace research {

class WorkPoolAdapter final : public Pool {
	WorkPool _pool;

	// Task embeds its WorkItem as its first member (research_ab.sh patches it in).
	static void trampoline(WorkItem* w) noexcept {
		auto* t = reinterpret_cast<Task*>(w);
		t->fn(t);
	}

public:
	explicit WorkPoolAdapter(unsigned n) :
		_pool(WorkPool::Options{.threads = n, .name_format = "P{:02}"}) {}

	const char* name() const override { return "workpool"; }
	unsigned workers() const override { return _pool.threads(); }
	Group* open(unsigned width) override {
		auto* g = new Group;
		g->width = width;
		return g;
	}
	void close(Group* g) override { delete g; }
	void post(Task* t) override {
		t->wi.run = trampoline;
		_pool.post(t->wi);
	}
	std::size_t group_bytes() const override { return sizeof(Group); }
};

inline std::unique_ptr<Pool> make_work_pool(unsigned n) { return std::make_unique<WorkPoolAdapter>(n); }

}  // namespace research
