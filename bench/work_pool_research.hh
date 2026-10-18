// WorkPool behind the research harness's Pool interface (research/pools.hh on the
// research/scheduler branch), so bench/research_ab.sh runs it interleaved with today's
// pool and the prototypes over the same workloads. Bench code only.
#pragma once

#include "work_pool.hh"

namespace research {

class WorkPoolAdapter final : public Pool {
	struct G final : Group {
		WorkGroup wg;
		G(WorkPool& pool, unsigned width) :
			wg(pool, width) {}
	};

	WorkPool _pool;
	std::string _name;

	// Task embeds its WorkItem as its first member (research_ab.sh patches it in), so the
	// item and the task share an address and fn runs as the item's run directly: the
	// prototypes call fn once per task, and a trampoline would charge WorkPool a second
	// indirect call. Bench only: calling through the cast type is outside the standard.
	static auto as_run(void (*fn)(Task*)) noexcept {
		return reinterpret_cast<void (*)(WorkItem*) noexcept>(fn);
	}

public:
	WorkPoolAdapter(unsigned n, unsigned reserve) :
		_pool(WorkPool::Options{.threads = n, .reserve = reserve, .name_format = "P{:02}"}),
		_name(reserve == 1 ? "workpool" : "workpool-r" + std::to_string(reserve)) {}

	const char* name() const override { return _name.c_str(); }
	unsigned workers() const override { return _pool.threads(); }
	Group* open(unsigned width) override {
		auto* g = new G(_pool, width);
		g->width = width;
		return g;
	}
	void close(Group* g) override { delete static_cast<G*>(g); }
	void post(Task* t) override {
		t->wi.run = as_run(t->fn);
		t->wi.group = &static_cast<G*>(t->group)->wg;
		t->wi.cls = static_cast<WorkClass>(t->prio);  // PRIO_QUERY, PRIO_FLUSH, PRIO_MERGE
		_pool.post(t->wi);
	}
	std::size_t group_bytes() const override { return sizeof(G) + sizeof(threadpool_detail::GroupState); }
};

// "workpool" is the defaults (reserve 1, merges up to threads - 1: the prototypes' prio-r1);
// "workpool-r0" has no reserve (R2 alone), "workpool-r2" reserves two.
inline std::unique_ptr<Pool> make_work_pool(const std::string& name, unsigned n) {
	unsigned reserve = name == "workpool-r0" ? 0 : name == "workpool-r2" ? 2 : 1;
	return std::make_unique<WorkPoolAdapter>(n, reserve);
}

}  // namespace research
