// Tests for the scheduling rules a host relies on: merge aging, a parse-ahead group beside an
// index's, the reserve under aging, the backlog report, waits that cannot deadlock (V2), and
// the block guard (V5). Each hang is a watchdog abort, not a stuck run.
// Build: c++ -std=c++20 -I.. work_scheduling.cc ../thread.cc -o work_scheduling && ./work_scheduling
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "work_pool.hh"

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

void burn(std::chrono::microseconds d) {
	auto end = Clock::now() + d;
	while (Clock::now() < end) {
	}
}

template <typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit) {
	auto end = Clock::now() + limit;
	while (!pred()) {
		if (Clock::now() > end) {
			return false;
		}
		std::this_thread::sleep_for(200us);
	}
	return true;
}

// Aborts the process if the test does not finish in time.
struct Watchdog {
	std::atomic<bool> done{false};
	std::thread thread;
	explicit Watchdog(int seconds, const char* what) :
		thread([this, seconds, what] {
			for (int i = 0; i < seconds * 10 && !done.load(); ++i) {
				std::this_thread::sleep_for(100ms);
			}
			if (!done.load()) {
				std::fprintf(stderr, "TIMEOUT: %s hung\n", what);
				std::abort();
			}
		}) {}
	~Watchdog() {
		done = true;
		thread.join();
	}
};

// Flushes that repost themselves in `groups` groups until stopped: the flush ring is never
// empty, so a merge ticket is picked only if aging puts it ahead.
struct Flood {
	std::atomic<bool> stop{false};
	std::atomic<unsigned> running{0};
	std::atomic<unsigned> peak{0};
	std::vector<std::unique_ptr<WorkGroup>> groups;
	std::vector<std::unique_ptr<WorkItem>> items;

	Flood(WorkPool& pool, unsigned n) {
		for (unsigned i = 0; i < n; ++i) {
			groups.push_back(std::make_unique<WorkGroup>(pool));
			items.push_back(std::make_unique<WorkItem>());
		}
		for (unsigned i = 0; i < n; ++i) {
			WorkItem* w = items[i].get();
			w->group = groups[i].get();
			w->cls = WorkClass::flush;
		}
	}
};

Flood* g_flood = nullptr;  // the flood the items belong to; one runs at a time

void note_peak(std::atomic<unsigned>& running, std::atomic<unsigned>& peak) {
	auto n = running.fetch_add(1) + 1;
	for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
	}
}

void flood_run(WorkItem* it) noexcept {
	Flood& f = *g_flood;
	note_peak(f.running, f.peak);
	burn(200us);
	f.running.fetch_sub(1);
	if (!f.stop.load()) {
		it->group->post(*it);
	}
}

void start(Flood& f) {
	g_flood = &f;
	for (auto& w : f.items) {
		w->run = flood_run;
		w->group->post(*w);
	}
}

// Stops the flood and waits for its items to end.
void finish(Flood& f) {
	f.stop = true;
	for (auto& g : f.groups) {
		g->wait();
	}
}

thread_local bool t_loop = false;
std::atomic<int> g_hits{0};
std::atomic<const char*> g_site{nullptr};

void block_guard_hit(const char* site) noexcept {
	if (t_loop) {
		g_site = site;
		g_hits.fetch_add(1);
	}
}

}  // namespace

int main() {
	// ---- 1. Without aging a flood of flushes starves a merge; with it the merge runs once
	// it is older than merge_age. ----
	{
		Watchdog dog(60, "aging");
		// The control, without aging: 32 gated flushes (one per group, none reposting) hold all four
		// workers, a merge is posted behind them, then the gate opens. Strict order starts the merge
		// after every flush but the up to threads - 1 popped before they counted themselves started.
		// A self-reposting flood would leave Ring::pop's unpublished-slot window open under load.
		{
			WorkPool pool({.threads = 4, .reserve = 0, .merge_age = 0ms});
			std::vector<std::unique_ptr<WorkGroup>> groups;
			for (int i = 0; i < 32; ++i) {
				groups.push_back(std::make_unique<WorkGroup>(pool));
			}
			std::atomic<bool> gate{false};
			std::atomic<int> started{0};
			std::atomic<int> merge_at{0};
			for (auto& g : groups) {
				g->enqueue([&] {
					started.fetch_add(1);
					while (!gate.load()) {
						std::this_thread::yield();
					}
				}, WorkClass::flush);
			}
			while (started.load() < 4) {
				std::this_thread::yield();
			}
			WorkGroup merges(pool);
			merges.enqueue([&] { merge_at = started.fetch_add(1) + 1; }, WorkClass::merge);
			gate = true;
			for (auto& g : groups) {
				g->wait();
			}
			merges.wait();
			assert(merge_at.load() >= 33 - 3);
			assert(pool.merges_aged() == 0);
			std::printf("work_scheduling OK: aging off: merge started %dth of 33\n", merge_at.load());
		}
		// With aging: under a flood the merge runs once it is older than merge_age.
		{
			WorkPool pool({.threads = 4, .reserve = 0, .merge_age = 20ms});
			Flood flood(pool, 32);
			start(flood);
			WorkGroup merges(pool);
			std::atomic<bool> ran{false};
			auto t0 = Clock::now();
			merges.enqueue([&] { ran = true; }, WorkClass::merge);
			bool in_time = wait_for([&] { return ran.load(); }, 400ms);
			auto waited = Clock::now() - t0;
			assert(in_time);
			assert(waited >= 15ms);  // it was not picked early: it was starved until it aged
			assert(pool.merges_aged() >= 1);
			finish(flood);
			merges.wait();
			assert(ran.load());
			std::printf("work_scheduling OK: aging on: merge ran while flushes flooded (waited %lld ms)\n",
				static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(waited).count()));
		}
	}

	// ---- 2. Every overdue merge entry is promoted, not one per interval, and merge_cap and
	// the reserve still hold. ----
	{
		Watchdog dog(60, "aging backlog");
		WorkPool pool({.threads = 4, .reserve = 1, .merge_cap = 2, .merge_age = 10ms});
		Flood flood(pool, 32);
		start(flood);
		std::vector<std::unique_ptr<WorkGroup>> groups;
		std::atomic<int> done{0};
		std::atomic<unsigned> merging{0};
		std::atomic<unsigned> merge_peak{0};
		for (int i = 0; i < 40; ++i) {
			groups.push_back(std::make_unique<WorkGroup>(pool));
			groups.back()->enqueue([&] {
				note_peak(merging, merge_peak);
				note_peak(flood.running, flood.peak);
				burn(1ms);
				flood.running.fetch_sub(1);
				merging.fetch_sub(1);
				done.fetch_add(1);
			}, WorkClass::merge);
		}
		auto t0 = Clock::now();
		bool all = wait_for([&] { return done.load() == 40; }, 5000ms);
		auto took = Clock::now() - t0;
		assert(all);
		assert(pool.merges_aged() >= 40);
		// Promoted back to back (about 20 ms of merging at two a time, after the first
		// aged); one per merge_age would take 400 ms.
		assert(took < 200ms);
		assert(merge_peak.load() <= 2);
		// Flushes and merges together never held the reserve: at most threads - reserve at once.
		assert(flood.peak.load() <= 3);
		finish(flood);
		for (auto& g : groups) {
			g->wait();
		}
		std::printf("work_scheduling OK: 40 starved merges all promoted in %lld ms (%llu aged turns), at most %u at once\n",
			static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()),
			static_cast<unsigned long long>(pool.merges_aged()), merge_peak.load());
	}

	// ---- 3. A query runs at once beside a saturated pool even while aging promotes merges. ----
	{
		Watchdog dog(60, "reserve under aging");
		WorkPool pool({.threads = 4, .reserve = 1, .merge_age = 5ms});
		Flood flood(pool, 32);
		start(flood);
		std::vector<std::unique_ptr<WorkGroup>> groups;
		std::atomic<int> merged{0};
		for (int i = 0; i < 40; ++i) {
			groups.push_back(std::make_unique<WorkGroup>(pool));
			groups.back()->enqueue([&] {
				note_peak(flood.running, flood.peak);
				burn(2ms);
				flood.running.fetch_sub(1);
				merged.fetch_add(1);
			}, WorkClass::merge);
		}
		WorkGroup queries(pool);
		Clock::duration worst{};
		for (int i = 0; i < 50; ++i) {
			std::atomic<bool> ran{false};
			auto t0 = Clock::now();
			queries.enqueue([&] { ran = true; }, WorkClass::query);
			bool ok = wait_for([&] { return ran.load(); }, 2000ms);
			assert(ok);
			worst = std::max(worst, Clock::now() - t0);
			std::this_thread::sleep_for(2ms);
		}
		assert(wait_for([&] { return merged.load() == 40; }, 5000ms));
		assert(pool.merges_aged() > 0);
		// The flood and the merges share three background slots between them.
		assert(flood.peak.load() <= 3);
		finish(flood);
		for (auto& g : groups) {
			g->wait();
		}
		queries.wait();
		std::printf("work_scheduling OK: 50 queries beside aged merges and a flood, worst wait %lld us\n",
			static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(worst).count()));
	}

	// ---- 4. The backlog report: waiting merge entries and the age of the oldest. ----
	{
		Watchdog dog(60, "merge backlog");
		WorkPool pool({.threads = 4, .reserve = 0, .merge_age = 60s});
		assert(pool.merge_waiting() == 0 && pool.merge_oldest() == 0ns);
		Flood flood(pool, 32);
		start(flood);
		std::vector<std::unique_ptr<WorkGroup>> groups;
		std::atomic<int> done{0};
		for (int i = 0; i < 5; ++i) {
			groups.push_back(std::make_unique<WorkGroup>(pool));
			groups.back()->enqueue([&] { done.fetch_add(1); }, WorkClass::merge);
		}
		std::this_thread::sleep_for(80ms);
		assert(pool.merge_waiting() == 5);
		assert(pool.merge_oldest() >= 60ms);
		assert(pool.merge_oldest() < 10s);
		assert(done.load() == 0);
		finish(flood);
		for (auto& g : groups) {
			g->wait();
		}
		assert(done.load() == 5);
		assert(pool.merge_waiting() == 0 && pool.merge_oldest() == 0ns);
		std::printf("work_scheduling OK: the backlog showed 5 merges, the oldest waiting 60 ms or more\n");
	}

	// ---- 5. Parse-ahead in its own group does not delay the index's refresh (C8.1): groups
	// take turns, while one group's lane is a FIFO. Counts and order only, no clock. ----
	{
		Watchdog dog(60, "parse-ahead");
		const unsigned threads = 1;  // one worker: the backlog and the refresh share it
		int own_between = -1;
		int shared_before = -1;
		for (bool own : {true, false}) {
			WorkPool pool({.threads = threads, .reserve = 0});
			WorkGroup index(pool);
			WorkGroup parse(pool);
			WorkGroup& ahead = own ? parse : index;
			std::atomic<int> parsed{0};
			for (int i = 0; i < 200; ++i) {
				ahead.enqueue([&] {
					burn(1ms);
					parsed.fetch_add(1);
				});
			}
			// The refresh is posted after some of the backlog ran, and after however much
			// more ran before it was posted; what it sees is measured against that.
			assert(wait_for([&] { return parsed.load() >= 3; }, 60000ms));
			int before = parsed.load();
			std::atomic<int> at_refresh{-1};
			index.enqueue([&] { at_refresh = parsed.load(); });
			assert(wait_for([&] { return at_refresh.load() >= 0; }, 60000ms));
			ahead.wait();
			index.wait();
			if (own) {
				own_between = at_refresh.load() - before;
				assert(own_between <= int(2 * threads + 2));  // a turn or two of parse-ahead
			} else {
				shared_before = at_refresh.load();
				// One FIFO: every parse item was queued ahead of it; at most threads - 1 were still
				// running when it started.
				assert(shared_before >= 200 - int(threads));
			}
		}
		std::printf("work_scheduling OK: %d parse items ran between the refresh's post and its run beside a parse-ahead group, all 200 first in one FIFO\n", own_between);
	}

	// ---- 6. V2: a worker waits on work queued only in a child group, in every class pairing,
	// on one worker and with width 1 (the own-group wait is an abort: test/work_wait.cc). ----
	{
		Watchdog dog(60, "V2");
		const WorkClass classes[] = {WorkClass::query, WorkClass::flush, WorkClass::merge};
		for (unsigned threads : {1u, 2u}) {
			for (auto outer : classes) {
				for (auto inner : classes) {
					WorkPool pool({.threads = threads});
					WorkGroup group(pool, 1);
					WorkGroup child(pool, 1);
					std::atomic<int> children{0};
					std::atomic<bool> done{false};
					group.enqueue([&] {
						for (int i = 0; i < 4; ++i) {
							child.enqueue([&] { children.fetch_add(1); }, inner);
						}
						child.wait();  // the V2 shape: waits on work only the child group holds
						done = children.load() == 4;
					}, outer);
					assert(wait_for([&] { return done.load(); }, 5000ms));
					group.wait();
				}
			}
		}
		std::printf("work_scheduling OK: V2: a worker waiting on its child group's queue completed in all class pairings\n");
	}

	// ---- 7. V5: a thread that is not a worker calls a waiting entry and hits the guard,
	// before it blocks; a worker's wait does not. ----
	{
		Watchdog dog(60, "V5");
		WorkPool pool({.threads = 2, .block_guard = block_guard_hit});
		WorkGroup group(pool);
		std::atomic<bool> release{false};
		group.enqueue([&] {
			while (!release.load()) {
				std::this_thread::sleep_for(100us);
			}
		});
		std::thread loop([&] {
			t_loop = true;
			std::thread releaser([&] {
				std::this_thread::sleep_for(20ms);
				release = true;
			});
			group.wait();  // the reactor's call: the guard fires on entry
			assert(g_hits.load() == 1);
			assert(std::string(g_site.load()) == "WorkGroup::wait");
			releaser.join();
		});
		loop.join();
		// A worker waiting on a child group is allowed; so is a non-loop thread here
		// (the test's guard counts only the loop thread).
		WorkGroup child(pool);
		std::atomic<bool> worker_done{false};
		group.enqueue([&] {
			t_loop = true;  // even a worker marked as a loop does not hit the guard
			child.enqueue([] {});
			child.wait();
			worker_done = true;
		});
		assert(wait_for([&] { return worker_done.load(); }, 5000ms));
		group.wait();
		assert(g_hits.load() == 1);
		std::printf("work_scheduling OK: V5: a non-worker thread's wait() hits the guard; a worker's did not\n");
	}

	std::printf("all work_scheduling tests passed\n");
	return 0;
}
