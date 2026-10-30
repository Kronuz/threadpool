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
		for (bool aging : {false, true}) {
			WorkPool pool({.threads = 4, .reserve = 0, .merge_age = aging ? 20ms : 0ms});
			Flood flood(pool, 32);
			start(flood);
			WorkGroup merges(pool);
			std::atomic<bool> ran{false};
			auto t0 = Clock::now();
			merges.enqueue([&] { ran = true; }, WorkClass::merge);
			bool in_time = wait_for([&] { return ran.load(); }, 400ms);
			auto waited = Clock::now() - t0;
			if (aging) {
				assert(in_time);
				assert(waited >= 15ms);  // it was not picked early: it was starved until it aged
				assert(pool.merges_aged() >= 1);
			} else {
				assert(!in_time);  // the control: strict order starves it
				assert(pool.merges_aged() == 0);
				assert(pool.merge_waiting() == 1);
			}
			finish(flood);
			merges.wait();
			assert(ran.load());
			std::printf("work_scheduling OK: aging %s: merge %s (waited %lld ms)\n", aging ? "on" : "off",
				in_time ? "ran while flushes flooded" : "starved until the flood stopped",
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

	// ---- 6. V2: a worker waits on work queued only in its own group, in every class pairing,
	// on one worker and with width 1; nested ten deep. ----
	{
		Watchdog dog(60, "V2");
		const WorkClass classes[] = {WorkClass::query, WorkClass::flush, WorkClass::merge};
		for (unsigned threads : {1u, 2u}) {
			for (auto outer : classes) {
				for (auto inner : classes) {
					WorkPool pool({.threads = threads});
					WorkGroup group(pool, 1);
					std::atomic<int> children{0};
					std::atomic<bool> done{false};
					group.enqueue([&] {
						for (int i = 0; i < 4; ++i) {
							group.enqueue([&] { children.fetch_add(1); }, inner);
						}
						group.wait();  // the V2 shape: waits on work only this group holds
						done = children.load() == 4;
					}, outer);
					assert(wait_for([&] { return done.load(); }, 5000ms));
					group.wait();
				}
			}
		}
		WorkPool pool({.threads = 1});
		WorkGroup group(pool, 1);
		std::atomic<int> depth{0};
		std::atomic<bool> done{false};
		struct Nest {
			WorkGroup& g;
			std::atomic<int>& depth;
			void operator()(int level) {
				depth.fetch_add(1);
				if (level < 10) {
					g.enqueue([this, level] { (*this)(level + 1); });
					g.wait();
				}
			}
		} nest{group, depth};
		group.enqueue([&] {
			nest(1);
			done = true;
		});
		assert(wait_for([&] { return done.load(); }, 5000ms));
		group.wait();
		assert(depth.load() == 10);
		std::printf("work_scheduling OK: V2: a worker waiting on its own group's queue completed in all class pairings and ten deep\n");
	}

	// ---- 6b. Items of one group that wait together do not wait on each other (a flush chore
	// and a merge chore of one index both calling wait()); an outside wait still ends only
	// when they have finished. ----
	{
		Watchdog dog(60, "concurrent waits");
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup group(pool);
		std::atomic<int> waiting{0};
		std::atomic<int> after{0};
		for (int i = 0; i < 3; ++i) {
			group.enqueue([&] {
				waiting.fetch_add(1);
				auto end = Clock::now() + 200ms;
				while (waiting.load() < 3 && Clock::now() < end) {
					std::this_thread::sleep_for(100us);
				}
				group.wait();  // every other running item is in this call too
				std::this_thread::sleep_for(20ms);
				after.fetch_add(1);
			}, static_cast<WorkClass>(i));
		}
		group.wait();  // from outside: returns when the items have finished, not before
		assert(after.load() == 3);
		std::printf("work_scheduling OK: three items of one group waited together, and an outside wait saw them finish\n");
	}

	// ---- 6c. A wait that found the group quiet ends the other waiters' waits with it, even
	// when the first to return then blocks on the second (each sets its flag after wait() and
	// spins for the other's). Only rounds where both items really ran at once count; a pool
	// does not fan out to an idle worker until a turn ends, so some rounds run one at a time. ----
	{
		Watchdog dog(60, "post-wait siblings");
		WorkPool pool({.threads = 2, .reserve = 0});
		std::atomic<int> overlapped{0};
		for (int round = 0; round < 400; ++round) {
			WorkGroup group(pool);
			std::atomic<int> started{0};
			std::atomic<int> decision{0};
			std::atomic<bool> flag[2] = {false, false};
			for (int i = 0; i < 2; ++i) {
				group.enqueue([&, i] {
					started.fetch_add(1);
					auto end = Clock::now() + 20ms;
					while (started.load() < 2 && Clock::now() < end) {
						std::this_thread::yield();
					}
					// The first to leave the barrier decides for both.
					int verdict = 0;
					decision.compare_exchange_strong(verdict, started.load() >= 2 ? 1 : 2);
					bool both = decision.load() == 1;
					group.wait();
					if (both) {
						flag[i] = true;
						while (!flag[1 - i].load()) {
							std::this_thread::yield();
						}
						if (i == 0) {
							overlapped.fetch_add(1);
						}
					}
				});
			}
			group.wait();
		}
		assert(overlapped.load() >= 1);
		std::printf("work_scheduling OK: siblings that block on each other after wait() ended (%d of 400 rounds overlapped)\n", overlapped.load());
	}

	// ---- 6c2. An item that waits on another group counts as a waiter in its own group: a
	// query of its group served inline on its stack (while it waits) can finish its own wait,
	// where waiting for the item below it would never end. ----
	{
		Watchdog dog(60, "cross wait then own wait");
		WorkPool pool({.threads = 2, .reserve = 0});
		WorkGroup g(pool);
		WorkGroup h(pool);
		std::atomic<bool> c_started{false};
		std::atomic<bool> release{false};
		std::atomic<bool> done{false};
		h.enqueue([&] {
			c_started = true;
			while (!release.load()) {
				std::this_thread::yield();
			}
		});
		assert(wait_for([&] { return c_started.load(); }, 60000ms));
		g.enqueue([&] {
			g.enqueue([&] {
				g.wait();  // the item below, in h.wait(), is suspended too
				release = true;
			}, WorkClass::query);
			h.wait();
			done = true;
		});
		g.wait();
		assert(done.load());
		h.wait();
		std::printf("work_scheduling OK: a query served inline under a cross-group wait ended its own wait\n");
	}

	// ---- 6d. A worker waits on another group with a width, all workers waiting at once
	// (the ring ticket of a width-capped group is not a running turn). ----
	{
		Watchdog dog(60, "width cross wait");
		for (unsigned threads : {1u, 2u, 4u}) {
			for (unsigned width : {0u, 1u, 2u, 3u}) {
				WorkPool pool({.threads = threads});
				WorkGroup h(pool, width);
				WorkGroup p(pool);
				std::atomic<int> ran{0};
				for (unsigned i = 0; i < threads; ++i) {
					p.enqueue([&] {
						std::this_thread::sleep_for(30ms);  // every worker is inside a p item
						h.enqueue([&] { ran.fetch_add(1); });
						h.wait();
					});
				}
				p.wait();
				assert(ran.load() == int(threads));
			}
		}
		std::printf("work_scheduling OK: workers waiting on a width-capped group completed at 1, 2 and 4 threads\n");
	}

	// ---- 6e. Waiting helpers keep the width: at most one item of a width-1 group at a time. ----
	{
		Watchdog dog(60, "width under helpers");
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup h(pool, 1);
		WorkGroup p(pool);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		for (int i = 0; i < 4; ++i) {
			p.enqueue([&] {
				for (int k = 0; k < 60; ++k) {
					h.enqueue([&] {
						note_peak(now, peak);
						burn(100us);
						now.fetch_sub(1);
					});
					h.wait();
				}
			});
		}
		p.wait();
		assert(peak.load() == 1);
		std::printf("work_scheduling OK: width 1 held under 4 waiting helpers (peak %u)\n", peak.load());
	}

	// ---- 6f. Waits nest deeper than a stack allows: 20,000 queued items that each wait on
	// their group, and a chain of 300 groups each waited on from the previous one's item, on
	// one worker. Past max_inline_depth a rescue thread serves, so none deadlocks or
	// overflows. ----
	{
		Watchdog dog(120, "deep waits");
		{
			WorkPool pool({.threads = 1});
			WorkGroup group(pool);
			std::atomic<int> done{0};
			for (int i = 0; i < 20000; ++i) {
				group.enqueue([&] {
					group.wait();
					done.fetch_add(1);
				});
			}
			group.wait();
			assert(wait_for([&] { return done.load() == 20000; }, 60000ms));
		}
		{
			constexpr int links = 300;
			WorkPool pool({.threads = 1});
			std::vector<std::unique_ptr<WorkGroup>> chain;
			for (int i = 0; i < links; ++i) {
				chain.push_back(std::make_unique<WorkGroup>(pool));
			}
			std::atomic<int> done{0};
			struct Link {
				std::vector<std::unique_ptr<WorkGroup>>& chain;
				std::atomic<int>& done;
				void operator()(int i) {
					if (i + 1 < links) {
						chain[i + 1]->enqueue([this, i] { (*this)(i + 1); });
						chain[i + 1]->wait();  // not this item's group: served as a helper
					}
					done.fetch_add(1);
				}
			} link{chain, done};
			chain[0]->enqueue([&] { link(0); });
			chain[0]->wait();
			assert(wait_for([&] { return done.load() == links; }, 60000ms));
		}
		std::printf("work_scheduling OK: 20,000 self-waiting items and a 300-group wait chain finished on one worker\n");
	}

	// ---- 7. V5: a thread that is not a worker calls a waiting entry and hits the guard,
	// before it blocks; a worker's wait and a quiet thread (the guard decides) do not. ----
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
			pool.help_until([&] { return release.load(); });
			assert(g_hits.load() == 2);
			assert(std::string(g_site.load()) == "WorkPool::help_until");
			releaser.join();
		});
		loop.join();
		// A worker waiting on its own group's child is allowed; so is a non-loop thread here
		// (the test's guard counts only the loop thread).
		std::atomic<bool> worker_done{false};
		group.enqueue([&] {
			t_loop = true;  // even a worker marked as a loop does not hit the guard
			group.enqueue([] {});
			group.wait();
			pool.help_until([] { return true; });
			worker_done = true;
		});
		assert(wait_for([&] { return worker_done.load(); }, 5000ms));
		group.wait();
		assert(g_hits.load() == 2);
		std::printf("work_scheduling OK: V5: a non-worker thread's wait() and help_until() hit the guard; a worker's did not\n");
	}

	std::printf("all work_scheduling tests passed\n");
	return 0;
}
