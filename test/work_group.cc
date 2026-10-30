// Tests for WorkGroup: fan-out, turns, width, weight, cancel, helping waits, the destructor
// (also from its own item), sleep_until, groups opening under load, and the classes.
// Build: c++ -std=c++20 -I.. work_group.cc ../thread.cc -o work_group && ./work_group
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "work_pool.hh"

using namespace std::chrono_literals;

namespace {

void burn(std::chrono::microseconds d) {
	auto end = std::chrono::steady_clock::now() + d;
	while (std::chrono::steady_clock::now() < end) {
	}
}

template <typename Pred>
void spin_until(Pred&& pred) {
	while (!pred()) {
		std::this_thread::sleep_for(100us);
	}
}

}  // namespace

int main() {
	// ---- 1. One group alone takes every worker at once (no reserve). ----
	{
		WorkPool pool({.threads = 4, .reserve = 0});
		WorkGroup group(pool);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		const auto end = std::chrono::steady_clock::now() + 10s;  // one deadline for the whole case
		for (int i = 0; i < 64; ++i) {
			group.enqueue([&] {
				auto n = now.fetch_add(1) + 1;
				for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
				}
				// Holds the worker until all four run at once (or 10 s pass), so a slow host cannot drain the
				// queue on fewer workers before the last one wakes.
				while (peak.load() < 4 && std::chrono::steady_clock::now() < end) {
					std::this_thread::yield();
				}
				now.fetch_sub(1);
			});
		}
		group.wait();
		assert(peak.load() == 4);
		std::printf("work_group OK: one group alone ran on all %u workers at once\n", peak.load());
	}

	// ---- 2. Groups take turns: a late group's item waits for one turn, not the backlog. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup a(pool);
		WorkGroup b(pool);
		std::atomic<int> a_done{0};
		std::atomic<int> a_seen{-1};
		for (int i = 0; i < 200; ++i) {
			a.enqueue([&] {
				burn(200us);
				a_done.fetch_add(1);
			});
		}
		b.enqueue([&] { a_seen = a_done.load(); });
		b.wait();
		a.wait();
		assert(a_seen.load() >= 0 && a_seen.load() <= 10);
		std::printf("work_group OK: a late group ran after %d of the busy group's 200 items\n", a_seen.load());
	}

	// ---- 3. Width caps how many of a group's items run at once. ----
	for (unsigned width : {1u, 2u}) {
		WorkPool pool({.threads = 4});
		WorkGroup group(pool, width);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		for (int i = 0; i < 40; ++i) {
			group.enqueue([&] {
				auto n = now.fetch_add(1) + 1;
				for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
				}
				burn(500us);
				now.fetch_sub(1);
			});
		}
		group.wait();
		assert(peak.load() >= 1 && peak.load() <= width);
		std::printf("work_group OK: width %u held the group to %u at once\n", width, peak.load());
	}

	// ---- 4. Weight: a weight-2 group gets about twice a weight-1 group's service. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup heavy(pool, 0, 2);
		WorkGroup light(pool, 0, 1);
		std::atomic<int> h{0};
		std::atomic<int> l{0};
		std::atomic<bool> stop{false};
		for (int i = 0; i < 6000; ++i) {
			heavy.enqueue([&] {
				if (!stop.load()) {
					burn(50us);
					h.fetch_add(1);
				}
			});
			light.enqueue([&] {
				if (!stop.load()) {
					burn(50us);
					l.fetch_add(1);
				}
			});
		}
		// Sampled at a fixed count of the light group's items, so the ratio averages over
		// thousands of turns whatever the clock does.
		spin_until([&] { return l.load() >= 1500; });
		stop = true;
		auto lc = l.load();
		auto hc = h.load();
		heavy.wait();
		light.wait();
		double ratio = double(hc) / double(lc);
		std::printf("work_group OK: weight 2 against 1 ran %d against %d items (%.2fx)\n", hc, lc, ratio);
		assert(ratio > 1.5 && ratio < 3.0);
	}

	// ---- 5. cancel() hands back the queued items; the running one sees its stop. ----
	{
		WorkPool pool({.threads = 2});
		WorkGroup group(pool, 1);
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		std::atomic<bool> saw_stop{false};
		std::atomic<int> ran{0};

		group.enqueue([&](const WorkItem& self) {
			started.set_value();
			release_future.wait();
			saw_stop = self.stop_requested() && group.stop_requested();
		});
		struct Plain : WorkItem {
			std::atomic<int>* ran;
		};
		std::vector<Plain> plain(10);
		for (auto& p : plain) {
			p.ran = &ran;
			p.run = [](WorkItem* w) noexcept { static_cast<Plain*>(w)->ran->fetch_add(1); };
			group.post(p);
		}
		started_future.wait();
		std::size_t dropped = 0;
		for (WorkItem* item = group.cancel(); item; item = item->next) {
			assert(dropped < plain.size() && item == &plain[dropped]);
			++dropped;
		}
		assert(dropped == 10);
		release.set_value();
		group.wait();
		assert(saw_stop.load());
		assert(ran.load() == 0);
		group.post(plain[0]);
		group.wait();
		assert(ran.load() == 1);
		std::printf("work_group OK: cancel() handed back %zu queued items in order; the running one saw stop\n", dropped);
	}

	// ---- 6. wait() from an item on a child group serves it, even on a one-worker pool. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup group(pool);
		WorkGroup halves_group(pool);
		std::atomic<int> halves{0};
		std::atomic<bool> done{false};
		group.enqueue([&] {
			halves_group.enqueue([&] { halves.fetch_add(1); });
			halves_group.enqueue([&] { halves.fetch_add(1); });
			halves_group.wait();  // waits for the halves, in a group of their own
			done = halves.load() == 2;
		});
		spin_until([&] { return done.load(); });
		group.wait();
		std::printf("work_group OK: a wait on a child group ran its halves inline\n");
	}

	// ---- 7. wait() on another group from a worker serves that group. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup outer(pool);
		WorkGroup inner(pool);
		std::atomic<int> ran{0};
		std::atomic<bool> done{false};
		outer.enqueue([&] {
			for (int i = 0; i < 5; ++i) {
				inner.enqueue([&] { ran.fetch_add(1); });
			}
			inner.wait();
			done = ran.load() == 5;
		});
		spin_until([&] { return done.load(); });
		outer.wait();
		std::printf("work_group OK: a worker waiting on another group served it instead of blocking\n");
	}

	// ---- 8. The destructor frees queued boxes unrun and waits for the running one. ----
	{
		WorkPool pool({.threads = 2});
		auto probe = std::make_shared<int>(0);
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		std::atomic<bool> finished{false};
		std::atomic<int> ran{0};
		auto* group = new WorkGroup(pool, 1);
		group->enqueue([&] {
			started.set_value();
			release_future.wait();
			finished = true;
		});
		for (int i = 0; i < 10; ++i) {
			group->enqueue([probe, &ran] { ran.fetch_add(1); });
		}
		started_future.wait();
		std::thread destroyer([group] { delete group; });
		spin_until([&] { return probe.use_count() == 1; });
		release.set_value();
		destroyer.join();
		assert(finished.load());
		assert(ran.load() == 0);
		std::printf("work_group OK: the destructor freed 10 queued boxes unrun and waited for the running one\n");
	}

	// ---- 9. A group destroyed from an item of another group (from its own item aborts:
	// test/work_wait.cc). ----
	{
		WorkPool pool({.threads = 2});
		WorkGroup owner(pool);
		std::atomic<bool> done{false};
		auto* group = new WorkGroup(pool);
		owner.enqueue([group, &done] {
			delete group;
			done = true;
		});
		spin_until([&] { return done.load(); });
		owner.wait();
		std::printf("work_group OK: a group was destroyed from another group's item\n");
	}

	// ---- 10. sleep_until: the deadline returns true; a cancel ends it early with false. ----
	{
		WorkPool pool({.threads = 2});
		WorkGroup group(pool);
		std::atomic<int> slept{-1};
		std::atomic<int> cancelled{-1};
		std::atomic<bool> sleeping{false};
		group.enqueue([&] {
			auto t0 = std::chrono::steady_clock::now();
			bool ok = pool.sleep_until(t0 + 20ms, group);
			slept = ok && std::chrono::steady_clock::now() - t0 >= 20ms;
		});
		group.wait();
		assert(slept.load() == 1);
		group.enqueue([&] {
			sleeping = true;
			auto t0 = std::chrono::steady_clock::now();
			bool ok = pool.sleep_until(t0 + 30s, group);
			cancelled = !ok && std::chrono::steady_clock::now() - t0 < 10s;
		});
		spin_until([&] { return sleeping.load(); });
		std::this_thread::sleep_for(10ms);
		group.cancel();
		group.wait();
		assert(cancelled.load() == 1);
		std::printf("work_group OK: sleep_until met its deadline, and a cancel woke it early\n");
	}

	// ---- 11. Thousands of groups open while others post (the ring grows under load). ----
	{
		WorkPool pool({.threads = 4});
		std::atomic<std::uint64_t> ran{0};
		std::vector<std::thread> producers;
		for (int p = 0; p < 4; ++p) {
			producers.emplace_back([&] {
				std::vector<std::unique_ptr<WorkGroup>> groups;
				for (int g = 0; g < 2500; ++g) {
					groups.push_back(std::make_unique<WorkGroup>(pool));
					for (int i = 0; i < 4; ++i) {
						groups.back()->enqueue([&] { ran.fetch_add(1, std::memory_order_relaxed); });
					}
				}
				for (auto& g : groups) {
					g->wait();
				}
			});
		}
		for (auto& t : producers) {
			t.join();
		}
		assert(ran.load() == 4 * 2500 * 4);
		std::printf("work_group OK: 10000 groups opened under load ran all %llu items\n", static_cast<unsigned long long>(ran.load()));
	}

	// ---- 12. The reserve: flushes hold threads - reserve workers; a query runs beside them. ----
	{
		WorkPool pool({.threads = 4, .reserve = 1});
		WorkGroup flushes(pool);
		WorkGroup queries(pool);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		std::atomic<unsigned> beside{0};
		for (int i = 0; i < 12; ++i) {
			flushes.enqueue([&] {
				auto n = now.fetch_add(1) + 1;
				for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
				}
				burn(20ms);
				now.fetch_sub(1);
			});
		}
		spin_until([&] { return now.load() == 3; });
		queries.enqueue([&] { beside = now.load(); }, WorkClass::query);
		queries.wait();
		flushes.wait();
		assert(peak.load() == 3);
		assert(beside.load() == 3);
		std::printf("work_group OK: flushes held 3 of 4 workers; a query ran beside all 3\n");
	}

	// ---- 13. Class order: queries, then flushes, then merges. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup blocker(pool);
		WorkGroup q(pool);
		WorkGroup f(pool);
		WorkGroup m(pool);
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		std::mutex mu;
		std::vector<char> order;
		auto record = [&](char c) {
			std::lock_guard<std::mutex> lk(mu);
			order.push_back(c);
		};
		blocker.enqueue([&] {
			started.set_value();
			release_future.wait();
		});
		started_future.wait();
		for (int i = 0; i < 3; ++i) {
			m.enqueue([&] { record('m'); }, WorkClass::merge);
			f.enqueue([&] { record('f'); }, WorkClass::flush);
			q.enqueue([&] { record('q'); }, WorkClass::query);
		}
		release.set_value();
		m.wait();
		f.wait();
		q.wait();
		blocker.wait();
		assert((order == std::vector<char>{'q', 'q', 'q', 'f', 'f', 'f', 'm', 'm', 'm'}));
		std::printf("work_group OK: queries ran before flushes, flushes before merges\n");
	}

	// ---- 14. merge_cap: merges hold at most that many workers; flushes run beside them. ----
	{
		WorkPool pool({.threads = 4, .reserve = 0, .merge_cap = 1});
		std::vector<std::unique_ptr<WorkGroup>> merges;
		WorkGroup flushes(pool);
		std::atomic<unsigned> now{0};
		std::atomic<unsigned> peak{0};
		std::atomic<int> flushed{0};
		for (int g = 0; g < 4; ++g) {
			merges.push_back(std::make_unique<WorkGroup>(pool));
			for (int i = 0; i < 3; ++i) {
				merges.back()->enqueue([&] {
					auto n = now.fetch_add(1) + 1;
					for (auto p = peak.load(); n > p && !peak.compare_exchange_weak(p, n);) {
					}
					burn(2ms);
					now.fetch_sub(1);
				}, WorkClass::merge);
			}
		}
		for (int i = 0; i < 20; ++i) {
			flushes.enqueue([&] { flushed.fetch_add(1); });
		}
		flushes.wait();
		for (auto& g : merges) {
			g->wait();
		}
		assert(peak.load() == 1);
		assert(flushed.load() == 20);
		std::printf("work_group OK: merge_cap 1 held merges to one worker while flushes ran\n");
	}

	// ---- 15. cancel() across classes: queries, then flushes, then merges. ----
	{
		WorkPool pool({.threads = 1});
		WorkGroup blocker(pool);
		WorkGroup group(pool);
		std::promise<void> started;
		std::promise<void> release;
		auto started_future = started.get_future();
		auto release_future = release.get_future().share();
		blocker.enqueue([&] {
			started.set_value();
			release_future.wait();
		});
		started_future.wait();
		struct Tagged : WorkItem {
			char tag;
		};
		Tagged items[3];
		const WorkClass classes[3] = {WorkClass::merge, WorkClass::flush, WorkClass::query};
		const char tags[3] = {'m', 'f', 'q'};
		for (int i = 0; i < 3; ++i) {
			items[i].tag = tags[i];
			items[i].cls = classes[i];
			items[i].run = [](WorkItem*) noexcept {};
			group.post(items[i]);
		}
		std::vector<char> got;
		for (WorkItem* item = group.cancel(); item; item = item->next) {
			got.push_back(static_cast<Tagged*>(item)->tag);
		}
		release.set_value();
		blocker.wait();
		assert((got == std::vector<char>{'q', 'f', 'm'}));
		std::printf("work_group OK: cancel() handed back queries, flushes, then merges\n");
	}

	// ---- 16. Churn (the note's correction 8), under a watchdog: pools and groups made and
	// destroyed right after their last item reports, so a stop that loses its wakeup hangs
	// and a group freed under a worker's ticket trips ASan. ----
	{
		std::atomic<bool> finished{false};
		std::thread watchdog([&] {
			for (int i = 0; i < 1200 && !finished.load(); ++i) {
				std::this_thread::sleep_for(100ms);
			}
			if (!finished.load()) {
				std::fprintf(stderr, "TIMEOUT: work_group churn hung\n");
				std::abort();
			}
		});
		for (int cycle = 0; cycle < 200; ++cycle) {
			WorkPool pool({.threads = 1 + unsigned(cycle % 4), .reserve = unsigned(cycle % 2)});
			for (int round = 0; round < 4; ++round) {
				std::atomic<int> ran{0};
				auto* group = new WorkGroup(pool);
				for (int i = 0; i < 32; ++i) {
					group->enqueue([&ran] { ran.fetch_add(1, std::memory_order_release); },
						i % 3 == 0 ? WorkClass::query : WorkClass::flush);
				}
				// Destroyed as soon as the last item reports, while workers may still hold
				// its tickets (fan-out put several on the rings).
				while (ran.load(std::memory_order_acquire) < 32) {
					threadpool_detail::cpu_relax();
				}
				delete group;
			}
		}
		finished = true;
		watchdog.join();
		std::printf("work_group OK: 200 pools and 800 groups churned without a hang or a use after free\n");
	}

	// ---- 17. Every cancel path frees the queued boxed items and leaves the owner's (L2-B5). ----
	{
		static std::atomic<int> live{0};
		struct Tracked {
			Tracked() { live.fetch_add(1); }
			Tracked(const Tracked&) { live.fetch_add(1); }
			~Tracked() { live.fetch_sub(1); }
		};
		struct Plain : WorkItem {
			std::atomic<int>* ran;
		};
		std::atomic<int> ran{0};
		std::vector<Plain> plain(3);
		for (auto& p : plain) {
			p.ran = &ran;
			p.run = [](WorkItem* w) noexcept { static_cast<Plain*>(w)->ran->fetch_add(1); };
		}
		for (int path = 0; path < 3; ++path) {
			WorkPool pool({.threads = 2});
			std::atomic<bool> started{false};
			std::size_t returned = 0;
			std::size_t dropped = 0;
			{
				WorkGroup group(pool, 1);
				group.enqueue([&] {
					started = true;
					while (!group.stop_requested()) {  // the cancel ends it
						std::this_thread::yield();
					}
				});
				spin_until([&] { return started.load(); });
				for (int i = 0; i < 5; ++i) {
					group.enqueue([t = Tracked()] {});
					if (i < 3) {
						group.post(plain[i]);
					}
				}
				assert(live.load() == 5);
				if (path == 0) {
					dropped = group.cancel_and_wait();
					assert(live.load() == 0);
				} else if (path == 1) {
					for (WorkItem* item = group.cancel(); item; item = item->next) {
						assert(item == &plain[returned]);
						++returned;
					}
					assert(live.load() == 0);
					group.wait();
				}
			}  // path 2: the destructor cancels
			assert(live.load() == 0);
			assert(ran.load() == 0);
			assert(path != 0 || dropped == 8);
			assert(path != 1 || returned == 3);
		}
		std::printf("work_group OK: cancel(), cancel_and_wait() and the destructor all free queued boxes, leave the owner's\n");
	}

	// ---- 18. Work conservation (L2-B6): a worker that idles while a group has queued items
	// under its width takes one, even when no turn of that group ends meanwhile. ----
	{
		const auto limit = 10s;
		// (a) One group of long items runs as wide as the pool, from 1 to 8 workers.
		for (unsigned n = 1; n <= 8; ++n) {
			for (int rep = 0; rep < 20; ++rep) {
				WorkPool pool({.threads = n, .reserve = 0});
				WorkGroup group(pool);
				std::atomic<unsigned> now{0};
				std::atomic<unsigned> peak{0};
				const auto end = std::chrono::steady_clock::now() + limit;
				for (int i = 0; i < 64; ++i) {
					group.enqueue([&] {
						auto k = now.fetch_add(1) + 1;
						for (auto p = peak.load(); k > p && !peak.compare_exchange_weak(p, k);) {
						}
						while (peak.load() < n && std::chrono::steady_clock::now() < end) {
							std::this_thread::yield();
						}
						now.fetch_sub(1);
					});
				}
				group.wait();
				assert(peak.load() == n);
			}
		}
		// (b) Posted while every worker is busy, so nothing could be granted a worker then:
		// the others finish their items and the group widens with no turn of its own ending.
		for (int rep = 0; rep < 50; ++rep) {
			WorkPool pool({.threads = 4, .reserve = 0});
			WorkGroup holders(pool);
			WorkGroup group(pool);
			std::atomic<unsigned> held{0};
			std::atomic<bool> release{false};
			const auto end = std::chrono::steady_clock::now() + limit;
			for (int i = 0; i < 3; ++i) {
				holders.enqueue([&] {
					held.fetch_add(1);
					while (!release && std::chrono::steady_clock::now() < end) {
						std::this_thread::yield();
					}
				});
			}
			spin_until([&] { return held.load() == 3; });
			std::atomic<unsigned> now{0};
			std::atomic<unsigned> peak{0};
			for (int i = 0; i < 16; ++i) {
				group.enqueue([&] {
					auto k = now.fetch_add(1) + 1;
					for (auto p = peak.load(); k > p && !peak.compare_exchange_weak(p, k);) {
					}
					release = true;
					while (peak.load() < 4 && std::chrono::steady_clock::now() < end) {
						std::this_thread::yield();
					}
					now.fetch_sub(1);
				});
			}
			group.wait();
			holders.wait();
			assert(peak.load() == 4);
		}
		std::printf("work_group OK: groups of long items ran as wide as the pool at 1 to 8 workers, and widened as workers freed up\n");
	}

	std::printf("all work_group tests passed\n");
	return 0;
}
