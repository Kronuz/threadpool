// Tests for WorkGroup::pending() and running(): the backlog and load a host reports.
// Build: c++ -std=c++20 -I.. work_counts.cc ../thread.cc -o work_counts && ./work_counts
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "work_pool.hh"

using namespace std::chrono_literals;

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", #cond, __FILE__, __LINE__); \
			std::abort(); \
		} \
	} while (0)

int main() {
	WorkPool pool({.threads = 2, .reserve = 0});
	WorkGroup group(pool);
	WorkGroup other(pool);
	CHECK(group.pending() == 0 && group.running() == 0);

	// Two items hold both workers; five more wait behind them.
	std::atomic<bool> release{false};
	std::atomic<int> held{0};
	for (int i = 0; i < 7; ++i) {
		group.enqueue([&] {
			held.fetch_add(1);
			while (!release.load()) {
				std::this_thread::sleep_for(100us);
			}
		});
	}
	while (held.load() < 2) {
		std::this_thread::sleep_for(100us);
	}
	CHECK(group.running() == 2);
	CHECK(group.pending() == 5);
	// Another group's counts are its own.
	CHECK(other.running() == 0 && other.pending() == 0);

	// cancel() drops the queued ones; the running ones stay counted.
	group.cancel();
	CHECK(group.pending() == 0);
	CHECK(group.running() == 2);

	release = true;
	group.wait();
	CHECK(group.pending() == 0 && group.running() == 0);
	std::printf("work_counts OK: pending and running follow queued, started, cancelled and finished items per group\n");
	return 0;
}
