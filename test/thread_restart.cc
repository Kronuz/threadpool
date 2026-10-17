// Thread<> restarts: run -> join -> run runs again with a fresh promise; a run() after a run
// ended without a join joins it first, and rethrows what it threw; run while running is a
// no-op.
// Run under TSan as well: the restart must not race the previous runner's promise.
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>

#include "thread.hh"

using namespace std::chrono_literals;

struct Worker : Thread<Worker, ThreadPolicyType::regular> {
	std::atomic<int> runs{0};
	std::atomic<bool> fail{false}, hold{false};
	const std::string& name() const noexcept {
		static const std::string n = "restart";
		return n;
	}
	void operator()() {
		while (hold) { std::this_thread::sleep_for(1ms); }
		++runs;
		if (fail) { throw std::runtime_error("run failed"); }
	}
};

int main() {
	{
		Worker w;
		for (int i = 1; i <= 50; ++i) {
			w.run();
			assert(w.join(5s));
			assert(w.runs == i);
		}
	}
	auto finished = [](const Worker& w) {
		while (w.running()) { std::this_thread::sleep_for(1ms); }
	};
	{
		Worker w;  // run, end, run: each later run() joins the previous run
		for (int i = 1; i <= 50; ++i) {
			w.run();
			finished(w);
		}
		assert(w.join(5s));
		assert(w.runs == 50);
	}
	{
		Worker w;  // a no-op while running
		w.hold = true;
		w.run();
		w.run();
		w.hold = false;
		assert(w.join(5s));
		assert(w.runs == 1);
	}
	{
		Worker w;  // the unjoined run's exception surfaces from the next run()
		w.fail = true;
		w.run();
		finished(w);
		bool threw = false;
		try {
			w.run();
		} catch (const std::runtime_error&) {
			threw = true;
		}
		assert(threw);
		w.fail = false;
		w.run();
		assert(w.join(5s));
		assert(w.runs == 2);
	}
	std::puts("thread restart: ok");
	return 0;
}
