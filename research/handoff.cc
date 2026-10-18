// The cost of handing one unit of work to a sleeping worker thread, by wake primitive: a
// producer signals, the worker wakes, works, and sleeps again; the producer spins for the
// acknowledgment (a busy indexing thread, which never blocks). Per handoff: wall latency from
// signal to the worker running, and the process's user and kernel time and context
// switches (getrusage). For the FreeBSD finding: a std::thread with a condvar handoff cost
// about three times the kernel time of Linux and macOS (strata perf/index-size, E3c).
//   research_handoff [handoffs=20000] [work_us=0]
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "common.hh"
#include "futex.hh"

using namespace research;

namespace {

enum class Mode { spin, condvar_locked, condvar_unlocked, condvar_timed, atomic_wait, futex };

const char* name(Mode m) {
	switch (m) {
		case Mode::spin: return "spin (no sleep; the floor)";
		case Mode::condvar_locked: return "condvar, notify_one under the lock";
		case Mode::condvar_unlocked: return "condvar, notify_one after unlock";
		case Mode::condvar_timed: return "condvar, wait_for 100 ms (today's pool)";
		case Mode::atomic_wait: return "std::atomic::wait / notify_one";
		case Mode::futex: return RESEARCH_FUTEX;
	}
	return "?";
}

struct Usage {
	std::uint64_t user_ns, sys_ns, vcsw, ivcsw;
};

Usage usage() {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	auto tv = [](const timeval& t) { return std::uint64_t(t.tv_sec) * 1'000'000'000ULL + std::uint64_t(t.tv_usec) * 1'000ULL; };
	return {tv(ru.ru_utime), tv(ru.ru_stime), std::uint64_t(ru.ru_nvcsw), std::uint64_t(ru.ru_nivcsw)};
}

void run(Mode mode, unsigned handoffs, unsigned work_us) {
	alignas(64) std::atomic<std::uint32_t> seq{0};
	alignas(64) std::atomic<std::uint32_t> ack{0};
	alignas(64) std::atomic<std::uint64_t> stamp{0};
	std::mutex mu;
	std::condition_variable cv;
	std::vector<std::uint64_t> lat;
	lat.reserve(handoffs);

	std::thread worker([&] {
		for (std::uint32_t i = 1; i <= handoffs; ++i) {
			switch (mode) {
				case Mode::spin:
					while (seq.load(std::memory_order_acquire) < i) { cpu_relax(); }
					break;
				case Mode::condvar_locked:
				case Mode::condvar_unlocked: {
					std::unique_lock<std::mutex> lk(mu);
					cv.wait(lk, [&] { return seq.load(std::memory_order_acquire) >= i; });
					break;
				}
				case Mode::condvar_timed: {
					std::unique_lock<std::mutex> lk(mu);
					while (!cv.wait_for(lk, std::chrono::milliseconds(100), [&] { return seq.load(std::memory_order_acquire) >= i; })) {}
					break;
				}
				case Mode::atomic_wait:
					while (seq.load(std::memory_order_acquire) < i) { seq.wait(i - 1, std::memory_order_acquire); }
					break;
				case Mode::futex:
					while (seq.load(std::memory_order_acquire) < i) { futex_wait(&seq, i - 1); }
					break;
			}
			lat.push_back(now_ns() - stamp.load(std::memory_order_acquire));
			if (work_us) { burn_ns_clock(std::uint64_t(work_us) * 1000); }
			ack.store(i, std::memory_order_release);
		}
	});

	// Let the worker reach its wait before the first handoff, so every handoff is a wake.
	burn_ns_clock(2'000'000);
	auto u0 = usage();
	auto t0 = now_ns();
	for (std::uint32_t i = 1; i <= handoffs; ++i) {
		stamp.store(now_ns(), std::memory_order_release);
		switch (mode) {
			case Mode::spin:
				seq.store(i, std::memory_order_release);
				break;
			case Mode::condvar_locked:
			case Mode::condvar_timed: {
				std::lock_guard<std::mutex> lk(mu);
				seq.store(i, std::memory_order_release);
				cv.notify_one();
				break;
			}
			case Mode::condvar_unlocked: {
				{ std::lock_guard<std::mutex> lk(mu); seq.store(i, std::memory_order_release); }
				cv.notify_one();
				break;
			}
			case Mode::atomic_wait:
				seq.store(i, std::memory_order_release);
				seq.notify_one();
				break;
			case Mode::futex:
				seq.store(i, std::memory_order_release);
				futex_wake_one(&seq);
				break;
		}
		while (ack.load(std::memory_order_acquire) < i) { cpu_relax(); }
		// A pause with the worker asleep, so the next signal is a wake and not a race with
		// its return to the wait; 20 µs is well under any wake latency measured here.
		burn_ns_clock(20'000);
	}
	auto t1 = now_ns();
	auto u1 = usage();
	worker.join();
	auto p = percentiles(lat);
	const double n = handoffs;
	std::printf("handoff mode=%-42s n=%u work_us=%u wall_per=%.1f us lat_p50=%s lat_p99=%s lat_p999=%s user_per=%.2f us sys_per=%.2f us vcsw_per=%.2f ivcsw_per=%.2f\n",
		name(mode), handoffs, work_us, double(t1 - t0) / n / 1e3, fmt_ns(p.p50).c_str(), fmt_ns(p.p99).c_str(), fmt_ns(p.p999).c_str(),
		double(u1.user_ns - u0.user_ns) / n / 1e3, double(u1.sys_ns - u0.sys_ns) / n / 1e3,
		double(u1.vcsw - u0.vcsw) / n, double(u1.ivcsw - u0.ivcsw) / n);
	std::fflush(stdout);
}

// Timed sleeps that time out, one thread: what a sleep_until or a polling wait pays per call.
template <typename F>
void timeout(const char* what, unsigned n, F&& f) {
	auto u0 = usage();
	auto t0 = now_ns();
	for (unsigned i = 0; i < n; ++i) { f(); }
	auto t1 = now_ns();
	auto u1 = usage();
	std::printf("timeout mode=%-42s n=%u wall_per=%.1f us user_per=%.2f us sys_per=%.2f us vcsw_per=%.2f ivcsw_per=%.2f\n",
		what, n, double(t1 - t0) / n / 1e3, double(u1.user_ns - u0.user_ns) / n / 1e3, double(u1.sys_ns - u0.sys_ns) / n / 1e3,
		double(u1.vcsw - u0.vcsw) / n, double(u1.ivcsw - u0.ivcsw) / n);
	std::fflush(stdout);
}

void timeouts(unsigned n) {
	std::mutex mu;
	std::condition_variable cv;
	alignas(64) std::atomic<std::uint32_t> word{0};
	timeout("condvar wait_for 1 ms, timing out", n, [&] {
		std::unique_lock<std::mutex> lk(mu);
		cv.wait_for(lk, std::chrono::milliseconds(1));
	});
	timeout("nanosleep 1 ms", n, [] {
		timespec ts{0, 1'000'000};
		nanosleep(&ts, nullptr);
	});
	timeout("futex wait with a 1 ms timeout, timing out", n, [&] { futex_wait_for(&word, 0, 1'000'000); });
}

}  // namespace

int main(int argc, char** argv) {
	const unsigned handoffs = argc > 1 ? unsigned(std::strtoul(argv[1], nullptr, 10)) : 20000;
	const unsigned work_us = argc > 2 ? unsigned(std::strtoul(argv[2], nullptr, 10)) : 0;
	churn_per_ns();
	std::printf("handoff: %u hardware threads, futex = %s\n", hardware_threads(), RESEARCH_FUTEX);
	for (Mode m : {Mode::spin, Mode::condvar_locked, Mode::condvar_unlocked, Mode::condvar_timed, Mode::atomic_wait, Mode::futex}) {
		run(m, handoffs, work_us);
	}
	timeouts(2000);
	return 0;
}
