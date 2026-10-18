// Shared helpers for the scheduler research benchmarks: clocks, calibrated busy work,
// percentiles, process counters. Research code: off by default (THREADPOOL_BUILD_RESEARCH).
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>
#include <sys/time.h>
#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <sched.h>
#include <unistd.h>
#endif

namespace research {

inline std::uint64_t now_ns() noexcept {
	return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::steady_clock::now().time_since_epoch()).count());
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
	__asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
	__asm__ __volatile__("isb" ::: "memory");
#else
	std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

// Dependent integer work that the optimizer cannot remove; returns a value to sink.
inline std::uint64_t churn(std::uint64_t iterations, std::uint64_t seed = 0x9e3779b97f4a7c15ULL) noexcept {
	std::uint64_t x = seed;
	for (std::uint64_t i = 0; i < iterations; ++i) {
		x ^= x >> 12;
		x ^= x << 25;
		x ^= x >> 27;
		x *= 0x2545f4914f6cdd1dULL;
	}
	return x;
}

inline std::atomic<std::uint64_t> sink{0};

// Iterations of churn() per nanosecond, calibrated once against the steady clock.
inline double churn_per_ns() {
	static double rate = [] {
		const std::uint64_t n = 20'000'000;
		auto t0 = now_ns();
		sink.fetch_add(churn(n), std::memory_order_relaxed);
		auto t1 = now_ns();
		return double(n) / double(t1 - t0);
	}();
	return rate;
}

// Burns about `ns` nanoseconds of CPU without touching the clock (for sub-microsecond work).
inline void burn_ns(std::uint64_t ns) noexcept {
	sink.fetch_add(churn(static_cast<std::uint64_t>(double(ns) * churn_per_ns())), std::memory_order_relaxed);
}

// Burns about `ns` nanoseconds, polling the clock (for work of microseconds and up).
inline void burn_ns_clock(std::uint64_t ns) noexcept {
	auto end = now_ns() + ns;
	while (now_ns() < end) {
		sink.fetch_add(churn(64), std::memory_order_relaxed);
	}
}

struct Percentiles {
	std::uint64_t n = 0, min = 0, p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0;
	double mean = 0;
};

inline Percentiles percentiles(std::vector<std::uint64_t>& v) {
	Percentiles p;
	if (v.empty()) { return p; }
	std::sort(v.begin(), v.end());
	auto at = [&](double q) { return v[std::min(v.size() - 1, static_cast<std::size_t>(q * double(v.size())))]; };
	p.n = v.size();
	p.min = v.front();
	p.p50 = at(0.50);
	p.p90 = at(0.90);
	p.p99 = at(0.99);
	p.p999 = at(0.999);
	p.max = v.back();
	long double sum = 0;
	for (auto x : v) { sum += x; }
	p.mean = double(sum / v.size());
	return p;
}

inline std::string fmt_ns(std::uint64_t ns) {
	char buf[64];
	if (ns < 10'000) { std::snprintf(buf, sizeof buf, "%llu ns", static_cast<unsigned long long>(ns)); }
	else if (ns < 10'000'000) { std::snprintf(buf, sizeof buf, "%.1f us", double(ns) / 1e3); }
	else { std::snprintf(buf, sizeof buf, "%.2f ms", double(ns) / 1e6); }
	return buf;
}

inline void print_percentiles(const char* label, std::vector<std::uint64_t>& v) {
	auto p = percentiles(v);
	std::printf("%-44s n=%-8llu min=%-10s p50=%-10s p90=%-10s p99=%-10s p99.9=%-10s max=%-10s mean=%s\n",
		label, static_cast<unsigned long long>(p.n), fmt_ns(p.min).c_str(), fmt_ns(p.p50).c_str(), fmt_ns(p.p90).c_str(),
		fmt_ns(p.p99).c_str(), fmt_ns(p.p999).c_str(), fmt_ns(p.max).c_str(), fmt_ns(static_cast<std::uint64_t>(p.mean)).c_str());
}

// Process CPU time (user + system), nanoseconds.
inline std::uint64_t cpu_time_ns() {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	auto tv = [](const timeval& t) { return std::uint64_t(t.tv_sec) * 1'000'000'000ULL + std::uint64_t(t.tv_usec) * 1'000ULL; };
	return tv(ru.ru_utime) + tv(ru.ru_stime);
}

// Resident set size now, bytes.
inline std::uint64_t rss_bytes() {
#if defined(__APPLE__)
	mach_task_basic_info info{};
	mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS) {
		return info.resident_size;
	}
	return 0;
#elif defined(__linux__)
	long pages = 0, resident = 0;
	if (FILE* f = std::fopen("/proc/self/statm", "r")) {
		if (std::fscanf(f, "%ld %ld", &pages, &resident) != 2) { resident = 0; }
		std::fclose(f);
	}
	return std::uint64_t(resident) * std::uint64_t(sysconf(_SC_PAGESIZE));
#else
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	return std::uint64_t(ru.ru_maxrss) * 1024ULL;
#endif
}

// Voluntary and involuntary context switches, as a wakeup counter.
inline std::uint64_t context_switches() {
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	return std::uint64_t(ru.ru_nvcsw) + std::uint64_t(ru.ru_nivcsw);
}

inline unsigned hardware_threads() {
	unsigned n = std::thread::hardware_concurrency();
	return n ? n : 1;
}

inline unsigned env_unsigned(const char* name, unsigned def) {
	if (const char* v = std::getenv(name)) { return static_cast<unsigned>(std::strtoul(v, nullptr, 10)); }
	return def;
}

inline std::uint64_t splitmix(std::uint64_t& s) noexcept {
	std::uint64_t z = (s += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
	z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

}  // namespace research
