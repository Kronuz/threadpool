// A 32-bit futex on each platform: Linux futex(2), macOS os_sync_wait_on_address (14.4+) or
// __ulock before it, FreeBSD _umtx_op, else C++20 atomic wait. Research code.
#pragma once

#include <atomic>
#include <cstdint>
#include <ctime>

#if defined(__linux__)
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <climits>
#define RESEARCH_FUTEX "futex(2)"
#elif defined(__APPLE__)
#include <Availability.h>
#if __has_include(<os/os_sync_wait_on_address.h>)
#include <os/os_sync_wait_on_address.h>
#define RESEARCH_FUTEX "os_sync_wait_on_address"
#define RESEARCH_HAVE_OS_SYNC 1
#else
extern "C" int __ulock_wait(std::uint32_t operation, void* addr, std::uint64_t value, std::uint32_t timeout_us);
extern "C" int __ulock_wake(std::uint32_t operation, void* addr, std::uint64_t wake_value);
#define RESEARCH_FUTEX "__ulock_wait"
#endif
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/umtx.h>
#define RESEARCH_FUTEX "_umtx_op"
#else
#define RESEARCH_FUTEX "std::atomic::wait"
#endif

namespace research {

// Blocks while *addr == expected; spurious returns are allowed.
inline void futex_wait(std::atomic<std::uint32_t>* addr, std::uint32_t expected) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(RESEARCH_HAVE_OS_SYNC)
	os_sync_wait_on_address(addr, expected, 4, OS_SYNC_WAIT_ON_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wait(1 /* UL_COMPARE_AND_WAIT */ | 0x01000000 /* ULF_NO_ERRNO */, addr, expected, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, nullptr, nullptr);
#else
	addr->wait(expected);
#endif
}

// Blocks while *addr == expected, for at most `ns` (a timed sleep that may time out).
inline void futex_wait_for(std::atomic<std::uint32_t>* addr, std::uint32_t expected, std::uint64_t ns) noexcept {
#if defined(__linux__)
	timespec ts{static_cast<time_t>(ns / 1'000'000'000ULL), static_cast<long>(ns % 1'000'000'000ULL)};
	syscall(SYS_futex, addr, FUTEX_WAIT_PRIVATE, expected, &ts, nullptr, 0);
#elif defined(__APPLE__) && defined(RESEARCH_HAVE_OS_SYNC)
	os_sync_wait_on_address_with_timeout(addr, expected, 4, OS_SYNC_WAIT_ON_ADDRESS_NONE, OS_CLOCK_MACH_ABSOLUTE_TIME, ns);
#elif defined(__APPLE__)
	__ulock_wait(1 | 0x01000000, addr, expected, static_cast<std::uint32_t>(ns / 1000));
#elif defined(__FreeBSD__)
	struct _umtx_time tm{};
	tm._timeout.tv_sec = static_cast<time_t>(ns / 1'000'000'000ULL);
	tm._timeout.tv_nsec = static_cast<long>(ns % 1'000'000'000ULL);
	tm._flags = 0;  // relative
	tm._clockid = CLOCK_MONOTONIC;
	_umtx_op(addr, UMTX_OP_WAIT_UINT_PRIVATE, expected, reinterpret_cast<void*>(sizeof tm), &tm);
#else
	(void)addr; (void)expected;
	timespec ts{static_cast<time_t>(ns / 1'000'000'000ULL), static_cast<long>(ns % 1'000'000'000ULL)};
	nanosleep(&ts, nullptr);
#endif
}

inline void futex_wake_one(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(RESEARCH_HAVE_OS_SYNC)
	os_sync_wake_by_address_any(addr, 4, OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, 1, nullptr, nullptr);
#else
	addr->notify_one();
#endif
}

inline void futex_wake_all(std::atomic<std::uint32_t>* addr) noexcept {
#if defined(__linux__)
	syscall(SYS_futex, addr, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
#elif defined(__APPLE__) && defined(RESEARCH_HAVE_OS_SYNC)
	os_sync_wake_by_address_all(addr, 4, OS_SYNC_WAKE_BY_ADDRESS_NONE);
#elif defined(__APPLE__)
	__ulock_wake(1 | 0x01000000 | 0x00000100 /* ULF_WAKE_ALL */, addr, 0);
#elif defined(__FreeBSD__)
	_umtx_op(addr, UMTX_OP_WAKE_PRIVATE, INT_MAX, nullptr, nullptr);
#else
	addr->notify_all();
#endif
}

// A per-thread parking spot: park() sleeps until an unpark() that came after the last park()
// returned. A sequence word, so an unpark racing a park is never lost.
struct alignas(64) Parker {
	std::atomic<std::uint32_t> seq{0};

	std::uint32_t epoch() const noexcept { return seq.load(std::memory_order_acquire); }

	// Sleeps while seq == observed (the value read before deciding to park).
	void park(std::uint32_t observed) noexcept {
		while (seq.load(std::memory_order_acquire) == observed) {
			futex_wait(&seq, observed);
		}
	}

	void unpark() noexcept {
		seq.fetch_add(1, std::memory_order_release);
		futex_wake_one(&seq);
	}
};

}  // namespace research
