// SmallTask: the queued form of a std::function<void()>. Checks the empty states, that a closure
// up to inline_size bytes is stored with no allocation and a larger one with exactly one, that
// every closure is destroyed exactly once however it moves, and that a closure that only moves,
// or whose move may throw, is held correctly.
//
// Uses CHECK, not assert: a Release build must verify too.
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "small_task.h"

#define CHECK(cond) \
	do { \
		if (!(cond)) { \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
			std::abort(); \
		} \
	} while (0)

// Counts operator new calls on this thread while armed, to see whether storing a task allocates.
static thread_local bool counting = false;
static thread_local int allocations = 0;

void* operator new(std::size_t n) {
	if (counting) {
		++allocations;
	}
	if (void* p = std::malloc(n ? n : 1)) {
		return p;
	}
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using threadpool_detail::SmallTask;

// Makes the compiler treat the object as used, so it cannot elide the allocation behind it.
static void escape(void* p) { asm volatile("" : : "r"(p) : "memory"); }

// Counts constructions and destructions, to prove a closure is destroyed exactly once.
struct Tracker {
	static inline std::atomic<int> alive{0};
	static inline std::atomic<int> moves{0};
	int* hits;
	explicit Tracker(int* h) : hits(h) { ++alive; }
	Tracker(const Tracker& o) : hits(o.hits) { ++alive; }
	Tracker(Tracker&& o) noexcept : hits(o.hits) { ++alive; ++moves; }
	~Tracker() { --alive; }
	void operator()() const { ++*hits; }
};

// A closure whose move constructor may throw: it must be boxed so relocation stays noexcept.
struct ThrowingMove {
	int* hits;
	explicit ThrowingMove(int* h) : hits(h) {}
	ThrowingMove(const ThrowingMove&) = default;
	ThrowingMove(ThrowingMove&& o) noexcept(false) : hits(o.hits) {}
	void operator()() const { ++*hits; }
};

// A closure that only moves.
struct MoveOnly {
	std::unique_ptr<int> p;
	int* hits;
	MoveOnly(int* h) : p(new int(7)), hits(h) {}
	MoveOnly(MoveOnly&&) noexcept = default;
	void operator()() const { *hits += *p; }
};

// Over-aligned: stored boxed.
struct alignas(32) Aligned {
	int* hits;
	void operator()() const { ++*hits; }
};

int plain_calls = 0;
void plain() { ++plain_calls; }
int returns_int() { return ++plain_calls; }

int main() {
	// ---- empty states ----
	{
		SmallTask a;
		CHECK(!a);
		SmallTask b(nullptr);
		CHECK(!b);
		SmallTask c{std::function<void()>()};
		CHECK(!c);
		void (*null_fn)() = nullptr;
		SmallTask d(null_fn);
		CHECK(!d);
		bool threw = false;
		try { a(); } catch (const std::bad_function_call&) { threw = true; }
		CHECK(threw);
		a = SmallTask([] {});
		CHECK(a);
		a = nullptr;
		CHECK(!a);
	}

	// ---- callables: lambda, function pointer, std::function, a result that is discarded ----
	{
		int hits = 0;
		SmallTask l([&hits] { ++hits; });
		l();
		CHECK(hits == 1);
		SmallTask f(&plain);
		f();
		CHECK(plain_calls == 1);
		SmallTask r(&returns_int);
		r();
		CHECK(plain_calls == 2);
		SmallTask s{std::function<void()>([&hits] { hits += 10; })};
		s();
		CHECK(hits == 11);
		std::function<void()> lvalue = [&hits] { hits += 100; };
		SmallTask t(lvalue);  // an lvalue std::function is copied; the original still works
		t();
		lvalue();
		CHECK(hits == 211);
	}

	// ---- allocation: inline_size bytes inline, more boxed with one allocation ----
	{
		int hits = 0;
		auto* h = &hits;
		unsigned long a[5] = {1, 2, 3, 4, 5};  // 40 bytes + the pointer = 48
		counting = true;
		allocations = 0;
		SmallTask inl([h, a] { *h += static_cast<int>(a[4]); });
		CHECK(sizeof(inl) <= 64);
		CHECK(allocations == 0);
		SmallTask moved(std::move(inl));
		SmallTask assigned;
		assigned = std::move(moved);
		CHECK(allocations == 0);
		counting = false;
		assigned();
		CHECK(hits == 5);

		unsigned long big[8] = {1, 2, 3, 4, 5, 6, 7, 8};
		counting = true;
		allocations = 0;
		SmallTask boxed([h, big] { *h += static_cast<int>(big[7]); });
		escape(&boxed);
		CHECK(allocations == 1);
		SmallTask moved_boxed(std::move(boxed));  // moving a boxed task moves the pointer
		CHECK(allocations == 1);
		counting = false;
		moved_boxed();
		CHECK(hits == 13);
	}

	// ---- destruction: exactly once, through every path ----
	{
		int hits = 0;
		CHECK(Tracker::alive == 0);
		{
			SmallTask a{Tracker(&hits)};
			CHECK(Tracker::alive == 1);
			SmallTask b(std::move(a));  // relocate: one live closure
			CHECK(Tracker::alive == 1);
			CHECK(!a);
			SmallTask c;
			c = std::move(b);
			CHECK(Tracker::alive == 1);
			c();
			CHECK(hits == 1);
			SmallTask d{Tracker(&hits)};
			CHECK(Tracker::alive == 2);
			d = std::move(c);  // destroys d's closure, takes c's
			CHECK(Tracker::alive == 1);
			d = std::move(d);  // self-move leaves it alone
			CHECK(d);
			d();
			CHECK(hits == 2);
		}
		CHECK(Tracker::alive == 0);

		// A boxed closure follows the same rules.
		struct BigTracker : Tracker {
			char pad[100];
			explicit BigTracker(int* h) : Tracker(h) {}
		};
		{
			SmallTask a{BigTracker(&hits)};
			CHECK(Tracker::alive == 1);
			SmallTask b(std::move(a));
			CHECK(Tracker::alive == 1);
			b();
			CHECK(hits == 3);
			b = nullptr;
			CHECK(Tracker::alive == 0);
		}
		CHECK(Tracker::alive == 0);
	}

	// ---- closures that move only, throw on move, or are over-aligned ----
	{
		int hits = 0;
		SmallTask mo{MoveOnly(&hits)};
		SmallTask mo2(std::move(mo));
		mo2();
		CHECK(hits == 7);

		counting = true;
		allocations = 0;
		SmallTask tm{ThrowingMove(&hits)};
		escape(&tm);
		CHECK(allocations == 1);  // boxed
		counting = false;
		SmallTask tm2(std::move(tm));
		tm2();
		CHECK(hits == 8);

		SmallTask al{Aligned{&hits}};
		SmallTask al2(std::move(al));
		al2();
		CHECK(hits == 9);
	}

	// ---- a throwing task leaves it intact ----
	{
		SmallTask t([] { throw std::runtime_error("boom"); });
		bool threw = false;
		try { t(); } catch (const std::runtime_error& e) { threw = std::string(e.what()) == "boom"; }
		CHECK(threw);
		CHECK(t);
	}

	// ---- a vector of tasks moves through reallocation ----
	{
		int hits = 0;
		std::vector<SmallTask> v;
		for (int i = 0; i < 1000; ++i) {
			v.emplace_back(Tracker(&hits));
		}
		CHECK(Tracker::alive == 1000);
		for (auto& t : v) {
			t();
		}
		CHECK(hits == 1000);
		v.clear();
		CHECK(Tracker::alive == 0);
	}

	std::printf("all small-task tests passed\n");
	return 0;
}
