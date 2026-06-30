/*
 * Default (no-op) tracing and thread-registration hooks for the standalone
 * `threadpool` library.
 *
 * `threadpool.hh` and `thread.cc` instrument themselves through two extension
 * points that are no-ops by default, so the library builds with zero dependency
 * on any logging or crash-reporting header:
 *
 *   - L_EXC(...)                          — logs an exception swallowed in a
 *                                           destructor or a worker's catch-all.
 *   - THREADPOOL_THREAD_REGISTER(p, n)    — registers a freshly named thread
 *                                           (pthread handle `p`, C-string name
 *                                           `n`) so an external crash handler can
 *                                           print per-thread callstacks. No-op by
 *                                           default.
 *   - THREADPOOL_THREAD_UNREGISTER()      — releases the *calling* thread's
 *                                           registration as it is about to exit,
 *                                           the symmetric teardown for REGISTER.
 *                                           Lets a fixed-size registry reclaim the
 *                                           slot instead of leaking it on thread
 *                                           churn. No-op by default.
 *
 * To restore real tracing and thread registration (the way Xapiand uses them),
 * provide your own versions. Two ways:
 *
 *   1. Define THREADPOOL_TRACE_HEADER to the path of a header that defines them,
 *      e.g.
 *        c++ -DTHREADPOOL_TRACE_HEADER='"my_trace.h"' ...
 *      `threadpool.hh`/`thread.cc` will include that instead of this file.
 *
 *   2. Define the macros directly before including the library headers.
 *
 * Each macro is `#ifndef`-guarded, so defining any subset is fine; the rest fall
 * back to the no-op defaults here. To recover Xapiand's behavior, point L_EXC at
 * Xapiand's `log.h` macro and map THREADPOOL_THREAD_REGISTER to
 * `init_thread_info` from `traceback.h`:
 *
 *     #include "log.h"
 *     #include "traceback.h"
 *     #define THREADPOOL_THREAD_REGISTER(pthread, name) init_thread_info(pthread, name)
 *     #define THREADPOOL_THREAD_UNREGISTER() traceback::deregister_thread()
 */

#pragma once

// Logging hook. Used to report an exception swallowed in the ThreadPool
// destructor and in a worker's task catch-all. No-op by default.
#ifndef L_EXC
#define L_EXC(...)
#endif

// Thread-registration hook. Called right after a thread is named, with the
// pthread handle and the registered name (a C string that stays valid). A
// consumer maps this to its crash-callstack registry (Xapiand: init_thread_info
// from traceback.h). No-op by default.
#ifndef THREADPOOL_THREAD_REGISTER
#define THREADPOOL_THREAD_REGISTER(pthread, name) ((void)0)
#endif

// Thread-deregistration hook. Called as the calling thread is about to exit, the
// symmetric teardown for THREADPOOL_THREAD_REGISTER. A consumer maps this to its
// crash-callstack registry's per-thread release (Xapiand: deregister_thread from
// traceback.h) so a fixed-size registry reclaims the slot. No-op by default.
#ifndef THREADPOOL_THREAD_UNREGISTER
#define THREADPOOL_THREAD_UNREGISTER() ((void)0)
#endif
