/*
 * Copyright (c) 2015-2019 Dubalu LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "thread.hh"

#include "config.h"              // for HAVE_PTHREADS, HAVE_PTHREAD_SETNAME_NP

// Tracing and thread registration are optional and fully injectable. Define
// THREADPOOL_TRACE_HEADER (a header path) before compiling to plug in your own
// L_EXC logging macro and THREADPOOL_THREAD_REGISTER hook; otherwise the bundled
// no-op stubs in threadpool_trace.h are used. See threadpool_trace.h.
#ifdef THREADPOOL_TRACE_HEADER
#  include THREADPOOL_TRACE_HEADER
#else
#  include "threadpool_trace.h"
#endif

#include <errno.h>               // for errno
#include <mutex>                 // for std::mutex, std::lock_guard
#include <string>                // for std::string
#include <thread>                // for std::thread
#include <tuple>                 // for std::forward_as_tuple
#include <unordered_map>         // for std::unordered_map
#ifdef HAVE_PTHREADS
#include <pthread.h>             // for pthread_self
#endif
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>          // for pthread_setname_np
#endif


// Optional prefix prepended to OS-level thread names (what shows up in a
// debugger / `top`). Empty by default; a consumer can set e.g.
// -DTHREADPOOL_THREAD_NAME_PREFIX='"Xapiand:"' to recover the original naming.
#ifndef THREADPOOL_THREAD_NAME_PREFIX
#define THREADPOOL_THREAD_NAME_PREFIX ""
#endif


static std::mutex thread_names_mutex;
static std::unordered_map<std::thread::id, std::string> thread_names;


#ifdef __APPLE__
#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
int
sched_getcpu()
{
	uint32_t info[4];
	__cpuid_count(1, 0, info[0], info[1], info[2], info[3]);
	if ( (info[3] & (1 << 9)) == 0) {
		return -1;  // no APIC on chip
	}
	// info[1] is EBX, bits 24-31 are APIC ID
	return (unsigned)info[1] >> 24;
}
#else
int
sched_getcpu()
{
	// Apple Silicon (and other non-x86 macOS): there is no reliable way to read
	// the current CPU from user space, so report "unknown". Callers treat a
	// negative result as "don't know".
	return -1;
}
#endif
#endif


void
run_thread(void *(*thread_routine)(void *), void *arg, ThreadPolicyType)
{
	std::thread(thread_routine, arg).detach();
}


void
setup_thread(const std::string& name, ThreadPolicyType)
{
	set_thread_name(name);
}


////////////////////////////////////////////////////////////////////////////////


void
set_thread_name(const std::string& name)
{
	[[maybe_unused]] auto pthread = pthread_self();
#if defined(HAVE_PTHREAD_SETNAME_NP) && defined(__linux__)
	pthread_setname_np(pthread, (std::string(THREADPOOL_THREAD_NAME_PREFIX) + name).c_str());
	// pthread_setname_np(pthread, (std::string(THREADPOOL_THREAD_NAME_PREFIX) + name).c_str(), nullptr);
#elif defined(HAVE_PTHREAD_SETNAME_NP)
	pthread_setname_np((std::string(THREADPOOL_THREAD_NAME_PREFIX) + name).c_str());
#elif defined(HAVE_PTHREAD_SET_NAME_NP)
	pthread_set_name_np(pthread, (std::string(THREADPOOL_THREAD_NAME_PREFIX) + name).c_str());
#endif
	std::lock_guard<std::mutex> lk(thread_names_mutex);
	[[maybe_unused]] auto emplaced = thread_names.emplace(std::piecewise_construct,
		std::forward_as_tuple(std::this_thread::get_id()),
		std::forward_as_tuple(name));
	// Register the named thread so an external crash handler can dump per-thread
	// callstacks. No-op by default; Xapiand maps this to init_thread_info.
	THREADPOOL_THREAD_REGISTER(pthread, emplaced.first->second.c_str());
}


const std::string&
get_thread_name(std::thread::id thread_id)
{
	std::lock_guard<std::mutex> lk(thread_names_mutex);
	auto thread = thread_names.find(thread_id);
	if (thread == thread_names.end()) {
		static std::string _ = "???";
		return _;
	}
	return thread->second;
}


const std::string&
get_thread_name()
{
	return get_thread_name(std::this_thread::get_id());
}
