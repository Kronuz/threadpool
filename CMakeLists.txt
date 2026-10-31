cmake_minimum_required(VERSION 3.14)
project(threadpool LANGUAGES CXX)

include(CheckCXXSourceCompiles)
include(CheckIncludeFileCXX)

find_package(Threads REQUIRED)

# --- Platform feature detection for thread.cc (pthread thread-naming API) ---
# likely.h detects __builtin_expect on its own, so config.h only carries the
# pthread knobs.
set(CMAKE_REQUIRED_LIBRARIES Threads::Threads)

check_cxx_source_compiles("
  #include <pthread.h>
  int main() { pthread_t t = pthread_self(); (void)t; return 0; }
" HAVE_PTHREADS)

# Linux/glibc: pthread_setname_np(pthread_t, const char*)
check_cxx_source_compiles("
  #include <pthread.h>
  int main() { pthread_setname_np(pthread_self(), \"x\"); return 0; }
" HAVE_PTHREAD_SETNAME_NP_2ARG)

# macOS: pthread_setname_np(const char*)
check_cxx_source_compiles("
  #include <pthread.h>
  int main() { pthread_setname_np(\"x\"); return 0; }
" HAVE_PTHREAD_SETNAME_NP_1ARG)

if(HAVE_PTHREAD_SETNAME_NP_2ARG OR HAVE_PTHREAD_SETNAME_NP_1ARG)
  set(HAVE_PTHREAD_SETNAME_NP 1)
endif()

# Some BSDs: pthread_set_name_np(pthread_t, const char*) in <pthread_np.h>
check_include_file_cxx("pthread_np.h" HAVE_PTHREAD_NP_H)
check_cxx_source_compiles("
  #include <pthread.h>
  #if __has_include(<pthread_np.h>)
  #include <pthread_np.h>
  #endif
  int main() { pthread_set_name_np(pthread_self(), \"x\"); return 0; }
" HAVE_PTHREAD_SET_NAME_NP)

unset(CMAKE_REQUIRED_LIBRARIES)

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/config.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/config.h"
  @ONLY)

# --- Library target ---
# A small static library carrying thread.cc, plus the public headers. Consumers
# get both the source dir (for the headers) and the binary dir (for the
# generated config.h) on their include path.
add_library(threadpool STATIC thread.cc)
target_include_directories(threadpool PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_CURRENT_BINARY_DIR}")
target_compile_features(threadpool PUBLIC cxx_std_20)
target_link_libraries(threadpool PUBLIC Threads::Threads)
# Alias so consumers can link `threadpool::threadpool` whether they add this via
# add_subdirectory or FetchContent.
add_library(threadpool::threadpool ALIAS threadpool)

# Only build the test when this repo is the top-level project, so consumers
# (e.g. via FetchContent) don't build our tests.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  enable_testing()
  add_executable(threadpool_test test/test.cc)
  target_link_libraries(threadpool_test PRIVATE threadpool)
  add_test(NAME threadpool COMMAND threadpool_test)
  add_executable(thread_restart_test test/thread_restart.cc)
  target_link_libraries(thread_restart_test PRIVATE threadpool)
  add_test(NAME thread_restart COMMAND thread_restart_test)

  add_executable(task_group_test test/task_group.cc)
  target_link_libraries(task_group_test PRIVATE threadpool)
  add_test(NAME task_group COMMAND task_group_test)

  add_executable(work_pool_test test/work_pool.cc)
  target_link_libraries(work_pool_test PRIVATE threadpool)
  add_test(NAME work_pool COMMAND work_pool_test)

  add_executable(work_group_test test/work_group.cc)
  target_link_libraries(work_group_test PRIVATE threadpool)
  add_test(NAME work_group COMMAND work_group_test)

  add_executable(work_scheduling_test test/work_scheduling.cc)
  target_link_libraries(work_scheduling_test PRIVATE threadpool)
  add_test(NAME work_scheduling COMMAND work_scheduling_test)

  add_executable(work_wait_test test/work_wait.cc)
  target_link_libraries(work_wait_test PRIVATE threadpool)
  add_test(NAME work_wait COMMAND work_wait_test)

  # The pool's queue and tasks: SmallTask, the old pool as an oracle, stress for TSan/ASan.
  add_executable(small_task_test test/small_task.cc)
  target_link_libraries(small_task_test PRIVATE threadpool)
  add_test(NAME small_task COMMAND small_task_test)

  add_executable(pool_diff_test test/pool_diff.cc)
  target_link_libraries(pool_diff_test PRIVATE threadpool)
  target_include_directories(pool_diff_test PRIVATE test)
  add_test(NAME pool_diff COMMAND pool_diff_test)

  add_executable(pool_stress_test test/pool_stress.cc)
  target_link_libraries(pool_stress_test PRIVATE threadpool)
  add_test(NAME pool_stress COMMAND pool_stress_test)

  # Benchmarks (not tests): -DTHREADPOOL_BUILD_BENCH=ON builds them; see bench/.
  option(THREADPOOL_BUILD_BENCH "Build the benchmarks" OFF)
  if(THREADPOOL_BUILD_BENCH)
    add_executable(kernel_time bench/kernel_time.cc)
    target_link_libraries(kernel_time PRIVATE threadpool)

    # enqueue_bench compares ThreadPool with the frozen pre-change pool, a mutex + condition
    # variable pool and, when found, standalone asio (-DASIO_INCLUDE_DIR=... or a system asio;
    # otherwise it is fetched here, never by the library).
    add_executable(enqueue_bench bench/enqueue_bench.cc)
    target_link_libraries(enqueue_bench PRIVATE threadpool)
    target_include_directories(enqueue_bench PRIVATE test)
    find_path(THREADPOOL_ASIO_INCLUDE asio.hpp HINTS ${ASIO_INCLUDE_DIR} /opt/homebrew/include /usr/local/include)
    if(NOT THREADPOOL_ASIO_INCLUDE)
      include(FetchContent)
      FetchContent_Declare(asio GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git GIT_TAG asio-1-30-2)
      FetchContent_GetProperties(asio)
      if(NOT asio_POPULATED)
        FetchContent_Populate(asio)
      endif()
      set(THREADPOOL_ASIO_INCLUDE "${asio_SOURCE_DIR}/asio/include")
    endif()
    target_include_directories(enqueue_bench SYSTEM PRIVATE "${THREADPOOL_ASIO_INCLUDE}")
    target_compile_definitions(enqueue_bench PRIVATE ASIO_STANDALONE)
  endif()

  # A runnable tour of the library (not a test): build & run ./build/threadpool_demo
  add_executable(threadpool_demo examples/demo.cc)
  target_link_libraries(threadpool_demo PRIVATE threadpool)

  # Scheduler research prototypes (docs/research/scheduler.md); off by default.
  option(THREADPOOL_BUILD_RESEARCH "Build the scheduler research prototypes and benchmarks" OFF)
  if(THREADPOOL_BUILD_RESEARCH)
    add_subdirectory(research)
  endif()
endif()
