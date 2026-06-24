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
endif()
