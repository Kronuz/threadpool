# Scheduler research prototypes and benchmarks (docs/research/scheduler.md). Built only with
# -DTHREADPOOL_BUILD_RESEARCH=ON; never part of the library or its tests.
include(FetchContent)

option(THREADPOOL_RESEARCH_TASKFLOW "Fetch Taskflow as a reference executor" ON)
option(THREADPOOL_RESEARCH_MOODYCAMEL "Fetch moodycamel's BlockingConcurrentQueue as a reference queue" ON)

add_executable(research_bound bound.cc)
target_link_libraries(research_bound PRIVATE threadpool)
target_compile_features(research_bound PRIVATE cxx_std_20)

add_executable(research_workloads workloads.cc)
target_link_libraries(research_workloads PRIVATE threadpool)
target_compile_features(research_workloads PRIVATE cxx_std_20)

if(THREADPOOL_RESEARCH_TASKFLOW)
  FetchContent_Declare(taskflow
    GIT_REPOSITORY https://github.com/taskflow/taskflow.git
    GIT_TAG        v3.10.0
    GIT_SHALLOW    TRUE)
  set(TF_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(TF_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  FetchContent_Populate(taskflow)
  target_include_directories(research_workloads SYSTEM PRIVATE "${taskflow_SOURCE_DIR}")
  target_compile_definitions(research_workloads PRIVATE RESEARCH_HAVE_TASKFLOW=1)
endif()

if(THREADPOOL_RESEARCH_MOODYCAMEL)
  FetchContent_Declare(concurrentqueue
    GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
    GIT_TAG        v1.0.4
    GIT_SHALLOW    TRUE)
  FetchContent_Populate(concurrentqueue)
  target_include_directories(research_workloads SYSTEM PRIVATE "${concurrentqueue_SOURCE_DIR}")
  target_compile_definitions(research_workloads PRIVATE RESEARCH_HAVE_MOODYCAMEL=1)
endif()
