#!/usr/bin/env bash
# Builds the research harness (research/ on the research/scheduler branch) with WorkPool
# added as pool "workpool": research_ab.sh RESEARCH_DIR OUT_DIR. The copies are patched
# so Task embeds a WorkItem; the research tree itself is untouched. Run the binary as
# OUT_DIR/research_workloads WORKLOAD pools=baseline,groups,workpool trials=5 ...
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
research="${1:?usage: research_ab.sh RESEARCH_DIR OUT_DIR}/research"
out="${2:?usage: research_ab.sh RESEARCH_DIR OUT_DIR}"
mkdir -p "$out/src" "$out/cfg"
cp "$research"/{common.hh,futex.hh,idle.hh,queues.hh,pools.hh,workloads.cc} "$out/src/"
python3 - "$out/src/pools.hh" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace("struct Task {\n", "struct Task {\n\tWorkItem wi;  // first: WorkPoolAdapter's trampoline casts back\n", 1)
s = s.replace("inline std::unique_ptr<Pool> make_pool(", "}  // namespace research\n#include \"work_pool_research.hh\"\nnamespace research {\n\ninline std::unique_ptr<Pool> make_pool(", 1)
s = s.replace("\tif (name == \"baseline\")", "\tif (name == \"workpool\") { return make_work_pool(n); }\n\tif (name == \"baseline\")", 1)
s = s.replace("#include \"threadpool.hh\"\n", "#include \"threadpool.hh\"\n#include \"work_pool.hh\"\n", 1)
open(p, "w").write(s)
PY
cmake -S "$here" -B "$out/cfg" -DCMAKE_BUILD_TYPE=Release >/dev/null
"${CXX:-c++}" -std=c++20 -O2 -g -DNDEBUG -I"$out/src" -I"$here" -I"$here/bench" -I"$out/cfg" \
	"$out/src/workloads.cc" "$here/thread.cc" -o "$out/research_workloads" -lpthread
echo "built $out/research_workloads"
