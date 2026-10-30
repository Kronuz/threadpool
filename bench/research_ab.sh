#!/usr/bin/env bash
# Builds the research harness (research/, this repo's by default) with WorkPool added as
# pools "workpool", "workpool-r0", and "workpool-r2": research_ab.sh OUT_DIR [REPO_DIR].
# The copies are patched so Task embeds a WorkItem; research/ itself is untouched. Run
# OUT_DIR/research_workloads WORKLOAD pools=baseline,groups,prio,workpool trials=5 ...
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:?usage: research_ab.sh OUT_DIR [REPO_DIR]}"
research="${2:-$here}/research"
mkdir -p "$out/src" "$out/cfg"
cp "$research"/{common.hh,futex.hh,idle.hh,queues.hh,pools.hh,workloads.cc} "$out/src/"
python3 - "$out/src/pools.hh" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace("struct Task {\n", "struct Task {\n\tWorkItem wi;  // first: WorkPoolAdapter's trampoline casts back\n", 1)
s = s.replace("inline std::unique_ptr<Pool> make_pool(", "}  // namespace research\n#include \"work_pool_research.hh\"\nnamespace research {\n\ninline std::unique_ptr<Pool> make_pool(", 1)
s = s.replace("\tif (name == \"baseline\")", "\tif (name.rfind(\"workpool\", 0) == 0) { return make_work_pool(name, n); }\n\tif (name == \"baseline\")", 1)
s = s.replace("#include \"threadpool.hh\"\n", "#include \"threadpool.hh\"\n#include \"work_pool.hh\"\n", 1)
open(p, "w").write(s)
PY
cmake -S "$here" -B "$out/cfg" -DCMAKE_BUILD_TYPE=Release >/dev/null
"${CXX:-c++}" -std=c++20 -O2 -g -DNDEBUG -I"$out/src" -I"$here" -I"$here/bench" -I"$out/cfg" \
	"$out/src/workloads.cc" "$here/thread.cc" -o "$out/research_workloads" -lpthread
echo "built $out/research_workloads"
