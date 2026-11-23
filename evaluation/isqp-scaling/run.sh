#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs the ISQP scaling experiment (isqp-scaling-benchmark) in the development container.
#
# usage: evaluation/isqp-scaling/run.sh <preset> [extra benchmark arguments]
#   presets:
#     smoke          tiny run of both experiments in planning mode, to check the setup
#     plan           planning only: 1 cloud, 10 fog, 100 leaf nodes, 100 queries
#     plan-original  planning only, as the original benchmark: 10 cloud, 10 fog, 100 leaf nodes, 4000 queries, unlimited capacity,
#                    a sequence of 200 requests with 10 moves each (4000 events)
#     deploy         planning and ISQD redeployment on embedded workers: 1 cloud, 4 fog, 16 leaf nodes, 16 queries
#     occ-limit      planning only, batch experiment, ISQP with OCC: 10 cloud, 10 fog, 100 leaf nodes, 400 queries, 1-24 threads; pass
#                    --fog-capacity <slots> to provoke conflicts (the even share is 40)
#
# Environment:
#   BUILD_DIR  build directory relative to the repository (default: cmake-build-release; built if the binary is missing)
#   RUNS       repetitions of every point (default: 3)
#   PLACEMENTS, AMENDMENT_MODES, THREADS  override the preset's configurations (space-separated)
#   NAME_SUFFIX  appended to the preset in the result file name
#
# Every configuration and repetition runs in its own process (see the shortcut in IsqpScalingBenchmark.cpp).
# Results: evaluation/isqp-scaling/results/<preset>-<timestamp>.csv (plus the console log); plot them with plot.py.
set -euo pipefail

preset=${1:?"usage: $0 <smoke|plan|plan-original|occ-limit|deploy> [extra benchmark arguments]"}
shift
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
build_dir=${BUILD_DIR:-cmake-build-release}
runs=${RUNS:-3}

placements="incremental holistic"
amendment_modes="Optimistic Pessimistic"
case "$preset" in
  smoke)
    args=(--mode plan --fog 3 --leaves 6 --queries 6 --batch-moves 1,2,4 --sequence-requests 10)
    threads="1 2"; runs=1 ;;
  plan)
    args=(--mode plan --cloud 1 --fog 10 --leaves 100 --queries 100
          --batch-moves 1,2,4,8,16,32,64,128,256 --sequence-requests 500)
    threads="1 4 8" ;;
  plan-original)
    args=(--mode plan --experiments sequence --cloud 10 --fog 10 --leaves 100 --queries 4000 --unlimited-capacity
          --sequence-requests 200 --sequence-moves 10)
    threads="1 10" ;;
  occ-limit)
    args=(--mode plan --experiments batch --cloud 10 --fog 10 --leaves 100 --queries 400 --batch-moves 1,2,4,8,16,32,64,128)
    placements="incremental"; amendment_modes="Optimistic"; threads="1 4 8 16 24" ;;
  deploy)
    args=(--mode deploy --cloud 1 --fog 4 --leaves 16 --queries 16 --emit-rate 100
          --batch-moves 1,2,4,8,16 --sequence-requests 50)
    threads="1 4" ;;
  *)
    echo "unknown preset: $preset" >&2
    exit 1 ;;
esac
placements=${PLACEMENTS:-$placements}
amendment_modes=${AMENDMENT_MODES:-$amendment_modes}
threads=${THREADS:-$threads}

results="evaluation/isqp-scaling/results"
mkdir -p "$repo/$results"
name="$preset${NAME_SUFFIX:-}-$(date +%Y%m%d-%H%M%S)"

# The repository must be mounted at /tmp/nes-adaptive-optimization, as the build directories are shared with CLion's toolchain.
# Every configuration and repetition runs in its own process: torn-down embedded workers leave threads behind, and a process aborts
# after 4096 threads.
docker run --rm \
  -v "$repo":/tmp/nes-adaptive-optimization \
  -v nes-ccache:/root/.cache/ccache \
  --workdir /tmp/nes-adaptive-optimization \
  nebulastream/nes-development:local \
  bash -c "
    set -euo pipefail
    [ -f $build_dir/build.ninja ] || cmake -GNinja -B $build_dir -DCMAKE_BUILD_TYPE=Release
    cmake --build $build_dir -j --target isqp-scaling-benchmark | tail -1
    cd $results
    for placement in $placements; do
      for amendment_mode in $amendment_modes; do
        for thread_count in $threads; do
          for run in \$(seq 0 \$(($runs - 1))); do
            # The workers log to stdout as well; keep only the benchmark's own progress lines.
            /tmp/nes-adaptive-optimization/$build_dir/nes-frontend/apps/isqp-scaling-benchmark ${args[*]} \\
              --placement \$placement --amendment-modes \$amendment_mode --threads \$thread_count --runs 1 --first-run \$run \\
              $* --append -o $name.csv 2>&1 | grep --line-buffered -E '^(batch|sequence) |^isqp-scaling-benchmark' | tee -a $name.log
            cat isqp-scaling-benchmark.log >> $name.benchmark.log
          done
        done
      done
    done
    rm -f isqp-scaling-benchmark.log
  "
echo "Results: $repo/$results/$name.csv"
