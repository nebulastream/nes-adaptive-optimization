# ISQP scaling experiment

How does the incremental placement (ISQP) scale with the number of topology change events? Port of the original's
`ISQPBenchmarkTopologyChanges` (nes-isqd, `nes-benchmark/src/MicroBenchmarks/ISQPBenchmark`).

## Setup

- Three layers: cloud ← fog ← leaf nodes. Leaves are spread over the fog nodes in contiguous blocks; every fog node links to every cloud
  node. Leaves host one generator source each and have no operator slots; fog nodes have twice their even share of the queries as slots
  (`--fog-capacity`); cloud nodes are unlimited. With `--unlimited-capacity`, every node is unlimited, as in the original.
- Queries (`--query`): query `q` reads leaf `l = q mod L` and writes to cloud node `q mod C`.
  - `selection` (default): `SELECT * FROM stream<l> WHERE id >= 0 INTO sink<q>`.
  - `join` (not in the original): a 1 s tumbling window join of leaf `l` with the leaf `r` at the same position in the block of the next fog
    node (`r = l + L/F` when `L` is a multiple of `F`), so the join sits above the fog layer and every leaf feeds two joins:
    `SELECT * FROM (SELECT * FROM stream<l>) INNER JOIN (SELECT * FROM stream<r>) ON id<l> = id<r> WINDOW TUMBLING (ts<l>, ts<r>, size 1 sec) INTO sink<q>`.
    The sources then have the fields `id<l>` and `ts<l>` (the timestamp advances with the emit rate). Needs at least two fog nodes.
- Change event: the first leaf of a fog node moves to the next fog node (a `RemoveLink` and an `AddLink`), round-robin over the fog nodes,
  as in the original. A request with `m` moves has `2m` events.
- Experiments:
  - `sequence`: many consecutive requests on one cluster, as in the original; also shows whether the cost per request drifts (change
    log, slot fragmentation, versions of the global execution plan).
  - `batch` (not in the original): one request with 1, 2, 4, … moves, each on a fresh cluster.
- Configurations: incremental (ISQP) vs. holistic placement × OCC vs. 2PL × placement amendment threads.
- Modes:
  - `plan`: the workers only pretend to run the plans; deployment requests succeed. The deployment phase still runs; a drained plan
    counts as stopped as soon as a reconfiguration marker with its drain event was injected.
  - `deploy`: embedded workers in the same process run the plans over the in-memory transport, so the times include the ISQD redeployment
    (starting plans, markers, draining). Limited to a few dozen workers.

## Running

```bash
evaluation/isqp-scaling/run.sh smoke           # seconds; checks the setup
evaluation/isqp-scaling/run.sh plan            # 1 cloud / 10 fog / 100 leaves, 100 queries
evaluation/isqp-scaling/run.sh plan-original   # as the original: 10 cloud / 10 fog / 100 leaves, 4000 queries, unlimited capacity,
                                               # 200 requests of 10 moves
evaluation/isqp-scaling/run.sh deploy          # 1 cloud / 4 fog / 16 leaves, 16 queries, with ISQD
evaluation/isqp-scaling/run.sh occ-limit --fog-capacity 40   # 10 cloud / 10 fog / 100 leaves, 400 queries, OCC with 1-24 threads
```

`occ-limit` looks for the limits of optimistic synchronization: with tight fog capacities (the even share is 40 slots), concurrent
amendments compete for the same slots. Override the configurations with `PLACEMENTS`, `AMENDMENT_MODES` and `THREADS`, and name the
result file with `NAME_SUFFIX`.

The script runs in the development container and builds `isqp-scaling-benchmark` in `cmake-build-release` if it is missing (override with
`BUILD_DIR`, repetitions with `RUNS`). Extra arguments go to the benchmark (`--help` lists them). Results land in `results/`:
`<preset>-<time>.csv` (one row per request), the console log, and the benchmark's log. Every configuration and repetition runs in its
own process: torn-down embedded workers leave network sender threads behind, and a process aborts after 4096 threads (about 100
clusters in `deploy` mode).

Use a Release build for numbers; in a Debug build a single placement takes about 80 ms.

`deploy` mode gives every worker 256 MiB of buffer memory (`--worker-memory-mb`); the process uses roughly 140 MiB per worker, e.g.,
about 17 GB for the 120 workers of the original's topology.

## Plotting

```bash
python3 -m venv .venv && .venv/bin/pip install matplotlib pandas
.venv/bin/python evaluation/isqp-scaling/plot.py evaluation/isqp-scaling/results/<file>.csv
```

Produces `batch-latency` (request time over events), `batch-per-query`, `batch-breakdown` (placement vs. deployment) and
`sequence-latency` (per request, rolling median) as PDF and PNG next to the CSV.

## CSV columns

| Column | Meaning |
|---|---|
| `experiment`, `mode` | `batch`/`sequence`, `plan`/`deploy` |
| `cloud_nodes`, `fog_nodes`, `leaf_nodes`, `queries`, `fog_capacity` | Cluster size and operator slots per fog node (`unlimited`: every node is unlimited) |
| `query` | `selection` or `join` (missing in results from before the option existed) |
| `incremental`, `amendment_mode`, `threads` | Configuration |
| `run`, `request`, `moves`, `events` | Repetition, request number in a sequence, leaf moves and events of the request |
| `affected_queries`, `pending_queries` | Queries affected by the events, and other queries amended because they are not deployed (an earlier amendment processed them only partially or failed before its placement completed) |
| `failed_amendments`, `partial_amendments` | Amendments that failed (deployment or exception), and amendments with failed change log entries (e.g., a failed optimistic validation); as in the original, the latter count as successful and are processed again by the next request |
| `processing_us` | Whole request: applying the events, finding affected queries, amending |
| `amendment_us` | Wall time of the (parallel) amendments |
| `placement_us_sum`, `placement_us_max` | Placement time summed over / maximum of the affected queries |
| `deployment_us_sum`, `deployment_us_max` | Deployment time summed over / maximum of the affected queries |
| `broken_queries` | `deploy` + `batch` only: queries not running after the request |

## Differences to the original

`plan-original` matches the original's topology, number of queries, capacities and event sequence: 4000 events in batches of 10 moves
(the original's `BatchSize` counts moves, i.e., 20 events per request), so 200 requests on one cluster. Remaining differences:

- One source per leaf instead of 32; queries are single selections instead of the query set files of the original. The initial queries
  are started with the initial placement instead of one ISQP request with query additions.
- Capacities: the original gives every node `UINT16_MAX` slots, leaves included, so operators can stay on the leaves and a move only
  changes the network path. The other presets give leaves no slots and fog nodes a tight capacity, so every move re-places the selection
  of the moved leaf and concurrent amendments compete for fog slots. `--unlimited-capacity` (used by `plan-original`) gives every node
  unlimited slots.
- Cloud nodes: the original registers 9 root nodes besides the coordinator's own worker, i.e., 10 root nodes (the fog nodes have the ids
  11 to 20).
- `plan` mode: the original ran the coordinator without workers, so its deployment requests to the absent workers (`localhost:0`) failed
  and every amendment that deployed plans counted as failed. Here, the workers accept the plans.
- The `batch` experiment is new; the original only ran the sequence. Its last configuration (holistic, one thread, one move per request)
  is not part of `plan-original`.
- Fixed issue: the original never fills its map of the leaves per fog node (`baseStationToConnectedWorkerIds`), so generating the moves
  reads from empty lists. The port keeps track of the leaves of every fog node.
