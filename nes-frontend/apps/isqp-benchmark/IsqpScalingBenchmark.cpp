/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/// Measures how ISQP scales with the number of topology change events (the port of the original's ISQPBenchmarkTopologyChanges).
///
/// A three-layer topology (cloud <- fog <- leaves) runs selection queries on the leaf sources, or with --query join (not in the original)
/// windowed joins of two leaves under different fog nodes. A change event moves a leaf from its fog node to the next one (a link removal
/// and a link addition, as in the original). Two experiments:
/// - sequence: a long sequence of requests on one cluster, as in the original; also shows whether the cost per request drifts.
/// - batch (not in the original): one request with an increasing number of moves, on a fresh cluster per point.
///
/// Capacities: by default, leaves have no operator slots and fog nodes twice their even share, so every move re-places the selection of
/// the moved leaf and concurrent amendments compete for fog slots. With --unlimited-capacity, every node is unlimited, as in the original
/// (UINT16_MAX slots per node), so the selections stay on the leaves and a move only changes the network path.
///
/// Two modes:
/// - plan: the workers only pretend to run the plans. The original ran the coordinator without workers, so its deployment requests to the
///   absent workers failed; here, they succeed. The deployment phase still runs; a drained plan counts as stopped once a reconfiguration
///   marker with its drain event was injected.
/// - deploy: embedded workers run the plans and communicate over the in-memory transport, so the times include ISQD.
///
/// Fixed issue: the original never fills its map of the leaves per fog node, so generating the moves reads from empty lists. The
/// cluster below keeps track of the leaves of every fog node.
///
/// Shortcut: torn-down embedded workers leave network sender threads behind, and the process aborts once it created 4096 threads
/// (sharded-slab's limit, about 100 clusters of 21 workers). Long deploy runs are split into processes with --first-run and --append
/// (see evaluation/isqp-scaling/run.sh).

#include <QueryManager/IncrementalPlacementRequest.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <nes-network-bindings/lib.h>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <PlacementAmendmentMode.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <Thread.hpp>
#include <TopologyChangeEvent.hpp>
#include <Version.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerOptimizerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
namespace
{

/// The local queries that terminated, shared by the dry-run backends of all workers: a reconfiguration marker reaches the drained plans
/// of other workers over the data channels, which the dry run skips.
struct DryRunState
{
    std::mutex mutex;
    std::unordered_set<std::string> stoppedLocalQueries;
};

/// A worker that accepts every plan without running it.
class DryRunBackend final : public QuerySubmissionBackend
{
public:
    explicit DryRunBackend(std::shared_ptr<DryRunState> state) : state(std::move(state)) { }

    std::expected<QueryId, Exception> start(LogicalPlan plan) override
    {
        return QueryId::create(LocalQueryId(generateUUID()), plan.getQueryId().getDistributedQueryId());
    }

    std::expected<void, Exception> stop(QueryId queryId) override
    {
        const std::scoped_lock lock(state->mutex);
        state->stoppedLocalQueries.insert(queryId.getLocalQueryId().getRawValue());
        return {};
    }

    std::expected<bool, Exception> addReconfigurationMarker(QueryId, const ReconfigurationMarker& marker) override
    {
        const std::scoped_lock lock(state->mutex);
        for (const auto& [localQueryId, event] : marker.getAllReconfigurationEvents())
        {
            if (not std::holds_alternative<UpdateQueryMetadata>(event))
            {
                state->stoppedLocalQueries.insert(localQueryId);
            }
        }
        return true;
    }

    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId queryId) const override
    {
        const std::scoped_lock lock(state->mutex);
        const auto stopped = state->stoppedLocalQueries.contains(queryId.getLocalQueryId().getRawValue());
        return LocalQueryStatusSnapshot{
            .queryId = queryId,
            .state = stopped ? QueryStatus::Stopped : QueryStatus::Running,
            .metrics = {},
            .networkSinksConnected = not stopped};
    }

    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override
    {
        return WorkerStatus{};
    }

    [[nodiscard]] std::expected<VersionInfo, Exception> version() const override { return VersionInfo("dry-run"); }

private:
    std::shared_ptr<DryRunState> state;
};

enum class Mode : uint8_t
{
    Plan,
    Deploy
};

enum class QueryType : uint8_t
{
    Selection,
    Join
};

struct BenchmarkConfiguration
{
    Mode mode = Mode::Plan;
    QueryType queryType = QueryType::Selection;
    size_t cloudNodes = 1;
    size_t fogNodes = 10;
    size_t leafNodes = 100;
    size_t queries = 100;
    /// Operator slots per fog node. Zero: twice the even share of the queries.
    size_t fogCapacity = 0;
    /// Every node has unlimited operator slots, as in the original. Overrides fogCapacity.
    bool unlimitedCapacity = false;
    size_t emitRate = 100;
    size_t workerThreads = 1;
    size_t workerMemory = size_t{256} * 1024 * 1024;
    std::chrono::milliseconds drainTimeout = std::chrono::seconds(60);
};

struct RunConfiguration
{
    bool incremental = true;
    PlacementAmendmentMode amendmentMode = PlacementAmendmentMode::Optimistic;
    size_t threads = 1;
};

size_t effectiveFogCapacity(const BenchmarkConfiguration& configuration)
{
    return configuration.fogCapacity != 0 ? configuration.fogCapacity
                                          : 2 * ((configuration.queries + configuration.fogNodes - 1) / configuration.fogNodes);
}

/// The fog node of a leaf in the initial topology: the leaves are spread over the fog nodes in contiguous blocks, as in the original.
size_t initialFogOfLeaf(const BenchmarkConfiguration& configuration, const size_t leaf)
{
    return leaf * configuration.fogNodes / configuration.leafNodes;
}

/// The first leaf of the block of a fog node in the initial topology.
size_t firstLeafOfFog(const BenchmarkConfiguration& configuration, const size_t fog)
{
    return (fog * configuration.leafNodes + configuration.fogNodes - 1) / configuration.fogNodes;
}

/// The leaves query q reads: leaf l = q mod L and, for a join, the leaf at the same position in the block of the next fog node (the leaf
/// L/F further when L is a multiple of F).
std::pair<size_t, size_t> leavesOfQuery(const BenchmarkConfiguration& configuration, const size_t query)
{
    const auto left = query % configuration.leafNodes;
    const auto fog = initialFogOfLeaf(configuration, left);
    const auto nextFog = (fog + 1) % configuration.fogNodes;
    const auto nextBlockSize = firstLeafOfFog(configuration, nextFog + 1) - firstLeafOfFog(configuration, nextFog);
    return {left, firstLeafOfFog(configuration, nextFog) + ((left - firstLeafOfFog(configuration, fog)) % nextBlockSize)};
}

std::string describeQueryType(const BenchmarkConfiguration& configuration)
{
    return configuration.queryType == QueryType::Selection ? "selection" : "join";
}

std::string describeFogCapacity(const BenchmarkConfiguration& configuration)
{
    return configuration.unlimitedCapacity ? std::string("unlimited") : std::to_string(effectiveFogCapacity(configuration));
}

/// A three-layer cluster with its catalogs, query manager and running queries.
class Cluster
{
public:
    Cluster(const BenchmarkConfiguration& configuration, const std::string& suffix)
        : configuration(configuration), suffix(suffix), fogOfLeaf(configuration.leafNodes)
    {
        const Capacity fogCapacity = configuration.unlimitedCapacity ? Capacity{CapacityKind::Unlimited{}}
                                                                     : Capacity{CapacityKind::Limited{effectiveFogCapacity(configuration)}};
        const Capacity leafCapacity
            = configuration.unlimitedCapacity ? Capacity{CapacityKind::Unlimited{}} : Capacity{CapacityKind::Limited{0}};
        std::vector<Host> clouds;
        for (size_t cloud = 0; cloud < configuration.cloudNodes; ++cloud)
        {
            clouds.push_back(host("cloud", cloud));
            addWorker(clouds.back(), "cloud", cloud, CapacityKind::Unlimited{}, {});
        }
        for (size_t fog = 0; fog < configuration.fogNodes; ++fog)
        {
            addWorker(host("fog", fog), "fog", fog, fogCapacity, clouds);
        }
        /// The leaves are spread over the fog nodes in contiguous blocks, as in the original.
        leavesOfFog.resize(configuration.fogNodes);
        for (size_t leaf = 0; leaf < configuration.leafNodes; ++leaf)
        {
            const auto fog = initialFogOfLeaf(configuration, leaf);
            fogOfLeaf.at(leaf) = fog;
            leavesOfFog.at(fog).push_back(leaf);
            addWorker(host("leaf", leaf), "leaf", leaf, leafCapacity, {host("fog", fog)});
        }
        addSourcesAndSinks();
        queryManager = std::make_unique<QueryManager>(copyPtr(workerCatalog), createBackendProvider());
    }

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&&) = delete;
    Cluster& operator=(Cluster&&) = delete;

    ~Cluster()
    {
        for (const auto& queryId : queryIds)
        {
            if (auto stopped = queryManager->stop(queryId); not stopped)
            {
                NES_WARNING("Could not stop query {}", queryId);
            }
        }
    }

    /// Starts the queries; query q reads the source of leaf q mod L (and, for a join, a leaf under the next fog node) and writes to a cloud
    /// node. A join needs unique field names, so in join mode the fields of stream<l> are id<l> and ts<l>.
    void startQueries()
    {
        const QueryOptimizer optimizer(
            defaultConfiguration<QueryOptimizerConfiguration>(),
            copyPtr(sourceCatalog),
            copyPtr(sinkCatalog),
            copyPtr(workerCatalog),
            std::make_shared<ModelCatalog>());
        for (size_t query = 0; query < configuration.queries; ++query)
        {
            const auto [left, right] = leavesOfQuery(configuration, query);
            const auto sql = configuration.queryType == QueryType::Selection
                ? fmt::format("SELECT * FROM stream{} WHERE id >= 0 INTO sink{}", left, query)
                : fmt::format(
                      "SELECT * FROM (SELECT * FROM stream{0}) INNER JOIN (SELECT * FROM stream{1}) ON id{0} = id{1} "
                      "WINDOW TUMBLING (ts{0}, ts{1}, size 1 sec) INTO sink{2}",
                      left,
                      right,
                      query);
            auto started = queryManager->start(optimizer.optimize(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql)));
            if (not started)
            {
                throw QueryStartFailed("Could not start query {}: {}", query, fmt::join(started.error(), ", "));
            }
            queryIds.push_back(*started);
        }
    }

    /// The next moves, round-robin over the fog nodes as in the original: the first leaf of a fog node moves to the next fog node.
    std::vector<TopologyChangeEvent> nextMoves(const size_t numberOfMoves)
    {
        std::vector<TopologyChangeEvent> events;
        while (events.size() < 2 * numberOfMoves)
        {
            auto& leaves = leavesOfFog.at(nextFog);
            const auto from = nextFog;
            nextFog = (nextFog + 1) % configuration.fogNodes;
            if (leaves.empty())
            {
                continue;
            }
            const auto leaf = leaves.front();
            leaves.pop_front();
            leavesOfFog.at(nextFog).push_back(leaf);
            fogOfLeaf.at(leaf) = nextFog;
            events.emplace_back(RemoveLinkEvent{.upstream = host("leaf", leaf), .downstream = host("fog", from)});
            events.emplace_back(AddLinkEvent{.upstream = host("leaf", leaf), .downstream = host("fog", nextFog)});
        }
        return events;
    }

    IncrementalPlacementResult execute(const RunConfiguration& run, const std::vector<TopologyChangeEvent>& events)
    {
        IncrementalPlacementRequest request(
            *queryManager,
            copyPtr(workerCatalog),
            QueryDecomposer(copyPtr(workerCatalog), copyPtr(sourceCatalog), copyPtr(sinkCatalog)),
            defaultConfiguration<QueryOptimizerConfiguration>().network,
            IncrementalPlacementConfiguration{
                .enableIncrementalPlacement = run.incremental,
                .drainTimeout = configuration.drainTimeout,
                .placementAmendmentMode = run.amendmentMode,
                .placementAmendmentThreadCount = run.threads});
        return request.execute(events);
    }

    /// The number of queries that are not running anymore. Only meaningful when deploying. Newly started plans need a moment until they
    /// report that they run, so the count settles for up to the given time.
    [[nodiscard]] size_t countBrokenQueries(const std::chrono::milliseconds settleTime) const
    {
        const auto deadline = std::chrono::steady_clock::now() + settleTime;
        while (true)
        {
            std::vector<std::string> broken;
            for (const auto& queryId : queryIds)
            {
                const auto status = queryManager->status(queryId);
                if (not status)
                {
                    broken.push_back(fmt::format("{}: no status", queryId));
                }
                else if (const auto global = status->getGlobalQueryStatus(); global != DistributedQueryStatus::Running)
                {
                    broken.push_back(fmt::format("{}: {}", queryId, magic_enum::enum_name(global)));
                }
            }
            if (broken.empty() or std::chrono::steady_clock::now() > deadline)
            {
                if (not broken.empty())
                {
                    NES_WARNING("Queries that do not run: {}", fmt::join(broken, ", "));
                }
                return broken.size();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

private:
    [[nodiscard]] Host host(const std::string& layer, const size_t index) const
    {
        return Host(fmt::format("{}{}{}:8080", layer, index, suffix));
    }

    void addWorker(
        const Host& worker, const std::string& layer, const size_t index, const Capacity capacity, const std::vector<Host>& downstream)
    {
        if (not workerCatalog->addWorker(worker, fmt::format("{}{}{}:9090", layer, index, suffix), capacity, downstream))
        {
            throw InvalidTopology("Duplicate worker {}", worker);
        }
    }

    void addSourcesAndSinks()
    {
        const auto field = [](const std::string& name) { return UnqualifiedUnboundField{Identifier::parse(name), DataType::Type::UINT64}; };
        const auto join = configuration.queryType == QueryType::Join;
        /// Join mode: the timestamp advances by about a millisecond per millisecond at the configured emit rate.
        const auto generatorSchema = join
            ? fmt::format(
                  "SEQUENCE UINT64 0 1000000000 1, SEQUENCE UINT64 0 1000000000000 {}", std::max<size_t>(1, 1000 / configuration.emitRate))
            : std::string("SEQUENCE UINT64 0 1000000000 1");
        const auto sourceSchema = [&](const size_t leaf)
        {
            return join ? Schema<UnqualifiedUnboundField, Ordered>{field(fmt::format("id{}", leaf)), field(fmt::format("ts{}", leaf))}
                        : Schema<UnqualifiedUnboundField, Ordered>{field("id")};
        };
        for (size_t leaf = 0; leaf < configuration.leafNodes; ++leaf)
        {
            const auto logicalSource
                = sourceCatalog->addLogicalSource(Identifier::parse(fmt::format("stream{}", leaf)), sourceSchema(leaf));
            const auto physicalSource = sourceCatalog->addPhysicalSource(
                logicalSource.value(),
                Identifier::parse("Generator"),
                host("leaf", leaf),
                {{Identifier::parse("stop_generator_when_sequence_finishes"), "NONE"},
                 {Identifier::parse("seed"), "1"},
                 {Identifier::parse("generator_rate_type"), "FIXED"},
                 {Identifier::parse("generator_rate_config"), fmt::format("emit_rate {}", configuration.emitRate)},
                 {Identifier::parse("generator_schema"), generatorSchema}},
                {{Identifier::parse("type"), "CSV"}});
            if (not physicalSource)
            {
                throw physicalSource.error();
            }
        }
        for (size_t query = 0; query < configuration.queries; ++query)
        {
            const auto [left, right] = leavesOfQuery(configuration, query);
            const auto sinkSchema = join
                ? Schema<
                      UnqualifiedUnboundField,
                      Ordered>{field("start"), field("end"), field(fmt::format("id{}", left)), field(fmt::format("ts{}", left)), field(fmt::format("id{}", right)), field(fmt::format("ts{}", right))}
                : sourceSchema(left);
            const auto sink = sinkCatalog->addSinkDescriptor(
                Identifier::parse(fmt::format("sink{}", query)),
                sinkSchema,
                Identifier::parse("File"),
                host("cloud", query % configuration.cloudNodes),
                {{Identifier::parse("FILE_PATH"), "/dev/null"},
                 {Identifier::parse("APPEND"), "true"},
                 {Identifier::parse("OUTPUT_FORMAT"), "CSV"}},
                {});
            if (not sink)
            {
                throw sink.error();
            }
        }
    }

    BackendProvider createBackendProvider() const
    {
        if (configuration.mode == Mode::Plan)
        {
            return [state = std::make_shared<DryRunState>()](const WorkerConfig&) -> UniquePtr<QuerySubmissionBackend>
            { return std::make_unique<DryRunBackend>(state); };
        }
        return createEmbeddedBackend(
            [threads = configuration.workerThreads, memory = configuration.workerMemory](const WorkerConfig&)
            {
                auto worker = defaultConfiguration<WorkerOptimizerConfig>().worker;
                worker.workerConfiguration.queryEngine.numberOfWorkerThreads = threads;
                worker.workerConfiguration.totalMemoryInBytes = memory;
                return worker;
            });
    }

    BenchmarkConfiguration configuration;
    std::string suffix;
    SharedPtr<WorkerCatalog> workerCatalog = std::make_shared<WorkerCatalog>();
    SharedPtr<SourceCatalog> sourceCatalog = std::make_shared<SourceCatalog>();
    SharedPtr<SinkCatalog> sinkCatalog = std::make_shared<SinkCatalog>();
    std::unique_ptr<QueryManager> queryManager;
    std::vector<DistributedQueryId> queryIds;
    std::vector<size_t> fogOfLeaf;
    std::vector<std::deque<size_t>> leavesOfFog;
    size_t nextFog = 0;
};

class CsvWriter
{
public:
    CsvWriter(const std::string& path, const bool append)
    {
        const auto writeHeader = not append or not std::filesystem::exists(path) or std::filesystem::file_size(path) == 0;
        file.open(path, append ? std::ios::app : std::ios::trunc);
        if (not file)
        {
            throw InvalidConfigParameter("Cannot open the output file {}", path);
        }
        if (not writeHeader)
        {
            return;
        }
        file << "experiment,mode,cloud_nodes,fog_nodes,leaf_nodes,queries,query,fog_capacity,incremental,amendment_mode,threads,run,"
                "request,moves,"
                "events,affected_queries,pending_queries,failed_amendments,partial_amendments,processing_us,amendment_us,placement_us_sum,"
                "placement_us_max,deployment_us_sum,"
                "deployment_us_max,broken_queries\n";
    }

    void write(
        const std::string& experiment,
        const BenchmarkConfiguration& configuration,
        const RunConfiguration& run,
        const size_t runIndex,
        const size_t request,
        const size_t moves,
        const IncrementalPlacementResult& result,
        const std::optional<size_t> brokenQueries)
    {
        int64_t placementSum = 0;
        int64_t placementMax = 0;
        int64_t deploymentSum = 0;
        int64_t deploymentMax = 0;
        for (const auto& amendment : result.amendments)
        {
            placementSum += amendment.placementTime.count();
            placementMax = std::max(placementMax, amendment.placementTime.count());
            deploymentSum += amendment.deploymentTime.count();
            deploymentMax = std::max(deploymentMax, amendment.deploymentTime.count());
        }
        file << fmt::format(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            experiment,
            configuration.mode == Mode::Plan ? "plan" : "deploy",
            configuration.cloudNodes,
            configuration.fogNodes,
            configuration.leafNodes,
            configuration.queries,
            describeQueryType(configuration),
            describeFogCapacity(configuration),
            run.incremental,
            magic_enum::enum_name(run.amendmentMode),
            run.threads,
            runIndex,
            request,
            moves,
            2 * moves,
            result.numberOfAffectedQueries,
            result.numberOfPendingQueries,
            result.numberOfFailedAmendments,
            result.numberOfPartialAmendments,
            result.processingTime.count(),
            result.amendmentTime.count(),
            placementSum,
            placementMax,
            deploymentSum,
            deploymentMax,
            brokenQueries.has_value() ? std::to_string(*brokenQueries) : std::string{});
        file.flush();
    }

private:
    std::ofstream file;
};

std::vector<size_t> parseSizes(const std::string& list)
{
    return list | std::views::split(',')
        | std::views::transform([](auto part) { return std::stoul(std::string(part.begin(), part.end())); })
        | std::ranges::to<std::vector>();
}

std::vector<std::string> parseNames(const std::string& list)
{
    return list | std::views::split(',') | std::views::transform([](auto part) { return std::string(part.begin(), part.end()); })
        | std::ranges::to<std::vector>();
}

std::vector<RunConfiguration> parseRunConfigurations(const argparse::ArgumentParser& program)
{
    std::vector<RunConfiguration> runs;
    for (const auto& placement : parseNames(program.get<std::string>("--placement")))
    {
        if (placement != "incremental" and placement != "holistic")
        {
            throw InvalidConfigParameter("--placement must be incremental or holistic, but was {}", placement);
        }
        for (const auto& amendmentModeName : parseNames(program.get<std::string>("--amendment-modes")))
        {
            const auto amendmentMode = magic_enum::enum_cast<PlacementAmendmentMode>(amendmentModeName, magic_enum::case_insensitive);
            if (not amendmentMode)
            {
                throw InvalidConfigParameter("--amendment-modes must be Optimistic or Pessimistic, but was {}", amendmentModeName);
            }
            for (const auto threads : parseSizes(program.get<std::string>("--threads")))
            {
                runs.push_back({.incremental = placement == "incremental", .amendmentMode = *amendmentMode, .threads = threads});
            }
        }
    }
    return runs;
}

std::string describe(const RunConfiguration& run)
{
    return fmt::format(
        "{} {} threads={}", run.incremental ? "incremental" : "holistic", magic_enum::enum_name(run.amendmentMode), run.threads);
}

}
}

int main(int argc, char** argv)
{
    using namespace NES;
    argparse::ArgumentParser program("isqp-scaling-benchmark");
    program.add_description("Measures how ISQP scales with the number of topology change events on a cloud <- fog <- leaf topology.");
    program.add_argument("--mode")
        .default_value(std::string{"plan"})
        .choices("plan", "deploy")
        .help("plan: dry-run workers; deploy: embedded workers");
    program.add_argument("--experiments").default_value(std::string{"batch,sequence"}).help("Comma-separated: batch, sequence");
    program.add_argument("--cloud").default_value(size_t{1}).scan<'u', size_t>().help("Number of cloud nodes");
    program.add_argument("--fog").default_value(size_t{10}).scan<'u', size_t>().help("Number of fog nodes");
    program.add_argument("--leaves").default_value(size_t{100}).scan<'u', size_t>().help("Number of leaf nodes, one source each");
    program.add_argument("--queries").default_value(size_t{100}).scan<'u', size_t>().help("Number of queries; query q reads leaf q mod L");
    program.add_argument("--query")
        .default_value(std::string{"selection"})
        .choices("selection", "join")
        .help("selection: a filter on leaf q mod L; join: a 1 s tumbling window join of leaf q mod L and a leaf of the next fog node");
    program.add_argument("--fog-capacity")
        .default_value(size_t{0})
        .scan<'u', size_t>()
        .help("Operator slots per fog node; 0: twice the even share");
    program.add_argument("--unlimited-capacity")
        .flag()
        .help("Every node has unlimited operator slots, as in the original (overrides --fog-capacity)");
    program.add_argument("--batch-moves")
        .default_value(std::string{"1,2,4,8,16,32,64,128,256"})
        .help("batch: the numbers of leaf moves per request (one move is two events)");
    program.add_argument("--sequence-requests").default_value(size_t{200}).scan<'u', size_t>().help("sequence: number of requests");
    program.add_argument("--sequence-moves").default_value(size_t{1}).scan<'u', size_t>().help("sequence: leaf moves per request");
    program.add_argument("--placement").default_value(std::string{"incremental,holistic"}).help("Comma-separated: incremental, holistic");
    program.add_argument("--amendment-modes")
        .default_value(std::string{"Optimistic,Pessimistic"})
        .help("Comma-separated: Optimistic, Pessimistic");
    program.add_argument("--threads").default_value(std::string{"1,4"}).help("Comma-separated placement amendment thread counts");
    program.add_argument("--runs").default_value(size_t{3}).scan<'u', size_t>().help("Repetitions of every point");
    program.add_argument("--emit-rate").default_value(size_t{100}).scan<'u', size_t>().help("deploy: tuples per second and source");
    program.add_argument("--worker-threads").default_value(size_t{1}).scan<'u', size_t>().help("deploy: worker threads per worker");
    program.add_argument("--worker-memory-mb")
        .default_value(size_t{256})
        .scan<'u', size_t>()
        .help("deploy: buffer memory per worker in MiB (the process uses roughly 140 MiB per worker with the default)");
    program.add_argument("--drain-timeout-ms").default_value(size_t{60000}).scan<'u', size_t>().help("deploy: drain timeout per amendment");
    program.add_argument("--warmup-ms")
        .default_value(size_t{1000})
        .scan<'u', size_t>()
        .help("deploy: time the queries run before the first request");
    program.add_argument("--log-level").default_value(std::string{"LOG_WARNING"}).help("Log level of isqp-scaling-benchmark.log");
    program.add_argument("--first-run").default_value(size_t{0}).scan<'u', size_t>().help("Number of the first repetition");
    program.add_argument("--append").flag().help("Append to the output file instead of overwriting it");
    program.add_argument("--strict").flag().help("Exit with 2 if an amendment failed or a query stopped running");
    program.add_argument("-o", "--output").default_value(std::string{"isqp-scaling.csv"}).help("CSV output file");

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& exception)
    {
        std::cerr << exception.what() << "\n" << program;
        return 1;
    }

    try
    {
        loadBuiltinPlugins();
        Thread::initializeThread(Host("isqp-scaling-benchmark"), "main");
        Logger::setupLogging(
            "isqp-scaling-benchmark.log",
            magic_enum::enum_cast<LogLevel>(program.get<std::string>("--log-level")).value_or(LogLevel::LOG_WARNING));
        BenchmarkConfiguration configuration{
            .mode = program.get<std::string>("--mode") == "plan" ? Mode::Plan : Mode::Deploy,
            .queryType = program.get<std::string>("--query") == "selection" ? QueryType::Selection : QueryType::Join,
            .cloudNodes = program.get<size_t>("--cloud"),
            .fogNodes = program.get<size_t>("--fog"),
            .leafNodes = program.get<size_t>("--leaves"),
            .queries = program.get<size_t>("--queries"),
            .fogCapacity = program.get<size_t>("--fog-capacity"),
            .unlimitedCapacity = program.get<bool>("--unlimited-capacity"),
            .emitRate = program.get<size_t>("--emit-rate"),
            .workerThreads = program.get<size_t>("--worker-threads"),
            .workerMemory = program.get<size_t>("--worker-memory-mb") * 1024 * 1024,
            .drainTimeout = std::chrono::milliseconds(program.get<size_t>("--drain-timeout-ms"))};
        if (configuration.cloudNodes == 0 or configuration.fogNodes == 0 or configuration.leafNodes == 0 or configuration.queries == 0)
        {
            throw InvalidConfigParameter("Every layer needs at least one node and at least one query must run");
        }
        if (configuration.queryType == QueryType::Join and (configuration.fogNodes < 2 or configuration.leafNodes < configuration.fogNodes))
        {
            throw InvalidConfigParameter("--query join needs at least two fog nodes and at least one leaf per fog node");
        }
        if (configuration.mode == Mode::Deploy)
        {
            enable_memcom();
        }
        const auto warmup = std::chrono::milliseconds(program.get<size_t>("--warmup-ms"));
        const auto runs = parseRunConfigurations(program);
        const auto experiments = parseNames(program.get<std::string>("--experiments"));
        const auto repetitions = program.get<size_t>("--runs");
        CsvWriter csv(program.get<std::string>("--output"), program.get<bool>("--append"));
        const auto firstRun = program.get<size_t>("--first-run");

        size_t errors = 0;
        size_t clusterCounter = 0;
        auto createCluster = [&]
        {
            auto cluster = std::make_unique<Cluster>(configuration, fmt::format("-c{}", clusterCounter++));
            cluster->startQueries();
            if (configuration.mode == Mode::Deploy)
            {
                std::this_thread::sleep_for(warmup);
            }
            return cluster;
        };
        auto brokenQueries = [&](const Cluster& cluster)
        { return configuration.mode == Mode::Deploy ? std::optional(cluster.countBrokenQueries(std::chrono::seconds(5))) : std::nullopt; };

        /// The first request of a process pays one-time costs (e.g., the first network channels); a discarded request absorbs them.
        if (configuration.mode == Mode::Deploy and not runs.empty())
        {
            const auto cluster = createCluster();
            [[maybe_unused]] const auto warmupResult = cluster->execute(runs.front(), cluster->nextMoves(1));
        }

        for (const auto& run : runs)
        {
            for (size_t repetition = firstRun; repetition < firstRun + repetitions; ++repetition)
            {
                if (std::ranges::contains(experiments, "batch"))
                {
                    for (const auto moves : parseSizes(program.get<std::string>("--batch-moves")))
                    {
                        const auto cluster = createCluster();
                        const auto result = cluster->execute(run, cluster->nextMoves(moves));
                        const auto broken = brokenQueries(*cluster);
                        errors += result.numberOfFailedAmendments + broken.value_or(0);
                        csv.write("batch", configuration, run, repetition, 0, moves, result, broken);
                        std::cout << fmt::format(
                            "batch    {} run={} moves={} affected={} failed={} partial={} broken={} time={}\n",
                            describe(run),
                            repetition,
                            moves,
                            result.numberOfAffectedQueries,
                            result.numberOfFailedAmendments,
                            result.numberOfPartialAmendments,
                            broken.has_value() ? std::to_string(*broken) : "-",
                            std::chrono::duration_cast<std::chrono::milliseconds>(result.processingTime));
                    }
                }
                if (std::ranges::contains(experiments, "sequence"))
                {
                    const auto cluster = createCluster();
                    const auto moves = program.get<size_t>("--sequence-moves");
                    std::chrono::microseconds total{0};
                    size_t failed = 0;
                    size_t partial = 0;
                    for (size_t request = 0; request < program.get<size_t>("--sequence-requests"); ++request)
                    {
                        const auto result = cluster->execute(run, cluster->nextMoves(moves));
                        total += result.processingTime;
                        failed += result.numberOfFailedAmendments;
                        partial += result.numberOfPartialAmendments;
                        csv.write("sequence", configuration, run, repetition, request, moves, result, std::nullopt);
                    }
                    const auto broken = brokenQueries(*cluster);
                    errors += failed + broken.value_or(0);
                    std::cout << fmt::format(
                        "sequence {} run={} requests={} failed={} partial={} broken={} time={}\n",
                        describe(run),
                        repetition,
                        program.get<size_t>("--sequence-requests"),
                        failed,
                        partial,
                        broken.has_value() ? std::to_string(*broken) : "-",
                        std::chrono::duration_cast<std::chrono::milliseconds>(total));
                }
            }
        }
        if (errors != 0 and program.get<bool>("--strict"))
        {
            std::cerr << "isqp-scaling-benchmark: " << errors << " failed amendments or broken queries\n";
            return 2;
        }
    }
    catch (const std::exception& exception)
    {
        std::cerr << "isqp-scaling-benchmark failed: " << exception.what() << "\n";
        return 1;
    }
    return 0;
}
