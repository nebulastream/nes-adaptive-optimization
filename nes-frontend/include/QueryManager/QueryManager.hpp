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

#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/DrainPlanner.hpp>
#include <Util/Pointers.hpp>
#include <absl/functional/any_invocable.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <DrainRequest.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SinkDrain.hpp>
#include <Version.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<QueryId, Exception> start(LogicalPlan) = 0;
    virtual std::expected<void, Exception> stop(QueryId) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
    [[nodiscard]] virtual std::expected<VersionInfo, Exception> version() const = 0;
    /// Configures the drain of a local query (phase 1 of a drain).
    virtual std::expected<void, Exception> drain(QueryId, const DrainRequest&) = 0;
    /// Starts the configured drain of a local query (phase 2 of a drain).
    virtual std::expected<void, Exception> startDrain(QueryId, DrainId) = 0;
    /// Redirects the network sink with the given channel id of a local query, which buffers since its receiver was drained.
    virtual std::expected<void, Exception> redirectSink(QueryId, const std::string& channel, const RedirectTarget&) = 0;
};

/// std::move_only_function is the C++23 equivalent but is not yet available in libc++19.
using BackendProvider = absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, DistributedQuery> queries;
    /// The plans the local queries were started with, which a drain is worked out from. Queries which were started by another
    /// QueryManager, e.g. restored from a saved state, have none and cannot be drained.
    std::unordered_map<LocalQueryRef, LogicalPlan> localPlans;
};

/// Manages the lifecycle of distributed queries in a NebulaStream cluster.
/// Tracks active queries, maintains query submission backends (embedded or gRPC),
/// and coordinates query registration, execution, and status polling.
/// Thread-safety: Single-threaded, called from CLI/REPL main thread.
/// State model: Queries transition Registered → Running → Stopped/Failed.
class QueryManager
{
    class QueryManagerBackends
    {
        SharedPtr<WorkerCatalog> workerCatalog;
        mutable BackendProvider backendProvider;
        mutable std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>> backends;
        mutable uint64_t cachedWorkerCatalogVersion = 0;
        static std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>>
        createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider);
        void rebuildBackendsIfNeeded() const;

    public:
        QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);

        auto begin() const
        {
            rebuildBackendsIfNeeded();
            return backends.begin();
        }

        auto end() const { return backends.end(); }

        bool contains(const Host& worker) const
        {
            rebuildBackendsIfNeeded();
            return backends.contains(worker);
        }

        const QuerySubmissionBackend& at(const Host& worker) const
        {
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }

        QuerySubmissionBackend& at(const Host& worker)
        {
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }
    };

    QueryManagerState state;
    QueryManagerBackends backends;
    DrainId nextDrainId = INITIAL_DRAIN_ID;

    /// Stops the given local queries to cancel a drain which could not start.
    void cancelDrain(const std::unordered_set<LocalQueryRef>& drained);

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state);
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
    /// Compiles and starts the distributed query on all assigned workers. Blocks until the query state has advanced past Registered.
    [[nodiscard]] std::expected<DistributedQueryId, std::vector<Exception>> start(const DistributedLogicalPlan& plan);
    std::expected<void, std::vector<Exception>> stop(const DistributedQueryId& query);
    [[nodiscard]] std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> status(const DistributedQueryId& query) const;
    [[nodiscard]] std::vector<DistributedQueryId> getRunningQueries() const;
    [[nodiscard]] std::vector<DistributedQueryId> queries() const;
    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    [[nodiscard]] std::map<Host, std::expected<VersionInfo, Exception>> workerVersions() const;
    [[nodiscard]] std::expected<DistributedQuery, Exception> getQuery(const DistributedQueryId& query) const;

    /// Drains the given local queries of the distributed query: configures every local query which takes part in the drain, and
    /// starts the drain once all of them have accepted. If one of them rejects the drain, the drained local queries are stopped.
    /// Returns once the drain has started; the drained local queries report Drained (see `status`) once their data has drained.
    [[nodiscard]] std::expected<DrainId, std::vector<Exception>>
    drain(const DistributedQueryId& query, const std::unordered_set<LocalQueryRef>& drained);

    /// Redirects the network sink with the channel id `sinkChannel` of the local query `sink`, which buffers since its receiver was
    /// drained, to the network source with the channel id `targetChannel` of the local query `target`. Both have to be local queries
    /// started by this QueryManager.
    std::expected<void, Exception>
    redirectSink(const LocalQueryRef& sink, const std::string& sinkChannel, const LocalQueryRef& target, const std::string& targetChannel);
};

}
