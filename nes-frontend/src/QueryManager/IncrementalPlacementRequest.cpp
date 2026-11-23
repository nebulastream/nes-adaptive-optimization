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

#include <QueryManager/IncrementalPlacementRequest.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <iterator>
#include <ranges>
#include <set>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Placement/IncrementalPlacementUtils.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Placement/QueryPlacementAmendmentPhase.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Pointers.hpp>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <QueryStatus.hpp>
#include <SharedQueryPlan.hpp>
#include <TopologyChangeEvent.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

namespace
{
std::chrono::microseconds elapsedSince(const std::chrono::steady_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
}

bool isDrained(const DeploymentContext& context)
{
    return context.plan.state == DecomposedQueryPlanState::MarkedForMigration
        or context.plan.state == DecomposedQueryPlanState::MarkedForUpdateAndDrain;
}
}

DeploymentPhase::DeploymentPhase(QueryManager& queryManager, IncrementalPlacementConfiguration configuration)
    : queryManager(queryManager), configuration(configuration)
{
}

ReconfigurationMarker DeploymentPhase::computeReconfigurationMarker(const DeploymentUnit& deploymentUnit)
{
    auto marker = ReconfigurationMarker::create();
    for (const auto& context : deploymentUnit.deploymentAdditionContexts)
    {
        if (not context.plan.localQueryId.has_value())
        {
            continue;
        }
        switch (context.plan.state)
        {
            case DecomposedQueryPlanState::MarkedForRedeployment:
                marker.addReconfigurationEvent(*context.plan.localQueryId, UpdateQueryMetadata{.networkSinkUpdates = context.sinkUpdates});
                break;
            case DecomposedQueryPlanState::MarkedForMigration:
                marker.addReconfigurationEvent(
                    *context.plan.localQueryId, DrainQueryMetadata{.detachedChannels = context.detachedChannels});
                break;
            case DecomposedQueryPlanState::MarkedForUpdateAndDrain:
                marker.addReconfigurationEvent(
                    *context.plan.localQueryId,
                    UpdateAndDrainQueryMetadata{
                        .detachedChannels = context.detachedChannels, .stateMigrationChannels = context.stateMigrationChannels});
                break;
            default:
                break;
        }
    }
    return marker;
}

std::expected<void, std::vector<Exception>>
DeploymentPhase::execute(const DistributedQueryId& queryId, const DeploymentUnit& deploymentUnit)
{
    auto& globalExecutionPlan = queryManager.getGlobalExecutionPlan();
    std::vector<Exception> exceptions;
    auto localQueryOf = [&queryId](const DecomposedQueryPlan& plan) { return QueryId::create(*plan.localQueryId, queryId); };

    /// 1. Remove the plans of the holistic redeployment.
    for (const auto& context : deploymentUnit.deploymentRemovalContexts)
    {
        if (context.plan.localQueryId.has_value() and queryManager.hasWorker(context.plan.worker))
        {
            if (auto result = queryManager.stopLocalQuery(queryId, context.plan.worker, localQueryOf(context.plan)); not result)
            {
                exceptions.emplace_back(result.error());
            }
        }
        else if (context.plan.localQueryId.has_value())
        {
            queryManager.removeLocalQuery(queryId, context.plan.worker, localQueryOf(context.plan));
        }
    }

    /// 2. Deploy the new plans. They start asynchronously; their network sources wait for their senders.
    std::vector<DeploymentContext> contexts = deploymentUnit.deploymentAdditionContexts;
    for (auto& context : contexts)
    {
        if (context.plan.state != DecomposedQueryPlanState::MarkedForDeployment)
        {
            continue;
        }
        auto result = queryManager.startLocalQuery(queryId, context.plan.worker, context.plan.plan);
        if (not result)
        {
            exceptions.emplace_back(result.error());
            continue;
        }
        context.plan.localQueryId = result->getLocalQueryId();
        globalExecutionPlan.setLocalQueryId(queryId, context.plan.id, result->getLocalQueryId());
        globalExecutionPlan.setState(queryId, context.plan.id, DecomposedQueryPlanState::Running);
    }

    /// 3. Inject the reconfiguration marker at the marker units. It reaches the other affected plans over the data channels.
    if (not deploymentUnit.reconfigurationMarkerUnits.empty())
    {
        const auto marker = computeReconfigurationMarker(
            DeploymentUnit{
                .deploymentRemovalContexts = {},
                .deploymentAdditionContexts = contexts,
                .reconfigurationMarkerUnits = {},
                .stateMigrations = {}});
        NES_DEBUG("Injecting reconfiguration marker {} of query {}", marker, queryId);
        for (const auto& unit : deploymentUnit.reconfigurationMarkerUnits)
        {
            const auto plan = globalExecutionPlan.getDecomposedQueryPlan(queryId, unit.plan);
            if (not plan.has_value() or not plan->localQueryId.has_value() or not queryManager.hasWorker(unit.worker))
            {
                NES_WARNING("Cannot inject the reconfiguration marker of query {} into plan {} on {}", queryId, unit.plan, unit.worker);
                continue;
            }
            if (auto result = queryManager.addReconfigurationMarker(unit.worker, localQueryOf(*plan), marker); not result)
            {
                exceptions.emplace_back(result.error());
            }
        }
    }

    /// 4. Updated plans run their new version. Drained plans terminate by themselves; once they did, they are removed.
    const auto deadline = std::chrono::steady_clock::now() + configuration.drainTimeout;
    for (const auto& context : contexts)
    {
        if (context.plan.state == DecomposedQueryPlanState::MarkedForRedeployment)
        {
            globalExecutionPlan.setState(queryId, context.plan.id, DecomposedQueryPlanState::Running);
            continue;
        }
        if (not isDrained(context))
        {
            continue;
        }
        globalExecutionPlan.setState(queryId, context.plan.id, DecomposedQueryPlanState::Migrating);
        if (context.plan.localQueryId.has_value())
        {
            const auto localQuery = localQueryOf(context.plan);
            while (queryManager.hasWorker(context.plan.worker))
            {
                const auto status = queryManager.localQueryStatus(context.plan.worker, localQuery);
                if (not status or status->state == QueryStatus::Stopped or status->state == QueryStatus::Failed)
                {
                    break;
                }
                if (std::chrono::steady_clock::now() > deadline)
                {
                    exceptions.emplace_back(QueryStopFailed(
                        "Plan {} of query {} on {} was not drained within {}",
                        context.plan.id,
                        queryId,
                        context.plan.worker,
                        configuration.drainTimeout));
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            queryManager.removeLocalQuery(queryId, context.plan.worker, localQuery);
        }
        globalExecutionPlan.removeDecomposedQueryPlan(queryId, context.plan.id);
    }

    /// 5. Wait until the new and updated plans are connected to their receivers. A later amendment may drain such a plan and reuse its
    /// channel for a new chain; if the plan had not connected by then, the receiver would hand the channel to the new sender and the
    /// drained plan could never deliver its data and terminate. Deviation: the original creates a new partition for every new chain.
    for (const auto& context : contexts)
    {
        if ((context.plan.state != DecomposedQueryPlanState::MarkedForDeployment
             and context.plan.state != DecomposedQueryPlanState::MarkedForRedeployment)
            or not context.plan.localQueryId.has_value())
        {
            continue;
        }
        const auto localQuery = localQueryOf(context.plan);
        while (queryManager.hasWorker(context.plan.worker))
        {
            const auto status = queryManager.localQueryStatus(context.plan.worker, localQuery);
            if (not status or status->state == QueryStatus::Stopped or status->state == QueryStatus::Failed
                or (status->state == QueryStatus::Running and status->networkSinksConnected))
            {
                break;
            }
            if (std::chrono::steady_clock::now() > deadline)
            {
                exceptions.emplace_back(QueryStartFailed(
                    "Plan {} of query {} on {} did not connect to its receivers within {}",
                    context.plan.id,
                    queryId,
                    context.plan.worker,
                    configuration.drainTimeout));
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected(exceptions);
    }
    return {};
}

PlacementAmendmentInstance::PlacementAmendmentInstance(
    QueryManager& queryManager,
    SharedPtr<WorkerCatalog> workerCatalog,
    const QueryDecomposer& decomposer,
    QueryOptimizerNetworkConfiguration networkConfiguration,
    IncrementalPlacementConfiguration configuration)
    : queryManager(queryManager)
    , workerCatalog(std::move(workerCatalog))
    , decomposer(decomposer)
    , networkConfiguration(std::move(networkConfiguration))
    , configuration(configuration)
{
}

PlacementAmendmentResult PlacementAmendmentInstance::execute(const DistributedQueryId& queryId)
{
    auto& sharedQueryPlans = queryManager.getSharedQueryPlans();
    const auto it = sharedQueryPlans.find(queryId);
    if (it == sharedQueryPlans.end())
    {
        NES_WARNING("Query {} is not tracked by the incremental placement", queryId);
        return {};
    }
    auto& sharedQueryPlan = it->second;
    PlacementAmendmentResult result;

    try
    {
        const auto placementStart = std::chrono::steady_clock::now();
        auto amend = [&](const bool incremental)
        {
            return QueryPlacementAmendmentPhase(
                       queryManager.getGlobalExecutionPlan(),
                       copyPtr(workerCatalog),
                       decomposer,
                       networkConfiguration,
                       incremental,
                       configuration.placementAmendmentMode)
                .execute(sharedQueryPlan);
        };
        /// Deviation: a change log entry that fails because of a shortcut of the port, which the original does not have (e.g., one sink per
        /// plan), would fail again with every retry, so the query is re-placed holistically. Other failures (e.g., a failed optimistic
        /// validation) are handled as in the original: the entry is recorded again and the next request processes it.
        bool holistic = configuration.enableIncrementalPlacement and sharedQueryPlan.takeHolisticReplacementRequirement();
        auto deploymentUnit = amend(configuration.enableIncrementalPlacement and not holistic);
        if (configuration.enableIncrementalPlacement and not holistic
            and sharedQueryPlan.getStatus() == SharedQueryPlanStatus::PartiallyProcessed
            and sharedQueryPlan.takeHolisticReplacementRequirement())
        {
            NES_WARNING("The incremental amendment of query {} is not supported by the port, re-placing it holistically", queryId);
            holistic = true;
            deploymentUnit = amend(false);
        }
        if (holistic and sharedQueryPlan.getStatus() == SharedQueryPlanStatus::PartiallyProcessed)
        {
            /// The holistic re-placement processed the change log, so the next request has to re-place the query holistically again.
            sharedQueryPlan.requireHolisticReplacement();
        }
        result.placementTime = elapsedSince(placementStart);

        const auto deploymentStart = std::chrono::steady_clock::now();
        const auto deployed = DeploymentPhase(queryManager, configuration).execute(queryId, deploymentUnit);
        result.deploymentTime = elapsedSince(deploymentStart);
        if (not deployed)
        {
            for (const auto& exception : deployed.error())
            {
                NES_ERROR("Deployment of the amended query {} failed: {}", queryId, exception.what());
            }
            /// The original's query stays Processed after a failed deployment and is never amended again (see the catch below).
            sharedQueryPlan.setStatus(SharedQueryPlanStatus::Failed);
            return result;
        }
        /// As in the original: failed change log entries were recorded again, and the next request amends the query again.
        result.partiallyProcessed = sharedQueryPlan.getStatus() == SharedQueryPlanStatus::PartiallyProcessed;
        sharedQueryPlan.setStatus(result.partiallyProcessed ? SharedQueryPlanStatus::Updated : SharedQueryPlanStatus::Deployed);
        result.success = true;
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Failed to amend the placement of query {}: {}", queryId, exception.what());
        /// As in the original, a query whose amendment failed before its placement completed keeps its status (Migrating or Updated), so
        /// the next request amends it again. After the placement, the original's query stays Processed, which its amendment instance
        /// rejects with every later request; here, it becomes Failed and is not amended again.
        if (sharedQueryPlan.getStatus() == SharedQueryPlanStatus::Processed
            or sharedQueryPlan.getStatus() == SharedQueryPlanStatus::PartiallyProcessed)
        {
            sharedQueryPlan.setStatus(SharedQueryPlanStatus::Failed);
        }
    }
    return result;
}

PlacementAmendmentHandler::PlacementAmendmentHandler(const size_t numberOfThreads) : numberOfThreads(std::max<size_t>(numberOfThreads, 1))
{
}

std::vector<PlacementAmendmentResult> PlacementAmendmentHandler::process(
    const std::vector<DistributedQueryId>& queries, const std::function<PlacementAmendmentResult(const DistributedQueryId&)>& amend) const
{
    std::vector<PlacementAmendmentResult> results(queries.size());
    std::atomic<size_t> next = 0;
    auto worker = [&]
    {
        for (auto index = next++; index < queries.size(); index = next++)
        {
            results.at(index) = amend(queries.at(index));
        }
    };
    {
        std::vector<std::jthread> threads;
        for (size_t thread = 1; thread < std::min(numberOfThreads, queries.size()); ++thread)
        {
            threads.emplace_back(worker);
        }
        worker();
    }
    return results;
}

IncrementalPlacementRequest::IncrementalPlacementRequest(
    QueryManager& queryManager,
    SharedPtr<WorkerCatalog> workerCatalog,
    QueryDecomposer decomposer,
    QueryOptimizerNetworkConfiguration networkConfiguration,
    IncrementalPlacementConfiguration configuration)
    : queryManager(queryManager)
    , workerCatalog(std::move(workerCatalog))
    , decomposer(std::move(decomposer))
    , networkConfiguration(std::move(networkConfiguration))
    , configuration(configuration)
{
}

IncrementalPlacementResult IncrementalPlacementRequest::execute(const std::vector<TopologyChangeEvent>& events)
{
    const auto start = std::chrono::steady_clock::now();
    IncrementalPlacementResult result;

    /// 1. Apply all topology events. Removed nodes are handled like the removal of the links from their upstream nodes.
    std::vector<std::pair<Host, Host>> removedLinks;
    for (const auto& event : events)
    {
        std::visit(
            Overloaded{
                [&](const AddNodeEvent& addNode)
                {
                    if (not workerCatalog->addWorker(addNode.host, addNode.dataAddress, addNode.capacity, addNode.downstream))
                    {
                        throw InvalidTopology("Duplicate worker host '{}'", addNode.host);
                    }
                },
                [&](const RemoveNodeEvent& removeNode)
                {
                    for (const auto& upstream : workerCatalog->getTopology().getUpstreamNodesOf(removeNode.host))
                    {
                        removedLinks.emplace_back(upstream, removeNode.host);
                    }
                    if (not workerCatalog->removeWorker(removeNode.host))
                    {
                        throw UnknownWorker("{}", removeNode.host);
                    }
                },
                [&](const AddLinkEvent& addLink) { workerCatalog->addLink(addLink.upstream, addLink.downstream); },
                [&](const RemoveLinkEvent& removeLink)
                {
                    if (not workerCatalog->removeLink(removeLink.upstream, removeLink.downstream))
                    {
                        throw InvalidTopology("The link [{}] -> [{}] does not exist", removeLink.upstream, removeLink.downstream);
                    }
                    removedLinks.emplace_back(removeLink.upstream, removeLink.downstream);
                }},
            event);
    }

    /// 2. Identify the affected queries and the operators to re-place.
    auto& globalExecutionPlan = queryManager.getGlobalExecutionPlan();
    auto& sharedQueryPlans = queryManager.getSharedQueryPlans();
    const auto topology = workerCatalog->getTopology();
    std::set<DistributedQueryId> affectedQueries;
    for (const auto& [upstream, downstream] : removedLinks)
    {
        const auto upstreamQueries = globalExecutionPlan.getPlacedQueryIds(upstream);
        const auto downstreamQueries = globalExecutionPlan.getPlacedQueryIds(downstream);
        std::vector<DistributedQueryId> impactedQueries;
        std::ranges::set_intersection(upstreamQueries, downstreamQueries, std::back_inserter(impactedQueries));
        for (const auto& queryId : impactedQueries)
        {
            const auto it = sharedQueryPlans.find(queryId);
            if (it == sharedQueryPlans.end())
            {
                continue;
            }
            const auto pinned = findUpstreamAndDownstreamPinnedOperators(it->second, globalExecutionPlan, upstream, downstream, topology);
            if (pinned.upstreamOperators.empty())
            {
                continue;
            }
            it->second.setStatus(SharedQueryPlanStatus::Migrating);
            it->second.performReOperatorPlacement(pinned.upstreamOperators, pinned.downstreamOperators);
            affectedQueries.insert(queryId);
        }
    }
    NES_INFO("Topology change affects {} queries", affectedQueries.size());

    /// As in the original (`getSharedQueryPlansToDeploy`), every query that is not deployed is amended, e.g., queries that an earlier
    /// amendment processed only partially (Updated) or that failed before their placement completed (Migrating). The original also returns
    /// stopped and failed queries, for its stop and fail requests; the frontend stops queries with their own statements, and a Failed query
    /// is one whose deployment failed, which the original does not amend again either (see PlacementAmendmentInstance::execute).
    std::set<DistributedQueryId> queriesToAmend = affectedQueries;
    for (const auto& [queryId, sharedQueryPlan] : sharedQueryPlans)
    {
        const auto status = sharedQueryPlan.getStatus();
        if (status != SharedQueryPlanStatus::Deployed and status != SharedQueryPlanStatus::Failed
            and status != SharedQueryPlanStatus::Stopped and queriesToAmend.insert(queryId).second)
        {
            ++result.numberOfPendingQueries;
        }
    }

    /// 3. Amend and deploy every affected query, in parallel with the configured number of threads.
    const auto amendmentStart = std::chrono::steady_clock::now();
    result.amendments = PlacementAmendmentHandler(configuration.placementAmendmentThreadCount)
                            .process(
                                std::vector(queriesToAmend.begin(), queriesToAmend.end()),
                                [&](const DistributedQueryId& queryId)
                                {
                                    return PlacementAmendmentInstance(
                                               queryManager, copyPtr(workerCatalog), decomposer, networkConfiguration, configuration)
                                        .execute(queryId);
                                });
    result.amendmentTime = elapsedSince(amendmentStart);
    result.numberOfFailedAmendments = std::ranges::count_if(result.amendments, [](const auto& amendment) { return not amendment.success; });
    result.numberOfPartialAmendments
        = std::ranges::count_if(result.amendments, [](const auto& amendment) { return amendment.partiallyProcessed; });
    result.numberOfAffectedQueries = affectedQueries.size();
    result.processingTime = elapsedSince(start);
    NES_INFO(
        "Processed topology change: {} affected queries, {} pending queries, {} failed amendments, {} partial amendments, {}",
        result.numberOfAffectedQueries,
        result.numberOfPendingQueries,
        result.numberOfFailedAmendments,
        result.numberOfPartialAmendments,
        result.processingTime);
    return result;
}

}
