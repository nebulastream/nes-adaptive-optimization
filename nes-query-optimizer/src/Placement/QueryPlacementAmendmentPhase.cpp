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

#include <Placement/QueryPlacementAmendmentPhase.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Placement/IncrementalPlacementUtils.hpp>
#include <Placement/PlacementAdditionStrategy.hpp>
#include <Placement/PlacementRemovalStrategy.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/StateMigrationTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <ChangeLog.hpp>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <OperatorGraph.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

namespace
{
size_t countJoins(const LogicalPlan& plan)
{
    size_t joins = 0;
    for (const auto& root : plan.getRootOperators())
    {
        for (const auto& op : BFSRange(root))
        {
            joins += op.tryGetAs<JoinLogicalOperator>().has_value() ? 1 : 0;
        }
    }
    return joins;
}

/// Adds the trait to the join of the plan. Shortcut (documented): the old and the new plan host exactly one join each, so the plan's
/// only state migration channel belongs to its join. Otherwise the change log entry fails and the query is re-placed holistically.
LogicalPlan withStateMigrationTrait(
    SharedQueryPlan& sharedQueryPlan, const LogicalPlan& plan, const StateMigrationTrait& trait, const size_t joinsInOldPlan)
{
    if (joinsInOldPlan != 1 or countJoins(plan) != 1)
    {
        sharedQueryPlan.requireHolisticReplacement();
        throw PlacementFailure("State migration supports one join per decomposed query plan");
    }
    std::function<LogicalOperator(const LogicalOperator&)> rewrite = [&](const LogicalOperator& op) -> LogicalOperator
    {
        if (op.tryGetAs<JoinLogicalOperator>().has_value())
        {
            auto traitSet = op.getTraitSet();
            traitSet.insert(trait);
            return op.withTraitSet(std::move(traitSet));
        }
        const auto children = op.getChildren();
        const auto containsJoin = std::ranges::any_of(
            children,
            [](const auto& child)
            {
                return std::ranges::any_of(
                    BFSRange(child), [](const auto& node) { return node.template tryGetAs<JoinLogicalOperator>().has_value(); });
            });
        if (not containsJoin)
        {
            return op;
        }
        return op.withChildren(children | std::views::transform(rewrite) | std::ranges::to<std::vector>());
    };
    return plan.withRootOperators(plan.getRootOperators() | std::views::transform(rewrite) | std::ranges::to<std::vector>());
}
}

QueryPlacementAmendmentPhase::QueryPlacementAmendmentPhase(
    GlobalExecutionPlan& globalExecutionPlan,
    SharedPtr<WorkerCatalog> workerCatalog,
    const QueryDecomposer& decomposer,
    QueryOptimizerNetworkConfiguration networkConfiguration,
    const bool enableIncrementalPlacement,
    const PlacementAmendmentMode mode)
    : globalExecutionPlan(globalExecutionPlan)
    , workerCatalog(std::move(workerCatalog))
    , decomposer(decomposer)
    , networkConfiguration(std::move(networkConfiguration))
    , enableIncrementalPlacement(enableIncrementalPlacement)
    , mode(mode)
{
}

DeploymentUnit QueryPlacementAmendmentPhase::execute(SharedQueryPlan& sharedQueryPlan)
{
    return enableIncrementalPlacement ? executeIncrementally(sharedQueryPlan) : executeHolistically(sharedQueryPlan);
}

void QueryPlacementAmendmentPhase::computeReconfigurationMarkerDeploymentUnit(
    const SharedQueryPlan& sharedQueryPlan, const ChangeLogEntry& entry, std::set<ReconfigurationMarkerUnit>& units) const
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    const auto topology = workerCatalog->getTopology();
    for (const auto& upstreamOperator : entry.upstreamOperators)
    {
        const auto state = sharedQueryPlan.getOperatorState(upstreamOperator);
        if (state != OperatorState::Placed and state != OperatorState::ToBeRemoved)
        {
            continue;
        }
        /// Cond-1: the plan hosting the pinned upstream operator.
        const auto upstreamPlan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, upstreamOperator);
        INVARIANT(upstreamPlan.has_value(), "Placed operator {} of query {} is not hosted by any plan", upstreamOperator, queryId);
        units.insert({.worker = upstreamPlan->worker, .plan = upstreamPlan->id});

        /// Cond-2: the plan after a removed link on the way to a downstream operator on another worker.
        for (const auto& downstreamOperator : graph.getDownstreamOperators(upstreamOperator))
        {
            const auto downstreamState = sharedQueryPlan.getOperatorState(downstreamOperator);
            if (downstreamState != OperatorState::Placed and downstreamState != OperatorState::ToBeRemoved
                and downstreamState != OperatorState::ToBeReplaced)
            {
                continue;
            }
            if (sharedQueryPlan.getPinnedWorker(downstreamOperator) == upstreamPlan->worker)
            {
                continue;
            }
            for (const auto& hop : globalExecutionPlan.getChannelChain(queryId, upstreamOperator, downstreamOperator))
            {
                if (not topology.hasLink(hop.channel.upstreamWorker, hop.channel.downstreamWorker))
                {
                    units.insert({.worker = hop.channel.downstreamWorker, .plan = hop.downstreamPlan});
                }
            }
        }
    }
}

namespace
{
/// Deviation: the original removes the re-placed operators from a plan and redeploys the rest of the plan. The current query engine cannot
/// change a running plan, so if a plan hosts operators to be re-placed and operators that stay, the operators that stay are re-placed as
/// well, so the plan is drained as a whole. The pinned operators are adjusted to the extended set: the operators adjacent to re-placed
/// operators are pinned.
ChangeLogEntry
expandToWholePlans(SharedQueryPlan& sharedQueryPlan, const GlobalExecutionPlan& globalExecutionPlan, const ChangeLogEntry& entry)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    auto isReplaced = [&](const OperatorId op)
    {
        const auto state = sharedQueryPlan.getOperatorState(op);
        return state == OperatorState::ToBeReplaced or state == OperatorState::ToBeRemoved;
    };

    std::set<OperatorId> replaced;
    std::ranges::copy_if(entry.poSetOfSubQueryPlan, std::inserter(replaced, replaced.end()), isReplaced);
    bool expanded = false;
    for (bool changed = true; changed;)
    {
        changed = false;
        for (const auto& operatorId : std::set(replaced))
        {
            const auto plan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId);
            if (not plan.has_value())
            {
                continue;
            }
            for (const auto& hosted : plan->hostedOperators)
            {
                if (replaced.insert(hosted).second)
                {
                    NES_DEBUG("Operator {} of query {} is re-placed, as plan {} hosts re-placed operators", hosted, queryId, plan->id);
                    sharedQueryPlan.setOperatorState(hosted, OperatorState::ToBeReplaced);
                    changed = true;
                    expanded = true;
                }
            }
        }
    }
    if (not expanded)
    {
        return entry;
    }

    std::set<OperatorId> upstreamOperators;
    std::set<OperatorId> downstreamOperators;
    std::ranges::copy_if(
        entry.upstreamOperators,
        std::inserter(upstreamOperators, upstreamOperators.end()),
        [&](auto op) { return not replaced.contains(op); });
    std::ranges::copy_if(
        entry.downstreamOperators,
        std::inserter(downstreamOperators, downstreamOperators.end()),
        [&](auto op) { return not replaced.contains(op); });
    for (const auto& operatorId : replaced)
    {
        for (const auto& child : graph.getUpstreamOperators(operatorId))
        {
            if (not replaced.contains(child))
            {
                upstreamOperators.insert(child);
            }
        }
        for (const auto& parent : graph.getDownstreamOperators(operatorId))
        {
            if (not replaced.contains(parent))
            {
                downstreamOperators.insert(parent);
            }
        }
    }
    return ChangeLogEntry::create(std::move(upstreamOperators), std::move(downstreamOperators), graph);
}
}

DeploymentUnit QueryPlacementAmendmentPhase::executeIncrementally(SharedQueryPlan& sharedQueryPlan)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto now = sharedQueryPlan.getCurrentChangeLogTimestamp();
    const auto version = globalExecutionPlan.getNextVersion();

    DeploymentUnit deploymentUnit;
    std::map<DecomposedQueryId, DeploymentContext> deploymentContexts;
    std::vector<ChangeLogEntry> failedChangeLogEntries;
    for (const auto& compactedEntry : sharedQueryPlan.getChangeLogEntries(now) | std::views::values)
    {
        const auto entry = expandToWholePlans(sharedQueryPlan, globalExecutionPlan, compactedEntry);
        try
        {
            /// P0: the plans at which the reconfiguration marker is injected.
            computeReconfigurationMarkerDeploymentUnit(sharedQueryPlan, entry, deploymentUnit.reconfigurationMarkerUnits);

            /// Stateful operators that possibly have to be migrated, with their current worker and plan.
            /// Shortcut (same as the original): only joins are considered stateful.
            std::map<OperatorId, DecomposedQueryPlan> statefulOperators;
            for (const auto& operatorId : entry.poSetOfSubQueryPlan)
            {
                if (sharedQueryPlan.getGraph().getOperator(operatorId).tryGetAs<JoinLogicalOperator>().has_value())
                {
                    if (const auto plan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId))
                    {
                        statefulOperators.emplace(operatorId, *plan);
                    }
                }
            }

            std::set<OperatorId> upstreamOperators;
            std::set<OperatorId> downstreamOperators;
            std::ranges::copy_if(
                entry.upstreamOperators,
                std::inserter(upstreamOperators, upstreamOperators.end()),
                [&](auto op) { return sharedQueryPlan.getOperatorState(op) != OperatorState::Removed; });
            std::ranges::copy_if(
                entry.downstreamOperators,
                std::inserter(downstreamOperators, downstreamOperators.end()),
                [&](auto op) { return sharedQueryPlan.getOperatorState(op) != OperatorState::Removed; });

            /// P1: placement removal, P2: placement addition.
            const auto removal = PlacementRemovalStrategy(globalExecutionPlan, *workerCatalog, mode)
                                     .updateGlobalExecutionPlan(sharedQueryPlan, upstreamOperators, downstreamOperators, version);
            for (const auto& [planId, context] : removal.deploymentContexts)
            {
                deploymentContexts.insert_or_assign(planId, context);
            }
            const auto addition
                = PlacementAdditionStrategy(globalExecutionPlan, copyPtr(workerCatalog), decomposer, networkConfiguration, mode)
                      .updateGlobalExecutionPlan(sharedQueryPlan, upstreamOperators, downstreamOperators, version, removal);
            for (const auto& [planId, context] : addition.deploymentContexts)
            {
                deploymentContexts.insert_or_assign(planId, context);
            }
            /// The addition connected all edges of the entry, including the ones whose chains a failed earlier processing removed.
            sharedQueryPlan.forgetRemovedHops(getOperatorsToProcess(sharedQueryPlan, upstreamOperators, downstreamOperators));

            /// Stateful operators that moved hand their state over: their old plan is updated and drained.
            for (const auto& [operatorId, oldPlan] : statefulOperators)
            {
                const auto newWorker = sharedQueryPlan.getPinnedWorker(operatorId);
                if (not newWorker.has_value() or *newWorker == oldPlan.worker)
                {
                    continue;
                }
                /// The old plan still hosts the operator until it is drained, so look for the new plan on the new worker.
                const auto plansOnNewWorker = globalExecutionPlan.getDecomposedQueryPlans(queryId, *newWorker);
                const auto newPlan
                    = std::ranges::find_if(plansOnNewWorker, [&](const auto& plan) { return plan.hostedOperators.contains(operatorId); });
                INVARIANT(
                    newPlan != plansOnNewWorker.end(),
                    "Re-placed operator {} of query {} is not hosted by any plan on {}",
                    operatorId,
                    queryId,
                    *newWorker);
                const auto implementation
                    = getTrait<JoinImplementationTypeTrait>(sharedQueryPlan.getGraph().getOperator(operatorId).getTraitSet());
                if (not implementation.has_value() or implementation.value()->implementationType != JoinImplementation::NESTED_LOOP_JOIN)
                {
                    /// Shortcut (same as the original): only the state of nested loop joins is migrated.
                    NES_WARNING(
                        "Join {} of query {} moves to {}, but only nested loop joins migrate their state. Its state is lost",
                        operatorId,
                        queryId,
                        *newWorker);
                    continue;
                }
                const auto windowType = sharedQueryPlan.getGraph().getOperator(operatorId).getAs<JoinLogicalOperator>()->getWindowType();
                if (windowType.getSize() != windowType.getSlide())
                {
                    /// Shortcut (documented): a slice of a sliding window may belong to triggered and pending windows.
                    sharedQueryPlan.requireHolisticReplacement();
                    throw PlacementFailure("State migration supports joins over tumbling windows only");
                }
                deploymentUnit.stateMigrations.emplace_back(
                    StateMigration{
                        .operatorId = operatorId,
                        .oldWorker = oldPlan.worker,
                        .oldPlan = oldPlan.id,
                        .newWorker = *newWorker,
                        .newPlan = newPlan->id});
                /// The old join sends its state over a dedicated channel to the new join (see StateMigrationTransfer).
                const auto oldWorker = workerCatalog->getWorker(oldPlan.worker);
                const auto newWorkerInfo = workerCatalog->getWorker(*newWorker);
                INVARIANT(oldWorker.has_value() and newWorkerInfo.has_value(), "Workers of a state migration must be known");
                const StateMigrationChannel channel{
                    .channel = UUIDToString(generateUUID()),
                    .senderDataEndpoint = oldWorker->dataAddress,
                    .receiverDataEndpoint = newWorkerInfo->dataAddress};
                auto& newContext = deploymentContexts.at(newPlan->id);
                newContext.plan.plan = withStateMigrationTrait(
                    sharedQueryPlan,
                    newContext.plan.plan,
                    StateMigrationTrait{channel.channel, channel.receiverDataEndpoint},
                    countJoins(oldPlan.plan));
                globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, newContext.plan);

                auto& context = deploymentContexts.at(oldPlan.id);
                context.plan.state = DecomposedQueryPlanState::MarkedForUpdateAndDrain;
                context.stateMigrationChannels.emplace_back(channel);
                globalExecutionPlan.setState(queryId, oldPlan.id, DecomposedQueryPlanState::MarkedForUpdateAndDrain);
                NES_DEBUG("Stateful operator {} of query {} moves from {} to {}", operatorId, queryId, oldPlan.worker, *newWorker);
            }
        }
        catch (const Exception& exception)
        {
            NES_ERROR(
                "Failed to process a change log entry of query {}. The entry is recorded again for further processing: {}",
                queryId,
                exception.what());
            sharedQueryPlan.setStatus(SharedQueryPlanStatus::PartiallyProcessed);
            /// As in the original, the entry is recorded as it was processed. Here, that is the entry extended to whole plans: operators
            /// it re-places lost their workers, so they cannot be pinned when the entry is processed again.
            failedChangeLogEntries.emplace_back(entry);
        }
    }
    if (not failedChangeLogEntries.empty())
    {
        sharedQueryPlan.recordFailedChangeLogEntries(failedChangeLogEntries);
    }
    sharedQueryPlan.updateProcessedChangeLogTimestamp(now);
    if (sharedQueryPlan.getStatus() != SharedQueryPlanStatus::PartiallyProcessed
        and sharedQueryPlan.getStatus() != SharedQueryPlanStatus::Stopped)
    {
        sharedQueryPlan.setStatus(SharedQueryPlanStatus::Processed);
    }
    for (auto& context : deploymentContexts | std::views::values)
    {
        deploymentUnit.deploymentAdditionContexts.emplace_back(std::move(context));
    }
    return deploymentUnit;
}

DeploymentUnit QueryPlacementAmendmentPhase::executeHolistically(SharedQueryPlan& sharedQueryPlan)
{
    /// As in the original, the holistic baseline re-places every placed operator, pinned between the sources and the sinks, with the same
    /// removal and addition strategies, so it synchronizes optimistically or pessimistically like the incremental placement.
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    const auto now = sharedQueryPlan.getCurrentChangeLogTimestamp();
    const auto version = globalExecutionPlan.getNextVersion();
    for (const auto& operatorId : graph.getAllOperators())
    {
        if (sharedQueryPlan.getOperatorState(operatorId) == OperatorState::Placed)
        {
            sharedQueryPlan.setOperatorState(operatorId, OperatorState::ToBeReplaced);
        }
    }
    const auto leaves = graph.getLeafOperators();
    const auto roots = graph.getRootOperators();
    const std::set<OperatorId> pinnedUpstreamOperators(leaves.begin(), leaves.end());
    const std::set<OperatorId> pinnedDownstreamOperators(roots.begin(), roots.end());

    DeploymentUnit deploymentUnit;
    try
    {
        /// 1. Remove the placement. Every plan of the query is stopped, including plans that a failed incremental amendment left behind.
        const auto removal = PlacementRemovalStrategy(globalExecutionPlan, *workerCatalog, mode)
                                 .updateGlobalExecutionPlan(sharedQueryPlan, pinnedUpstreamOperators, pinnedDownstreamOperators, version);
        for (auto plan : globalExecutionPlan.getDecomposedQueryPlans(queryId))
        {
            const auto removed = removal.deploymentContexts.find(plan.id);
            const auto oldVersion = removed != removal.deploymentContexts.end() ? removed->second.oldVersion : plan.version;
            plan.state = DecomposedQueryPlanState::MarkedForMigration;
            deploymentUnit.deploymentRemovalContexts.emplace_back(
                DeploymentContext{
                    .plan = plan, .oldVersion = oldVersion, .sinkUpdates = {}, .detachedChannels = {}, .stateMigrationChannels = {}});
            globalExecutionPlan.removeDecomposedQueryPlan(queryId, plan.id);
        }
        for (const auto& placed : globalExecutionPlan.getChannels(queryId))
        {
            globalExecutionPlan.removeChannel(queryId, placed.channel.id);
        }

        /// 2. Place all operators again; the new plans are deployed from scratch.
        const auto addition
            = PlacementAdditionStrategy(globalExecutionPlan, copyPtr(workerCatalog), decomposer, networkConfiguration, mode)
                  .updateGlobalExecutionPlan(
                      sharedQueryPlan, pinnedUpstreamOperators, pinnedDownstreamOperators, version, PlacementRemovalResult{});
        for (const auto& context : addition.deploymentContexts | std::views::values)
        {
            deploymentUnit.deploymentAdditionContexts.emplace_back(context);
        }
        /// All plans and channels were replaced, so no removed hop is left to reconnect.
        const auto allOperators = graph.getAllOperators();
        sharedQueryPlan.forgetRemovedHops({allOperators.begin(), allOperators.end()});
    }
    catch (const Exception& exception)
    {
        /// As in the original: the failure is not rolled back, the query becomes PartiallyProcessed and is amended again later.
        NES_ERROR("Failed to re-place query {} holistically: {}", queryId, exception.what());
        sharedQueryPlan.setStatus(SharedQueryPlanStatus::PartiallyProcessed);
    }
    sharedQueryPlan.updateProcessedChangeLogTimestamp(now);
    if (sharedQueryPlan.getStatus() != SharedQueryPlanStatus::PartiallyProcessed
        and sharedQueryPlan.getStatus() != SharedQueryPlanStatus::Stopped)
    {
        sharedQueryPlan.setStatus(SharedQueryPlanStatus::Processed);
    }
    return deploymentUnit;
}

}
