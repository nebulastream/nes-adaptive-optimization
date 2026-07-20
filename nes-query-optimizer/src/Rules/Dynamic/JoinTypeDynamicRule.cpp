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

#include <Rules/Dynamic/JoinTypeDynamicRule.hpp>

#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>

namespace NES
{

namespace
{


LogicalOperator
replaceJoin(const QueryId& queryId, TypedLogicalOperator<JoinLogicalOperator> join, const LocalStatisticsCatalog& statistics)
{
    INVARIANT(join.getChildren().size() == 2, "A join must have exactly two children");
    auto left = join.getChildren().at(0);
    auto right = join.getChildren().at(1);

    auto leftStatsOpt = statistics.getOperatorStatistics(queryId, left.getId());
    auto rightStatsOpt = statistics.getOperatorStatistics(queryId, right.getId());


    if (leftStatsOpt && rightStatsOpt)
    {
        auto traitSet = join.getTraitSet();
        const auto joinTrait = traitSet.tryGet<JoinImplementationTypeTrait>();
        INVARIANT(joinTrait.has_value(), "running join must have value");
        const auto currentJoinType = joinTrait.value()->implementationType;
        const auto leftStats = leftStatsOpt.value();
        const auto rightStats = rightStatsOpt.value();


        if (leftStats < rightStats)
        {
            if (leftStats < JoinTypeDynamicRule::HASH_JOIN_CUTOFF_VALUE && currentJoinType != JoinImplementation::NESTED_LOOP_JOIN
                && join->getJoinType() == JoinLogicalOperator::JoinType::INNER_JOIN)
            {
                traitSet.insertOrReplace(JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
                return join.withTraitSet(traitSet).withChildren({right, left}).withInferredSchema();
            }
            if (leftStats >= JoinTypeDynamicRule::HASH_JOIN_CUTOFF_VALUE && currentJoinType != JoinImplementation::HASH_JOIN
                && DecideJoinTypesRule::canUseHashJoin(join->getJoinFunction()))
            {
                traitSet.insertOrReplace(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
                return join.withTraitSet(traitSet).withInferredSchema();
            }
        }
        else
        {
            if (rightStats < JoinTypeDynamicRule::HASH_JOIN_CUTOFF_VALUE && currentJoinType != JoinImplementation::NESTED_LOOP_JOIN)
            {
                traitSet.insertOrReplace(JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
                return join.withTraitSet(traitSet).withInferredSchema();
            }
            if (rightStats >= JoinTypeDynamicRule::HASH_JOIN_CUTOFF_VALUE && currentJoinType != JoinImplementation::HASH_JOIN
                && DecideJoinTypesRule::canUseHashJoin(join->getJoinFunction()))
            {
                traitSet.insertOrReplace(JoinImplementationTypeTrait{JoinImplementation::HASH_JOIN});
                return join.withTraitSet(traitSet).withInferredSchema();
            }
        }
    }

    return join;
}

LogicalOperator applyRecursive(const QueryId& queryId, LogicalOperator op, const LocalStatisticsCatalog& statistics)
{
    if (auto join = op.tryGetAs<JoinLogicalOperator>())
    {
        op = replaceJoin(queryId, join.value(), statistics);
    }

    std::vector<LogicalOperator> newChildren = {};
    for (const auto& child : op.getChildren())
    {
        newChildren.push_back(applyRecursive(queryId, child, statistics));
    }
    return op.withChildren(newChildren);
}

}

LogicalPlan JoinTypeDynamicRule::apply(LogicalPlan queryPlan, const LocalStatisticsCatalog& statistics) const
{
    INVARIANT(queryPlan.getRootOperators().size() == 1, "Only supports plans with single root operator");
    auto newRoot = applyRecursive(queryPlan.getQueryId(), queryPlan.getRootOperators().at(0), statistics).withInferredSchema();
    return queryPlan.withRootOperators({newRoot});
}

const std::type_info& JoinTypeDynamicRule::getType()
{
    return typeid(JoinTypeDynamicRule);
}

std::string_view JoinTypeDynamicRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> JoinTypeDynamicRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> JoinTypeDynamicRule::requiredBy() const
{
    return {};
}

bool JoinTypeDynamicRule::operator==(const JoinTypeDynamicRule&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan JoinTypeDynamicRule::apply(LogicalPlan /*tmp*/) const
{
    throw NotImplemented("use ::apply(LogicalPlan, LocalStatisticsCatalog)");
}


}
