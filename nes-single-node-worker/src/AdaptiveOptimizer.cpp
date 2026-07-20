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
#include <AdaptiveOptimizer.hpp>

#include <Rules/Dynamic/JoinTypeDynamicRule.hpp>
#include <Rules/Dynamic/StrawmanDynamicRule.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <fmt/format.h>
#include <LocalStatisticsCatalog.hpp>

namespace NES
{


AdaptiveOptimizer::AdaptiveOptimizer()
{
    ruleSequence.emplace_back(JoinTypeDynamicRule{});

    ruleSequence.emplace_back(DecideFieldOrder{});
    ruleSequence.emplace_back(DecideFieldMappings{});
    ruleSequence.emplace_back(DecideMemoryLayoutRule{});
};

std::expected<LogicalPlan, Exception> AdaptiveOptimizer::reoptimize(LogicalPlan plan, const LocalStatisticsCatalog& planStatistics)
{
    NES_DEBUG("ORIGINAL PLAN: {}", explain(plan, ExplainVerbosity::Debug));

    for (const auto& rule : ruleSequence)
    {
        if (auto dynamic = rule.tryGetAs<DynamicRule<LogicalPlan>>())
        {
            plan = (*dynamic.value())->apply(plan, planStatistics);
        }
        else
        {
            plan = rule.apply(plan);
        }
    }

    NES_DEBUG("OPTIMIZED PLAN: {}", explain(plan, ExplainVerbosity::Debug));
    return {plan};
}


}
