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

#include <Rules/Dynamic/StrawmanDynamicRule.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <fmt/format.h>
#include <QueryCompiler.hpp>
#include <Thread.hpp>

namespace NES
{

AdaptiveOptimizer::AdaptiveOptimizer(
    SharedPtr<LocalQueryCatalog> localQueryCatalog, SharedPtr<QueryCompilation::QueryCompiler> compiler, SharedPtr<NodeEngine> nodeEngine)
    : localQueryCatalog(std::move(localQueryCatalog)), compiler(std::move(compiler)), nodeEngine(std::move(nodeEngine))
{
    ruleSequence.push_back(StrawmanDynamicRule{});
    ruleSequence.push_back(DecideFieldOrder{});
    ruleSequence.push_back(DecideFieldMappings{});
    ruleSequence.push_back(DecideMemoryLayoutRule{});

    thread = Thread("Adaptive Optimizer", &AdaptiveOptimizer::start, this);
}

void AdaptiveOptimizer::reoptimize()
{
    for (auto id : localQueryCatalog->getAllQueryIds())
    {
        auto plan = localQueryCatalog->getPlan(id);
        PlanStatistics statistics{};
        auto result = reoptimize(plan, statistics);

        if (!result.has_value())
        {
            NES_WARNING("Failed to reoptimize query {}: ", id, result.error())
        }
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(result.value());
        auto compiled = compiler->compileQuery(std::move(request));
        nodeEngine->replaceQueryPlan(std::move(compiled));
        localQueryCatalog->replacePlan(id, result.value());
    }
}

std::expected<LogicalPlan, Exception> AdaptiveOptimizer::reoptimize(LogicalPlan plan, const PlanStatistics& planStatistics)
{
    NES_DEBUG("ORIGINAL PLAN: {}", plan);

    for (auto rule : ruleSequence)
    {
        if (auto dynamic = rule.tryGetAs<DynamicRule<LogicalPlan, PlanStatistics>>())
        {
            plan = (*dynamic.value())->apply(plan, planStatistics);
        }
        else
        {
            plan = rule.apply(plan);
        }
    }

    NES_DEBUG("OPTIMIZED PLAN: {}", plan);
    return {plan};
}

void AdaptiveOptimizer::start(std::stop_token stop_token)
{
    NES_INFO("Started adaptive optimization loop");
    while (!stop_token.stop_requested())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(REOPTIMIZATION_INTERVAL_MS));
        NES_INFO("Trigger adaptive optimization");
        reoptimize();
    }
    NES_INFO("Gracefully stopped adaptive optimizer loop");
}


}
