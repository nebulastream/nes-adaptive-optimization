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

#include <Rules/RuleManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/Pointers.hpp>
#include <LocalQueryCatalog.hpp>
#include <QueryCompiler.hpp>
#include <Statistics.hpp>
#include <Thread.hpp>

namespace NES
{
class AdaptiveOptimizer
{
public:
    explicit AdaptiveOptimizer(
        SharedPtr<LocalQueryCatalog> localQueryCatalog,
        SharedPtr<QueryCompilation::QueryCompiler> compiler,
        SharedPtr<NodeEngine> nodeEngine);
    void reoptimize();

    static constexpr int REOPTIMIZATION_INTERVAL_MS = 5000;

private:
    SharedPtr<LocalQueryCatalog> localQueryCatalog;
    SharedPtr<QueryCompilation::QueryCompiler> compiler;
    SharedPtr<NodeEngine> nodeEngine;
    std::vector<Rule<LogicalPlan>> ruleSequence;
    Thread thread;

    std::expected<LogicalPlan, Exception> reoptimize(LogicalPlan plan, const PlanStatistics& planStatistics);
    void start(std::stop_token stop_token);
};

}
