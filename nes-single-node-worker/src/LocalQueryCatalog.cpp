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

#include <LocalQueryCatalog.hpp>

namespace NES
{


LocalQueryCatalog::LocalQueryCatalog()
{
    localQueryCatalog.clear();
}

void LocalQueryCatalog::addPlan(QueryId id, LogicalPlan plan)
{
    localQueryCatalog.emplace(std::move(id), std::move(plan));
}

void LocalQueryCatalog::replacePlan(QueryId id, LogicalPlan plan)
{
    localQueryCatalog.at(id) = std::move(plan);
}

void LocalQueryCatalog::removePlan(const QueryId& id)
{
    localQueryCatalog.erase(id);
}

LogicalPlan LocalQueryCatalog::getPlan(const QueryId& id)
{
    return localQueryCatalog.at(id);
}

std::unordered_set<QueryId> LocalQueryCatalog::getAllQueryIds()
{
    return std::views::keys(localQueryCatalog) | std::ranges::to<std::unordered_set<QueryId>>();
}

}
