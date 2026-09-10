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
std::expected<LogicalPlan, Exception> LocalQueryCatalog::getLogicalPlan(QueryId id) const
{
    if (auto itr = this->data.find(id); itr != this->data.cend())
    {
        return itr->second;
    }
    return std::unexpected{QueryNotFound()};
}

std::expected<LogicalPlan, Exception> LocalQueryCatalog::getLogicalPlan(LocalQueryId localId) const
{
    std::vector<QueryId> queryIds;

    for (auto id : getAllQueryIds())
    {
        if (id.getLocalQueryId() == localId)
        {
            queryIds.push_back(id);
        }
    }

    if (queryIds.empty())
    {
        return std::unexpected{QueryNotFound("No query with given local id ({}) found", localId)};
    }

    if (queryIds.size() > 1)
    {
        return std::unexpected{QueryNotFound("Multiple Queries with same localId: {}", fmt::join(queryIds, ","))};
    }

    return getLogicalPlan(queryIds.front());
}

std::expected<LogicalPlan, Exception> LocalQueryCatalog::getLogicalPlan(std::string localIdPrefix) const
{
    std::vector<QueryId> queryIds;

    for (auto id : getAllQueryIds())
    {
        if (id.getLocalQueryId().getRawValue().starts_with(localIdPrefix))
        {
            queryIds.push_back(id);
        }
    }

    if (queryIds.empty())
    {
        return std::unexpected{QueryNotFound("No query with given prefix ({}) found", localIdPrefix)};
    }

    if (queryIds.size() > 1)
    {
        return std::unexpected{QueryNotFound("Multiple queries with same prefix (): {}", localIdPrefix, fmt::join(queryIds, ","))};
    }

    return getLogicalPlan(queryIds.front());
}

void LocalQueryCatalog::addQuery(QueryId id, LogicalPlan logicalPlan)
{
    this->data.emplace(id, logicalPlan);
}

std::expected<void, Exception> LocalQueryCatalog::updateQuery(QueryId id, LogicalPlan logicalPlan)
{
    if (auto itr = this->data.find(id); itr != this->data.end())
    {
        itr->second = logicalPlan;
        return {};
    }
    return std::unexpected{QueryNotFound()};
}

void LocalQueryCatalog::removeQuery(QueryId id)
{
    this->data.erase(id);
}

std::vector<QueryId> LocalQueryCatalog::getAllQueryIds() const
{
    return this->data | std::views::keys | std::ranges::to<std::vector<QueryId>>();
}
}
