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

#include <Plans/LogicalPlan.hpp>

namespace NES
{
class LocalQueryCatalog
{
public:
    LocalQueryCatalog() = default;

    std::expected<LogicalPlan, Exception> getLogicalPlan(QueryId id) const;
    std::expected<LogicalPlan, Exception> getLogicalPlan(LocalQueryId localId) const;
    std::expected<LogicalPlan, Exception> getLogicalPlan(std::string localIdPrefix) const;


    void addQuery(QueryId id, LogicalPlan logicalPlan);
    std::expected<void, Exception> updateQuery(QueryId id, LogicalPlan logicalPlan);
    void removeQuery(QueryId id);
    std::vector<QueryId> getAllQueryIds() const;


private:
    std::unordered_map<QueryId, LogicalPlan> data;
};
}
