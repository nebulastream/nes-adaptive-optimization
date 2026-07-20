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

#include <LocalStatisticsCatalog.hpp>

namespace  NES
{

LocalStatisticsCatalog::LocalStatisticsCatalog()
{
    statistics.clear();
}


void LocalStatisticsCatalog::setOperatorStatistics(QueryId queryId, OperatorId operatorId, int64_t value)
{
    if (!statistics.contains(queryId))
    {
        statistics.emplace(queryId, std::unordered_map<OperatorId, int64_t>());
        NES_DEBUG("Set local statistics: QueryId: {}, OperatorId: {}, Value: {}", queryId, operatorId, value);
    }
    statistics[queryId][operatorId] = value;
}

std::optional<int64_t> LocalStatisticsCatalog::getOperatorStatistics(QueryId queryId, OperatorId operatorId)
{
    if (auto queryIter = statistics.find(queryId); queryIter != statistics.end())
    {
        auto queryStatistics = queryIter->second;
        if (auto operatorIter = queryStatistics.find(operatorId); operatorIter != queryStatistics.end())
        {
            return operatorIter->second;
        }
    }

    return std::nullopt;
}

}