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

#include <Rules/DynamicRule.hpp>
#include <LocalStatisticsCatalog.hpp>

namespace NES
{
class JoinTypeDynamicRule : public DynamicRule<LogicalPlan>
{
public:
    static constexpr std::string_view NAME = "JoinTypeDynamicRule";

    static constexpr int64_t HASH_JOIN_CUTOFF_VALUE = 10;

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan, const LocalStatisticsCatalog& statistics) const override;

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;

    bool operator==(const JoinTypeDynamicRule& other) const;
};

static_assert(RuleConcept<JoinTypeDynamicRule, LogicalPlan>);
}
