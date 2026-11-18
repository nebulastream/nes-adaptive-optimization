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

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <DrainRequest.hpp>
#include <QueryId.hpp>
#include <SinkDrain.hpp>

namespace NES
{

/// A local query of a distributed query: the worker it runs on and its query id.
struct LocalQueryRef
{
    Host host;
    QueryId queryId;

    bool operator==(const LocalQueryRef&) const = default;
};

}

template <>
struct std::hash<NES::LocalQueryRef>
{
    size_t operator()(const NES::LocalQueryRef& ref) const noexcept
    {
        return std::hash<NES::Host>{}(ref.host) ^ (std::hash<NES::QueryId>{}(ref.queryId) << 1U);
    }
};

namespace NES
{

/// The requests a drain consists of.
struct DrainPlan
{
    /// The request of every local query which takes part in the drain. They are sent first, and all of them have to be accepted
    /// before the drain starts.
    std::unordered_map<LocalQueryRef, DrainRequest> requests;
    /// The local queries where the drain starts, i.e., which stop their local sources.
    std::vector<LocalQueryRef> starts;
};

/// Works out the role of every local query in the drain of `drained`, from the network channels of the local plans. `localQueries`
/// holds the local queries of the drained distributed query with the plans they were started with. Data downstream of a drained
/// query which detaches from a running query may only pass stateless operators until the running query ends (see `planRedirect`).
/// Throws `InvalidDrain` if the drain does not fit the local queries.
[[nodiscard]] DrainPlan planDrain(
    DrainId drainId, const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries, const std::unordered_set<LocalQueryRef>& drained);

/// Returns where the network sink with the channel id `sinkChannel` of the local query `sink` is redirected to, so that it sends to
/// the network source with the channel id `targetChannel` of the local query `target`. `localQueries` holds every known local query
/// with its plan. The operators downstream of the target source must be stateless, as the redirected data does not continue the
/// sequence numbers the target has seen. Throws `InvalidDrain` otherwise.
[[nodiscard]] RedirectTarget planRedirect(
    const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries,
    const LocalQueryRef& sink,
    const std::string& sinkChannel,
    const LocalQueryRef& target,
    const std::string& targetChannel);

}
