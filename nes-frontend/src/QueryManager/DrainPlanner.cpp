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

#include <QueryManager/DrainPlanner.hpp>

#include <any>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/NetworkSink.hpp>
#include <Sources/NetworkSource.hpp>
#include <DrainRequest.hpp>
#include <ErrorHandling.hpp>
#include <SinkDrain.hpp>

namespace NES
{
namespace
{
std::optional<NetworkSourceConfig> getNetworkSource(const LogicalOperator& op)
{
    if (const auto source = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        const auto descriptor = source.value()->getSourceDescriptor();
        if (const auto* config = std::any_cast<NetworkSourceConfig>(&descriptor.getPluginData().getUnderlying()))
        {
            return *config;
        }
    }
    return std::nullopt;
}

std::optional<NetworkSinkConfig> getNetworkSink(const LogicalOperator& op)
{
    if (const auto sink = op.tryGetAs<SinkLogicalOperator>())
    {
        if (const auto descriptor = sink.value()->getSinkDescriptor())
        {
            if (const auto* config = std::any_cast<NetworkSinkConfig>(&descriptor->getPluginData().getUnderlying()))
            {
                return *config;
            }
        }
    }
    return std::nullopt;
}

/// The network channels and the number of sources of a local plan.
struct PlanChannels
{
    /// The bind address of each network source, keyed by its channel id.
    std::unordered_map<std::string, std::string> networkSources;
    std::vector<std::string> networkSinks;
    size_t numberOfSources = 0;
};

PlanChannels collectChannels(const LogicalPlan& plan)
{
    PlanChannels channels;
    std::unordered_set<OperatorId> visited;
    for (const auto& root : plan.getRootOperators())
    {
        for (const auto& op : BFSRange(root))
        {
            if (!visited.insert(op.getId()).second)
            {
                continue;
            }
            if (op.tryGetAs<SourceDescriptorLogicalOperator>())
            {
                ++channels.numberOfSources;
            }
            if (const auto source = getNetworkSource(op))
            {
                channels.networkSources.emplace(source->channel, source->bind);
            }
            if (const auto sink = getNetworkSink(op))
            {
                channels.networkSinks.push_back(sink->channel);
            }
        }
    }
    return channels;
}

/// Who sends and who receives on a network channel. An end is missing if no known local query has it.
struct ChannelEnds
{
    std::optional<LocalQueryRef> sender;
    std::optional<LocalQueryRef> receiver;
};

/// The network channels of a set of local queries.
struct Topology
{
    std::unordered_map<LocalQueryRef, PlanChannels> channels;
    std::unordered_map<std::string, ChannelEnds> ends;

    explicit Topology(const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries)
    {
        for (const auto& [ref, plan] : localQueries)
        {
            auto planChannels = collectChannels(plan);
            for (const auto& channel : planChannels.networkSources | std::views::keys)
            {
                ends[channel].receiver = ref;
            }
            for (const auto& channel : planChannels.networkSinks)
            {
                ends[channel].sender = ref;
            }
            channels.emplace(ref, std::move(planChannels));
        }
    }
};

/// Collects the operators of the subtree of `op` which are downstream of the network source with the channel id `channel`, and
/// returns whether the subtree contains that source.
bool collectDownstreamOf(const LogicalOperator& op, const std::string& channel, std::vector<LogicalOperator>& downstream)
{
    if (const auto source = getNetworkSource(op))
    {
        return source->channel == channel;
    }
    bool containsSource = false;
    for (const auto& child : op.getChildren())
    {
        containsSource = collectDownstreamOf(child, channel, downstream) || containsSource;
    }
    if (containsSource)
    {
        downstream.push_back(op);
    }
    return containsSource;
}

/// Checks that the data which arrives through the network source with the channel id `channel` only passes stateless operators, up to
/// the end of the data flow or a drained local query. Window and join operators expect contiguous sequence numbers per origin, which a
/// detached or redirected channel does not continue.
void checkStatelessDownstream(
    const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries,
    const Topology& topology,
    const std::unordered_set<LocalQueryRef>& drained,
    const std::string& channel)
{
    std::vector<std::string> pending{channel};
    std::unordered_set<std::string> visited;
    while (!pending.empty())
    {
        const auto current = std::move(pending.back());
        pending.pop_back();
        if (!visited.insert(current).second)
        {
            continue;
        }
        const auto ends = topology.ends.find(current);
        if (ends == topology.ends.end() || !ends->second.receiver.has_value())
        {
            throw InvalidDrain("the receiver of the network channel {} is unknown, so its operators cannot be checked", current);
        }
        const auto& receiver = *ends->second.receiver;
        if (drained.contains(receiver))
        {
            continue;
        }
        std::vector<LogicalOperator> downstream;
        for (const auto& root : localQueries.at(receiver).getRootOperators())
        {
            collectDownstreamOf(root, current, downstream);
        }
        for (const auto& op : downstream)
        {
            if (op.tryGetAs<WindowedAggregationLogicalOperator>() || op.tryGetAs<JoinLogicalOperator>())
            {
                throw InvalidDrain(
                    "the operator {} of the local query {} on {} is stateful and downstream of the network channel {}, which does not "
                    "continue its sequence numbers",
                    op.getName(),
                    receiver.queryId,
                    receiver.host,
                    current);
            }
            if (const auto sink = getNetworkSink(op))
            {
                pending.push_back(sink->channel);
            }
        }
    }
}

const ChannelEnds& getEnds(const Topology& topology, const std::string& channel, const LocalQueryRef& ref)
{
    const auto ends = topology.ends.find(channel);
    INVARIANT(ends != topology.ends.end(), "The channel {} of the local query {} is not part of the topology", channel, ref.queryId);
    return ends->second;
}
}

DrainPlan planDrain(
    const DrainId drainId,
    const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries,
    const std::unordered_set<LocalQueryRef>& drained)
{
    PRECONDITION(drainId != INVALID_DRAIN_ID, "A drain requires a valid drain id");
    if (drained.empty())
    {
        throw InvalidDrain("no local query is drained");
    }
    for (const auto& ref : drained)
    {
        if (!localQueries.contains(ref))
        {
            throw InvalidDrain("the local query {} on {} is not part of the distributed query", ref.queryId, ref.host);
        }
    }

    const Topology topology(localQueries);
    DrainPlan plan;
    auto requestOf = [&](const LocalQueryRef& ref) -> DrainRequest&
    {
        auto [request, inserted] = plan.requests.try_emplace(ref);
        if (inserted)
        {
            request->second.drainId = drainId;
        }
        return request->second;
    };

    for (const auto& ref : drained)
    {
        const auto& channels = topology.channels.at(ref);
        auto& request = requestOf(ref);
        request.drainsQuery = true;
        for (const auto& channel : channels.networkSources | std::views::keys)
        {
            const auto& sender = getEnds(topology, channel, ref).sender;
            if (!sender.has_value())
            {
                throw InvalidDrain("the sender of the network channel {} is unknown", channel);
            }
            if (drained.contains(*sender))
            {
                request.markerChannels.push_back(channel);
            }
            else
            {
                /// The upstream query keeps running and buffers until its sink is redirected.
                requestOf(*sender).sinkModes.emplace(channel, SinkDrainMode::BufferOnDetach);
            }
        }
        request.stopLocalSources = request.markerChannels.size() < channels.numberOfSources;
        if (request.stopLocalSources)
        {
            plan.starts.push_back(ref);
        }

        for (const auto& channel : channels.networkSinks)
        {
            const auto& receiver = getEnds(topology, channel, ref).receiver;
            if (!receiver.has_value())
            {
                throw InvalidDrain("the receiver of the network channel {} is unknown", channel);
            }
            if (drained.contains(*receiver))
            {
                request.sinkModes.emplace(channel, SinkDrainMode::ForwardMarker);
            }
            else
            {
                /// The downstream query keeps running and waits for a new sender.
                checkStatelessDownstream(localQueries, topology, drained, channel);
                request.sinkModes.emplace(channel, SinkDrainMode::Detach);
            }
        }
    }
    INVARIANT(!plan.starts.empty(), "The drained local queries form a cycle, which a data flow cannot");
    return plan;
}

RedirectTarget planRedirect(
    const std::unordered_map<LocalQueryRef, LogicalPlan>& localQueries,
    const LocalQueryRef& sink,
    const std::string& sinkChannel,
    const LocalQueryRef& target,
    const std::string& targetChannel)
{
    if (!localQueries.contains(sink))
    {
        throw InvalidDrain("the local query {} on {} is unknown", sink.queryId, sink.host);
    }
    if (!localQueries.contains(target))
    {
        throw InvalidDrain("the local query {} on {} is unknown", target.queryId, target.host);
    }
    const Topology topology(localQueries);
    if (!std::ranges::contains(topology.channels.at(sink).networkSinks, sinkChannel))
    {
        throw InvalidDrain("the local query {} on {} has no network sink with the channel {}", sink.queryId, sink.host, sinkChannel);
    }
    const auto& targetSources = topology.channels.at(target).networkSources;
    const auto targetSource = targetSources.find(targetChannel);
    if (targetSource == targetSources.end())
    {
        throw InvalidDrain(
            "the local query {} on {} has no network source with the channel {}", target.queryId, target.host, targetChannel);
    }
    checkStatelessDownstream(localQueries, topology, {}, targetChannel);
    return RedirectTarget{.connection = targetSource->second, .channel = targetChannel};
}

}
