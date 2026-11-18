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


#include <QueryDrainTracker.hpp>

#include <memory>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <DrainState.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{

namespace
{
void validate(const QueryId& queryId, const QueryChannels& channels, const DrainRequest& request)
{
    if (request.drainId == INVALID_DRAIN_ID)
    {
        throw InvalidDrainRequest("the drain of query {} has an invalid drain id", queryId);
    }
    for (const auto& channel : request.markerChannels)
    {
        if (!channels.networkSources.contains(channel))
        {
            throw InvalidDrainRequest("query {} has no network source with channel {}", queryId, channel);
        }
    }
    const std::unordered_set<std::string> uniqueMarkerChannels(request.markerChannels.begin(), request.markerChannels.end());
    if (uniqueMarkerChannels.size() != request.markerChannels.size())
    {
        throw InvalidDrainRequest("the drain of query {} lists a marker channel more than once", queryId);
    }
    for (const auto& channel : request.sinkModes | std::views::keys)
    {
        if (!channels.networkSinks.contains(channel))
        {
            throw InvalidDrainRequest("query {} has no network sink with channel {}", queryId, channel);
        }
    }

    if (!request.drainsQuery)
    {
        /// The query only takes part in the drain by buffering until its sinks are redirected.
        if (request.stopLocalSources || !request.markerChannels.empty())
        {
            throw InvalidDrainRequest("query {} is not drained, so none of its sources can be drained", queryId);
        }
        if (request.sinkModes.empty())
        {
            throw InvalidDrainRequest("query {} neither is drained nor has a sink which takes part in the drain", queryId);
        }
        for (const auto& [channel, mode] : request.sinkModes)
        {
            if (mode != SinkDrainMode::BufferOnDetach)
            {
                throw InvalidDrainRequest(
                    "query {} is not drained, so its sink {} cannot end its channel in mode {}", queryId, channel, mode);
            }
        }
        return;
    }

    for (const auto& [channel, mode] : request.sinkModes)
    {
        if (mode == SinkDrainMode::BufferOnDetach)
        {
            throw InvalidDrainRequest("query {} is drained, so its sink {} cannot wait to be redirected", queryId, channel);
        }
    }
    if (!request.stopLocalSources && request.markerChannels.size() != channels.numberOfSources)
    {
        throw InvalidDrainRequest(
            "query {} does not stop its local sources, so all of its {} sources have to end with a marker, but only {} do",
            queryId,
            channels.numberOfSources,
            request.markerChannels.size());
    }
}
}

void QueryDrainTracker::registerQuery(const QueryId& queryId, QueryChannels channels, std::shared_ptr<DrainState> drainState)
{
    PRECONDITION(drainState != nullptr, "A query requires a drain state");
    const auto locked = entries.wlock();
    const auto [it, inserted] = locked->try_emplace(
        queryId, DrainEntry{.channels = std::move(channels), .drainState = std::move(drainState), .sourcesEndingWithMarker = {}});
    INVARIANT(inserted, "Query {} is registered twice", queryId);
}

void QueryDrainTracker::configureDrain(const QueryId& queryId, const DrainRequest& request)
{
    const auto locked = entries.wlock();
    const auto it = locked->find(queryId);
    if (it == locked->end())
    {
        throw QueryNotFound("{}", queryId);
    }
    auto& entry = it->second;
    validate(queryId, entry.channels, request);

    if (!entry.drainState->configure(
            DrainConfiguration{
                .drainId = request.drainId,
                .drainsQuery = request.drainsQuery,
                .stopLocalSources = request.stopLocalSources,
                .sinkModes = request.sinkModes}))
    {
        throw InvalidDrainRequest("query {} is already being drained", queryId);
    }
    for (const auto& channel : request.markerChannels)
    {
        entry.sourcesEndingWithMarker.insert(entry.channels.networkSources.at(channel));
    }
    NES_INFO("Configured drain {} of query {}", request.drainId, queryId);
}

std::unordered_set<OriginId> QueryDrainTracker::sourcesEndingWithMarker(const QueryId& queryId, const DrainId drainId) const
{
    const auto locked = entries.rlock();
    const auto it = locked->find(queryId);
    if (it == locked->end())
    {
        throw QueryNotFound("{}", queryId);
    }
    const auto configuration = it->second.drainState->getConfiguration();
    if (!configuration.has_value() || configuration->drainId != drainId)
    {
        throw InvalidDrainRequest("drain {} of query {} has not been configured", drainId, queryId);
    }
    if (!configuration->stopLocalSources)
    {
        throw InvalidDrainRequest("drain {} does not start at query {}", drainId, queryId);
    }
    return it->second.sourcesEndingWithMarker;
}

void QueryDrainTracker::redirectSink(const QueryId& queryId, const std::string& channel, RedirectTarget target)
{
    const auto locked = entries.rlock();
    const auto it = locked->find(queryId);
    if (it == locked->end())
    {
        throw QueryNotFound("{}", queryId);
    }
    if (it->second.drainState->getSinkMode(channel) != SinkDrainMode::BufferOnDetach)
    {
        throw InvalidDrainRequest("the network sink {} of query {} does not wait to be redirected", channel, queryId);
    }
    if (target.connection.empty() || target.channel.empty())
    {
        throw InvalidDrainRequest("the network sink {} of query {} cannot be redirected to an empty target", channel, queryId);
    }
    NES_INFO("Redirecting the network sink {} of query {} to {}", channel, queryId, target);
    it->second.drainState->setRedirect(channel, std::move(target));
}

}
