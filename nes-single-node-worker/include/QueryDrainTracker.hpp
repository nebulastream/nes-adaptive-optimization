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
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Identifiers/Identifiers.hpp>
#include <folly/Synchronized.h>
#include <DrainRequest.hpp>
#include <DrainState.hpp>
#include <QueryId.hpp>

namespace NES
{

/// The network channels of a local query, which a drain request refers to.
struct QueryChannels
{
    /// The channel ids of the network sources, with the origin id of the source.
    std::unordered_map<std::string, OriginId> networkSources;
    /// The channel ids of the network sinks.
    std::unordered_set<std::string> networkSinks;
    /// The number of sources of the query, including the network sources.
    size_t numberOfSources = 0;
};

/// Keeps the drain state and the network channels of the local queries of a worker, and checks the drain requests against them.
/// All members are thread-safe. Requests which do not match the query throw `InvalidDrainRequest`, unknown queries `QueryNotFound`.
class QueryDrainTracker
{
public:
    void registerQuery(const QueryId& queryId, QueryChannels channels, std::shared_ptr<DrainState> drainState);

    /// Configures the drain of the query (phase 1 of a drain).
    void configureDrain(const QueryId& queryId, const DrainRequest& request);

    /// Checks that the query can start the drain `drainId` and returns the origin ids of the sources which end with a marker instead of
    /// being drained locally.
    [[nodiscard]] std::unordered_set<OriginId> sourcesEndingWithMarker(const QueryId& queryId, DrainId drainId) const;

    /// Redirects the network sink with the channel id `channel`, which buffers since its receiver was drained.
    void redirectSink(const QueryId& queryId, const std::string& channel, RedirectTarget target);

private:
    /// What the tracker knows about the drain of one local query.
    struct DrainEntry
    {
        QueryChannels channels;
        std::shared_ptr<DrainState> drainState;
        std::unordered_set<OriginId> sourcesEndingWithMarker;
    };

    folly::Synchronized<std::unordered_map<QueryId, DrainEntry>> entries;
};

}
