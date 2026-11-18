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

#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SinkDrain.hpp>

namespace NES
{

/// What the coordinator tells a worker about the drain of one of its local queries.
struct DrainRequest
{
    DrainId drainId = INVALID_DRAIN_ID;
    bool drainsQuery = false;
    bool stopLocalSources = false;
    /// The channel ids of the network sources which end with a marker from a drained upstream query.
    std::vector<std::string> markerChannels;
    std::unordered_map<std::string, SinkDrainMode> sinkModes;

    bool operator==(const DrainRequest&) const = default;
};

}
