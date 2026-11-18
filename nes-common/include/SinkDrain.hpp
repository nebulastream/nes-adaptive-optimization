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

#include <cstdint>
#include <ostream>
#include <string>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

/// How a network sink ends its channel when its query is drained.
enum class SinkDrainMode : uint8_t
{
    /// The downstream query is drained as well: send a drain marker, then close the channel.
    ForwardMarker,
    /// The downstream query keeps running: leave the channel without ending the stream, so another sender can attach to it.
    Detach,
    /// The sink's own query is not drained, but the downstream query is. When the receiver detaches, the sink keeps its data
    /// until it is redirected to a new receiver.
    BufferOnDetach,
};

/// The receiver a network sink in `SinkDrainMode::BufferOnDetach` is redirected to.
struct RedirectTarget
{
    std::string connection;
    std::string channel;

    bool operator==(const RedirectTarget&) const = default;
};

inline std::ostream& operator<<(std::ostream& os, const SinkDrainMode mode)
{
    return os << magic_enum::enum_name(mode);
}

inline std::ostream& operator<<(std::ostream& os, const RedirectTarget& target)
{
    return os << target.connection << "/" << target.channel;
}

}

FMT_OSTREAM(NES::SinkDrainMode);
FMT_OSTREAM(NES::RedirectTarget);
