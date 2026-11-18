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

#include <DrainState.hpp>

#include <optional>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

bool DrainState::configure(DrainConfiguration configuration)
{
    auto locked = internal.wlock();
    if (locked->configuration.has_value())
    {
        return false;
    }
    locked->configuration = std::move(configuration);
    return true;
}

std::optional<DrainConfiguration> DrainState::getConfiguration() const
{
    return internal.rlock()->configuration;
}

std::optional<SinkDrainMode> DrainState::getSinkMode(const std::string& channelId) const
{
    const auto locked = internal.rlock();
    if (!locked->configuration.has_value())
    {
        return std::nullopt;
    }
    if (const auto it = locked->configuration->sinkModes.find(channelId); it != locked->configuration->sinkModes.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void DrainState::setRedirect(const std::string& channelId, RedirectTarget target)
{
    internal.wlock()->redirects.insert_or_assign(channelId, std::move(target));
}

std::optional<RedirectTarget> DrainState::getRedirect(const std::string& channelId) const
{
    const auto locked = internal.rlock();
    if (const auto it = locked->redirects.find(channelId); it != locked->redirects.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void DrainState::recordSourceEnd(const OriginId originId, std::optional<DrainId> drainId)
{
    internal.wlock()->sourceEnds.insert_or_assign(originId, drainId);
}

std::optional<std::string> DrainState::findIncompleteDrain() const
{
    const auto locked = internal.rlock();
    PRECONDITION(locked->configuration.has_value(), "Checking a drain which has not been configured");
    return findIncompleteDrain(*locked);
}

void DrainState::markStopped()
{
    internal.wlock()->stopped = true;
}

bool DrainState::isDrained() const
{
    const auto locked = internal.rlock();
    return locked->configuration.has_value() && locked->configuration->drainsQuery && !locked->stopped && !locked->sourceEnds.empty()
        && !findIncompleteDrain(*locked).has_value();
}

std::optional<std::string> DrainState::findIncompleteDrain(const Internal& internal)
{
    for (const auto& [originId, drainId] : internal.sourceEnds)
    {
        if (!drainId.has_value())
        {
            return fmt::format("source {} reached the end of its stream instead of being drained", originId);
        }
        if (*drainId != internal.configuration->drainId)
        {
            return fmt::format(
                "source {} was drained by drain {} instead of drain {}", originId, *drainId, internal.configuration->drainId);
        }
    }
    return std::nullopt;
}

}
