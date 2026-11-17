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
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>

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

/// What a worker is told about the drain of one of its local queries.
struct DrainConfiguration
{
    DrainId drainId = INVALID_DRAIN_ID;
    /// The query itself is drained. If false, the query only takes part in the drain through the modes of its sinks, e.g. it buffers
    /// until it is redirected, and keeps running.
    bool drainsQuery = false;
    /// The drain starts here: the query stops its sources which are not fed by a drained upstream query.
    bool stopLocalSources = false;
    /// The drain mode of the network sinks, keyed by their channel id. Sinks without an entry close their channel as usual.
    std::unordered_map<std::string, SinkDrainMode> sinkModes;

    bool operator==(const DrainConfiguration&) const = default;
};

/// The receiver a network sink in `SinkDrainMode::BufferOnDetach` is redirected to.
struct RedirectTarget
{
    std::string connection;
    std::string channel;

    bool operator==(const RedirectTarget&) const = default;
};

/// The drain state of one local query. The worker writes it when it is told about a drain, the query's pipelines read it through the
/// PipelineExecutionContext. All members are thread-safe.
class DrainState
{
public:
    /// Configures the drain. Only one drain per query is supported, so this returns false if a drain has already been configured.
    [[nodiscard]] bool configure(DrainConfiguration configuration);

    /// Returns the configuration of the drain, or nothing if the query is not being drained.
    [[nodiscard]] std::optional<DrainConfiguration> getConfiguration() const;

    /// Returns the drain mode of the network sink with the given channel id, or nothing if the sink closes its channel as usual.
    [[nodiscard]] std::optional<SinkDrainMode> getSinkMode(const std::string& channelId) const;

    /// Sets the receiver the network sink with the given channel id is redirected to. A later call replaces the target.
    void setRedirect(const std::string& channelId, RedirectTarget target);

    /// Returns the receiver the network sink with the given channel id is redirected to, or nothing if no redirect has been set.
    [[nodiscard]] std::optional<RedirectTarget> getRedirect(const std::string& channelId) const;

    /// Records how a source of the query ended its stream: drained by the given drain, or with a plain end of stream (nothing).
    void recordSourceEnd(OriginId originId, std::optional<DrainId> drainId);

    /// Returns why the drain is incomplete, or nothing if every source which ended was drained by the configured drain.
    /// Must only be called once a drain has been configured.
    [[nodiscard]] std::optional<std::string> findIncompleteDrain() const;

    /// Records that the query is being stopped. A stopped query is not drained, even if some of its sources were drained before.
    void markStopped();

    /// Returns true if the query drains itself and has been drained so far: it is not being stopped, at least one source has ended,
    /// and every source which ended was drained by the configured drain. A network sink checks this before it ends its channel in
    /// its drain mode, as all sources upstream of it have ended by then.
    [[nodiscard]] bool isDrained() const;

private:
    struct Internal
    {
        std::optional<DrainConfiguration> configuration;
        std::unordered_map<std::string, RedirectTarget> redirects;
        std::unordered_map<OriginId, std::optional<DrainId>> sourceEnds;
        bool stopped = false;
    };

    [[nodiscard]] static std::optional<std::string> findIncompleteDrain(const Internal& internal);

    folly::Synchronized<Internal> internal;
};

std::ostream& operator<<(std::ostream& os, SinkDrainMode mode);
std::ostream& operator<<(std::ostream& os, const RedirectTarget& target);

}

FMT_OSTREAM(NES::SinkDrainMode);
FMT_OSTREAM(NES::RedirectTarget);
