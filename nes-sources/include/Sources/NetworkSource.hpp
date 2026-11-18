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
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct NetworkSourceConfig
{
    /// UUID identifying the receiver channel of this source.
    std::string channel;
    /// host:port endpoint the receiver network service binds to.
    std::string bind;
    /// Per-channel receiver queue size override. 0 means use the worker-level default.
    size_t receiverQueueSize;

    static std::expected<NetworkSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Keeps the sequence numbers of a channel unique per origin when a new sender attaches after the previous one detached. A new sender may
/// number its buffers from the start, e.g. because it is a new instance of the drained upstream query. If its first buffer of an origin
/// has a sequence number which was already delivered, all of its sequence numbers of that origin are shifted past the highest one
/// delivered, so that downstream operators see one gapless sequence per origin. A sender which continues the sequence numbers keeps them.
class SequenceRenumbering
{
public:
    /// Shifts the sequence number of a received buffer.
    void renumber(TupleBuffer& buffer);
    /// The buffers renumbered from now on come from a new sender.
    void startNewSender();

private:
    struct OriginSequences
    {
        SequenceNumber::Underlying offset = 0;
        SequenceNumber::Underlying highest = 0;
        /// Set until the first buffer of the origin from a new sender has decided the offset.
        bool newSender = false;
    };

    std::unordered_map<OriginId, OriginSequences> origins;
};

class NetworkSource final : public Source
{
public:
    constexpr static std::string_view NAME = "Network";

    explicit NetworkSource(const NetworkSourceConfig& config);
    ~NetworkSource() override = default;

    NetworkSource(const NetworkSource&) = delete;
    NetworkSource& operator=(const NetworkSource&) = delete;
    NetworkSource(NetworkSource&&) = delete;
    NetworkSource& operator=(NetworkSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> provider) override;
    void close() override;

    /// Detaches the receiver channel: the sender keeps all data which has not been received yet. The data which has already been
    /// received belongs to the drained query and is still delivered, then the stream ends.
    [[nodiscard]] bool drain() override;

    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string channelId;
    size_t receiverQueueSize;
    /// Protects `channel` and `drainRequested` against a concurrent `drain()`. The source thread does not lock while it receives, as it
    /// is the only one to change `channel`.
    std::mutex drainMutex;
    std::optional<rust::Box<ReceiverDataChannel>> channel;
    bool drainRequested = false;
    rust::Box<ReceiverNetworkService> receiverServer;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    SequenceRenumbering sequenceRenumbering;
};

}
