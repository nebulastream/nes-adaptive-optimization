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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Pointers.hpp>
#include <folly/Synchronized.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class NetworkSink final : public Sink
{
public:
    constexpr static auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    static const std::string& name()
    {
        static const std::string Instance = "Network";
        return Instance;
    }

    NetworkSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~NetworkSink() override = default;

    NetworkSink(const NetworkSink&) = delete;
    NetworkSink& operator=(const NetworkSink&) = delete;
    NetworkSink(NetworkSink&&) = delete;
    NetworkSink& operator=(NetworkSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    /// A sender channel that outlives a single NetworkSink, e.g. when the query plan is replaced during adaptive re-optimization.
    /// A replacing plan restarts its sources, so the sequence numbers of an origin start again at SequenceNumber::INITIAL.
    /// To present a single continuous stream to the receiver, the channel tracks the highest sequence number sent per origin and
    /// every new sink continues from there.
    struct SharedSenderChannel
    {
        explicit SharedSenderChannel(rust::Box<SenderDataChannel> channel) : channel(std::move(channel)) { }

        rust::Box<SenderDataChannel> channel;
        folly::Synchronized<std::unordered_map<OriginId, uint64_t>> highestSentSequenceNumbersLock;
    };

    static inline folly::Synchronized<std::unordered_map<std::string, SharedPtr<SharedSenderChannel>>> channelsLock;

    /// Sends the buffer with its sequence number shifted by the offset of its origin.
    SendResult sendBuffer(const TupleBuffer& buffer);

    size_t tupleSize;
    folly::Synchronized<std::vector<TupleBuffer>> bufferBacklog;
    BackpressureHandler backpressureHandler;
    std::optional<rust::Box<SenderNetworkService>> server;
    SharedPtr<SharedSenderChannel> channel;
    /// Snapshot of the channel's highestSentSequenceNumbers taken in start(). Read-only afterwards.
    std::unordered_map<OriginId, uint64_t> sequenceNumberOffsets;
    std::string channelId;
    std::string connectionAddr;
    std::string thisConnection;
    size_t senderQueueSize;
    size_t maxPendingAcks;
    std::atomic_bool closed;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersNetworkSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> DATA_ENDPOINT{
        "DATA_ENDPOINT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DATA_ENDPOINT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "BIND",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    /// Per-channel sender queue size override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> SENDER_QUEUE_SIZE{
        "SENDER_QUEUE_SIZE",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SENDER_QUEUE_SIZE, config); }};

    /// Per-channel max pending acks override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_PENDING_ACKS{
        "MAX_PENDING_ACKS",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_PENDING_ACKS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, DATA_ENDPOINT, CHANNEL, BIND, SENDER_QUEUE_SIZE, MAX_PENDING_ACKS);
};

/// NOLINTEND(cert-err58-cpp)

}
