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

#include <Sources/NetworkSource.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>

namespace NES
{

NetworkSource::NetworkSource(const SourceDescriptor& sourceDescriptor)
    : channelId(sourceDescriptor.getFromConfig(ConfigParametersNetworkSource::CHANNEL))
    , receiverQueueSize(sourceDescriptor.getFromConfig(ConfigParametersNetworkSource::RECEIVER_QUEUE_SIZE))
    , receiverServer(receiver_instance(sourceDescriptor.getFromConfig(ConfigParametersNetworkSource::BIND)))
{
}

std::ostream& NetworkSource::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSource({})", channelId);
}

void NetworkSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    this->bufferProvider = std::move(provider);

    /// Holding the lock across the registration prevents two concurrently opening sources from registering the same channel twice.
    const auto channels = channelsLock.wlock();
    auto itr = channels->find(channelId);
    if (itr == channels->end())
    {
        const NetworkServiceOptions options{
            .sender_queue_size = 0,
            .max_pending_acks = 0,
            .receiver_queue_size = static_cast<uint32_t>(receiverQueueSize),
        };
        itr = channels->emplace(channelId, register_receiver_channel(*receiverServer, rust::String(channelId), options)).first;
        NES_DEBUG("Receiver channel registered: {}", channelId);
    }
    else
    {
        NES_DEBUG("Reusing receiver channel {}", channelId);
    }
    this->channel = clone_receiver_channel(*itr->second);
}

Source::FillTupleBufferResult NetworkSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(channel.has_value(), "Network Source was not opened");
    PRECONDITION(bufferProvider, "Network Source was opened without a buffer provider");
    TupleBufferBuilder builder(tupleBuffer, *bufferProvider);

    /// If the source is requested to shutdown, this source's handle is detached, which interrupts the call to receive_buffer.
    /// The channel stays open for other sources, e.g. the source of a replacing query plan.
    const std::stop_callback callback(stopToken, [this] { interrupt_receive(**channel); });

    if (receive_buffer(**channel, builder))
    {
        return FillTupleBufferResult::withBytes(tupleBuffer.getNumberOfTuples()); /// Received one buffer
    }

    /// Receive Buffer has failed, which means that the queue was closed or this handle was detached.
    /// The SourceThread logic will figure out if the queue was closed by an external source (i.e. the other side of the network connection)
    /// or because of a stop_request.
    return FillTupleBufferResult::eos(); /// End of Stream
}

void NetworkSource::close()
{
    PRECONDITION(channel.has_value(), "Network Source was closed multiple times or never opened");
    /// Only drops this source's handle. The channel stays registered for replacing sources.
    channel.reset();
    NES_DEBUG("Receiver channel handle released: {}", channelId);
}

DescriptorConfig::Config NetworkSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersNetworkSource>(std::move(config), name());
}

}
