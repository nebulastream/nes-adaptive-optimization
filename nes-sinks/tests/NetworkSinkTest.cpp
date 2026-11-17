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

/// Tests how the NetworkSink ends or keeps its channel when its query takes part in a drain. The sink sends to a NetworkSource over the
/// in-memory transport. The test drives the sink itself and plays the role of the task queue for the tasks the sink repeats.

#include <Sinks/NetworkSink.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <numeric>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/NetworkSource.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <DrainState.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
namespace
{
constexpr auto TIMEOUT = std::chrono::seconds(10);

/// Collects the tasks the sink repeats instead of scheduling them.
struct TestPipelineExecutionContext final : PipelineExecutionContext
{
    TestPipelineExecutionContext(DrainState& drainState, std::shared_ptr<BufferManager> bufferManager)
        : drainState(drainState), bufferManager(std::move(bufferManager))
    {
    }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return false; }

    void repeatTask(const TupleBuffer& buffer, std::chrono::milliseconds) override { repeated.push_back(buffer); }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

    [[nodiscard]] DrainState& getDrainState() override { return drainState; }

    DrainState& drainState;
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::deque<TupleBuffer> repeated;
};

/// The sequence numbers of the buffers a source delivers until its stream ends, and how it ended.
struct Received
{
    std::vector<uint64_t> sequenceNumbers;
    bool endedWithMarker = false;
    DrainId drainId = INVALID_DRAIN_ID;
};

Received receiveUntilEndOfStream(NetworkSource& source, AbstractBufferProvider& bufferProvider)
{
    Received received;
    const std::stop_source neverStopped;
    while (true)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, neverStopped.get_token());
        if (result.isMarker())
        {
            received.endedWithMarker = true;
            received.drainId = result.getDrainId();
            return received;
        }
        if (result.isEoS())
        {
            return received;
        }
        received.sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
}

NetworkServiceOptions options(const uint32_t receiverQueueSize)
{
    return NetworkServiceOptions{
        .sender_queue_size = 1024,
        .max_pending_acks = 64,
        .receiver_queue_size = receiverQueueSize,
        .sender_io_threads = 1,
        .receiver_io_threads = 1,
    };
}

class NetworkSinkTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NetworkSinkTest.log", NES::LogLevel::LOG_DEBUG);
        enable_memcom();
    }

    /// Every test uses its own addresses, because the network services are process-wide singletons.
    void startServices(const uint16_t port, const uint32_t receiverQueueSize)
    {
        receiverAddress = fmt::format("receiver:{}", port);
        senderAddress = fmt::format("sender:{}", port);
        init_receiver_service(rust::String(receiverAddress), rust::String("receiver"), options(receiverQueueSize));
        init_sender_service(rust::String(senderAddress), rust::String("sender"), options(receiverQueueSize));
    }

    std::unique_ptr<NetworkSink> createSink(const std::string& channel)
    {
        const Schema<UnqualifiedUnboundField, Ordered> schema{
            UnqualifiedUnboundField{Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
        const std::unordered_map<Identifier, std::string> config{
            {Identifier::parse("DATA_ENDPOINT"), receiverAddress},
            {Identifier::parse("BIND"), senderAddress},
            {Identifier::parse("CHANNEL"), channel}};
        const auto descriptor = SinkDescriptor::createNamed(
            INVALID_SINK_ID, Identifier::parse("network_sink"), Identifier::parse("Network"), schema, Host{"localhost"}, config, {});
        if (!descriptor.has_value())
        {
            ADD_FAILURE() << "Could not create the descriptor of the network sink";
            return nullptr;
        }
        auto [controller, listener] = createBackpressureChannel();
        backpressureListener.emplace(std::move(listener));
        return std::make_unique<NetworkSink>(
            std::move(controller),
            NetworkSinkConfig{.dataEndpoint = receiverAddress, .bind = senderAddress, .channel = channel, .senderQueueSize = 0, .maxPendingAcks = 0},
            descriptor.value());
    }

    /// A buffer with a single tuple, whose sequence number is also its value.
    TupleBuffer createBuffer(const uint64_t sequenceNumber) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        std::memcpy(buffer.getAvailableMemoryArea<uint8_t>().data(), &sequenceNumber, sizeof(sequenceNumber));
        buffer.setNumberOfTuples(1);
        buffer.setSequenceNumber(SequenceNumber(sequenceNumber));
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);
        buffer.setOriginId(OriginId(1));
        buffer.setWatermark(Timestamp(sequenceNumber));
        return buffer;
    }

    /// Executes the tasks the sink repeated, like the task queue would, until it repeats no more tasks.
    static void runRepeatedTasks(NetworkSink& sink, TestPipelineExecutionContext& pec)
    {
        const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
        while (!pec.repeated.empty())
        {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "The sink keeps repeating its task";
            auto buffer = std::move(pec.repeated.front());
            pec.repeated.pop_front();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            sink.execute(buffer, pec);
        }
    }

    /// Stops the sink, repeating the stop while the sink has buffers in flight.
    static void stopSink(NetworkSink& sink, TestPipelineExecutionContext& pec)
    {
        const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
        sink.stop(pec);
        while (!pec.repeated.empty())
        {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "The sink does not finish stopping";
            pec.repeated.clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            sink.stop(pec);
        }
    }

    void sendAll(NetworkSink& sink, TestPipelineExecutionContext& pec, const uint64_t first, const uint64_t last)
    {
        for (auto sequenceNumber = first; sequenceNumber <= last; ++sequenceNumber)
        {
            sink.execute(createBuffer(sequenceNumber), pec);
            runRepeatedTasks(sink, pec);
        }
    }

    std::string receiverAddress;
    std::string senderAddress;
    std::optional<BackpressureListener> backpressureListener;
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 8192, std::make_shared<NesDefaultMemoryAllocator>());
};

DrainConfiguration drainedQuery(const std::string& channel, const SinkDrainMode mode)
{
    return DrainConfiguration{.drainId = DrainId(5), .drainsQuery = true, .stopLocalSources = true, .sinkModes = {{channel, mode}}};
}

std::vector<uint64_t> range(const uint64_t first, const uint64_t last)
{
    std::vector<uint64_t> values(last - first + 1);
    std::iota(values.begin(), values.end(), first);
    return values;
}

TEST_F(NetworkSinkTest, ForwardsAMarkerIfItsQueryWasDrained)
{
    startServices(3001, 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = receiverAddress, .receiverQueueSize = 0});
    source.open(bufferManager);
    DrainState drainState;
    ASSERT_TRUE(drainState.configure(drainedQuery("c", SinkDrainMode::ForwardMarker)));
    TestPipelineExecutionContext pec(drainState, bufferManager);

    auto sink = createSink("c");
    sink->start(pec);
    sendAll(*sink, pec, 1, 3);
    drainState.recordSourceEnd(OriginId(1), DrainId(5));
    stopSink(*sink, pec);

    const auto received = receiveUntilEndOfStream(source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, range(1, 3));
    EXPECT_TRUE(received.endedWithMarker);
    EXPECT_EQ(received.drainId, DrainId(5));
    source.close();
}

TEST_F(NetworkSinkTest, ClosesItsChannelIfItsQueryWasStoppedDuringTheDrain)
{
    startServices(3002, 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = receiverAddress, .receiverQueueSize = 0});
    source.open(bufferManager);
    DrainState drainState;
    ASSERT_TRUE(drainState.configure(drainedQuery("c", SinkDrainMode::ForwardMarker)));
    TestPipelineExecutionContext pec(drainState, bufferManager);

    auto sink = createSink("c");
    sink->start(pec);
    sendAll(*sink, pec, 1, 3);
    drainState.recordSourceEnd(OriginId(1), DrainId(5));
    drainState.markStopped();
    stopSink(*sink, pec);

    const auto received = receiveUntilEndOfStream(source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, range(1, 3));
    EXPECT_FALSE(received.endedWithMarker);
    source.close();
}

/// A detached sink leaves the stream of the source open, so another sender can continue it.
TEST_F(NetworkSinkTest, DetachesIfItsQueryWasDrained)
{
    startServices(3003, 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = receiverAddress, .receiverQueueSize = 0});
    source.open(bufferManager);
    DrainState drainState;
    ASSERT_TRUE(drainState.configure(drainedQuery("c", SinkDrainMode::Detach)));
    TestPipelineExecutionContext pec(drainState, bufferManager);

    auto sink = createSink("c");
    sink->start(pec);
    sendAll(*sink, pec, 1, 3);
    drainState.recordSourceEnd(OriginId(1), DrainId(5));
    stopSink(*sink, pec);

    /// A sink of a query which is not drained attaches to the same channel and ends the stream.
    DrainState otherDrainState;
    TestPipelineExecutionContext otherPec(otherDrainState, bufferManager);
    auto otherSink = createSink("c");
    otherSink->start(otherPec);
    sendAll(*otherSink, otherPec, 4, 6);
    stopSink(*otherSink, otherPec);

    const auto received = receiveUntilEndOfStream(source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, range(1, 6));
    EXPECT_FALSE(received.endedWithMarker);
    source.close();
}

/// The receiver of a sink in BufferOnDetach mode is drained. The sink keeps its data until it is redirected to a new receiver, which then
/// receives everything the old receiver did not, without duplicates.
TEST_F(NetworkSinkTest, BuffersWhileDetachedAndDeliversEverythingAfterTheRedirect)
{
    startServices(3004, 2);
    NetworkSource oldSource(NetworkSourceConfig{.channel = "old", .bind = receiverAddress, .receiverQueueSize = 0});
    oldSource.open(bufferManager);
    DrainState drainState;
    ASSERT_TRUE(drainState.configure(
        DrainConfiguration{.drainId = DrainId(5), .drainsQuery = false, .stopLocalSources = false, .sinkModes = {{"old", SinkDrainMode::BufferOnDetach}}}));
    TestPipelineExecutionContext pec(drainState, bufferManager);

    auto sink = createSink("old");
    sink->start(pec);
    sendAll(*sink, pec, 1, 20);

    /// The old receiver takes a few buffers, then it is drained
    std::vector<uint64_t> sequenceNumbers;
    const std::stop_source neverStopped;
    for (size_t i = 0; i < 3; ++i)
    {
        auto buffer = bufferManager->getBufferBlocking();
        ASSERT_FALSE(oldSource.fillTupleBuffer(buffer, neverStopped.get_token()).isEoS());
        sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
    ASSERT_TRUE(oldSource.drain());
    const auto remainder = receiveUntilEndOfStream(oldSource, *bufferManager);
    sequenceNumbers.insert(sequenceNumbers.end(), remainder.sequenceNumbers.begin(), remainder.sequenceNumbers.end());
    oldSource.close();

    /// Send until the sink notices the detach and buffers instead of sending
    uint64_t lastSent = 20;
    const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
    while (pec.repeated.empty())
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "The sink did not notice that its receiver detached";
        sink->execute(createBuffer(++lastSent), pec);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    for (size_t i = 0; i < 10; ++i)
    {
        sink->execute(createBuffer(++lastSent), pec);
    }
    /// Without a redirect target, the sink keeps retrying its pending buffer
    for (size_t i = 0; i < 10; ++i)
    {
        ASSERT_EQ(pec.repeated.size(), 1);
        auto pending = std::move(pec.repeated.front());
        pec.repeated.pop_front();
        sink->execute(pending, pec);
    }

    NetworkSource newSource(NetworkSourceConfig{.channel = "new", .bind = receiverAddress, .receiverQueueSize = 0});
    newSource.open(bufferManager);
    drainState.setRedirect("old", RedirectTarget{.connection = receiverAddress, .channel = "new"});
    Received rest;
    {
        /// The new source has to receive while the sink sends, as its small queue applies backpressure.
        const std::jthread receiver([&] { rest = receiveUntilEndOfStream(newSource, *bufferManager); });
        runRepeatedTasks(*sink, pec);
        stopSink(*sink, pec);
    }
    newSource.close();
    EXPECT_FALSE(rest.endedWithMarker);

    sequenceNumbers.insert(sequenceNumbers.end(), rest.sequenceNumbers.begin(), rest.sequenceNumbers.end());
    std::ranges::sort(sequenceNumbers);
    EXPECT_EQ(sequenceNumbers, range(1, lastSent));
}

/// Without the BufferOnDetach mode, a receiver which detaches fails the sink, as nobody would redirect it.
TEST_F(NetworkSinkTest, FailsIfItsReceiverDetachesWithoutBufferOnDetach)
{
    startServices(3005, 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = receiverAddress, .receiverQueueSize = 0});
    source.open(bufferManager);
    DrainState drainState;
    TestPipelineExecutionContext pec(drainState, bufferManager);

    auto sink = createSink("c");
    sink->start(pec);
    /// Connect the sink before the receiver detaches. A channel which detaches before any sender connected is removed instead.
    sendAll(*sink, pec, 1, 1);
    auto first = bufferManager->getBufferBlocking();
    const std::stop_source neverStopped;
    ASSERT_FALSE(source.fillTupleBuffer(first, neverStopped.get_token()).isEoS());
    ASSERT_TRUE(source.drain());
    receiveUntilEndOfStream(source, *bufferManager);
    source.close();

    uint64_t sequenceNumber = 1;
    const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
    EXPECT_THROW(
        {
            while (std::chrono::steady_clock::now() < deadline)
            {
                sink->execute(createBuffer(++sequenceNumber), pec);
                pec.repeated.clear();
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        },
        Exception);
}

}
}
