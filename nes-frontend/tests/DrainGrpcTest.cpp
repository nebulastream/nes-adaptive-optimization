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

#include <QueryManager/QueryManager.hpp>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/DrainPlanner.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Subprocess.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <BaseUnitTest.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <DrainTestUtil.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <WorkerCatalog.hpp>
#include <unistd.h>

namespace NES
{
namespace
{
constexpr auto STATUS_TIMEOUT = std::chrono::seconds(60);

/// Asks the kernel for a free port on the loopback interface. Another process may take the port before the worker binds it.
uint16_t freePort()
{
    const int socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0)
    {
        throw TestException("cannot open a socket to find a free port");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    socklen_t length = sizeof(address);
    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast) the socket API takes a generic address
    const bool found = ::bind(socketFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0
        && ::getsockname(socketFd, reinterpret_cast<sockaddr*>(&address), &length) == 0;
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
    ::close(socketFd);
    if (!found)
    {
        throw TestException("cannot find a free port");
    }
    return ntohs(address.sin_port);
}

/// A `nes-single-node-worker` process, which logs into its own directory.
class WorkerProcess
{
public:
    WorkerProcess(const std::filesystem::path& directory, const std::string& grpcAddress, const std::string& dataAddress)
        : grpcAddress(grpcAddress)
    {
        std::filesystem::create_directories(directory);
        process.emplace(
            std::vector<std::string>{
                NES_WORKER_BINARY,
                "--",
                "--grpc=" + grpcAddress,
                "--data_address=" + dataAddress,
                "--worker.default_query_execution.execution_mode=INTERPRETER",
                "--worker.query_engine.number_of_worker_threads=2"},
            subprocess::cwd{directory.string()},
            subprocess::output{(directory / "stdout.log").c_str()},
            subprocess::error{(directory / "stderr.log").c_str()},
            /// The worker dies with the test, even if the test crashes.
            subprocess::preexec_func{[] { ::prctl(PR_SET_PDEATHSIG, SIGKILL); }});
    }

    WorkerProcess(const WorkerProcess&) = delete;
    WorkerProcess& operator=(const WorkerProcess&) = delete;
    WorkerProcess(WorkerProcess&&) = delete;
    WorkerProcess& operator=(WorkerProcess&&) = delete;

    ~WorkerProcess()
    {
        process->kill(SIGTERM);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (process->poll() == -1 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (process->poll() == -1)
        {
            process->kill(SIGKILL);
            process->wait();
        }
    }

    [[nodiscard]] bool waitUntilReachable() const
    {
        const auto channel = grpc::CreateChannel(grpcAddress, grpc::InsecureChannelCredentials());
        return channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(30));
    }

private:
    std::string grpcAddress;
    std::optional<subprocess::Popen> process;
};

/// The first column of every line of a CSV file that starts with a number.
std::vector<uint64_t> readFirstColumn(const std::filesystem::path& file)
{
    std::vector<uint64_t> values;
    std::ifstream stream(file);
    std::string line;
    while (std::getline(stream, line))
    {
        const auto field = line.substr(0, line.find(','));
        if (!field.empty() && std::ranges::all_of(field, [](const char chr) { return chr >= '0' && chr <= '9'; }))
        {
            values.push_back(std::stoull(field));
        }
    }
    return values;
}
}

/// Runs the chain a → b → c on three worker processes, which the QueryManager drives over gRPC and which talk to each other over
/// TCP: a generator that counts up on a, a relay on b and a CSV file sink on c. All tests share the workers but start their own
/// queries.
class DrainGrpcTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DrainGrpcTest.log", LogLevel::LOG_DEBUG);
        directory = std::filesystem::temp_directory_path() / fmt::format("drain-grpc-test-{}", ::getpid());
        std::filesystem::remove_all(directory);
        for (const auto* name : {"a", "b", "c"})
        {
            const Host host(fmt::format("127.0.0.1:{}", freePort()));
            const auto dataAddress = fmt::format("127.0.0.1:{}", freePort());
            hosts.emplace(name, host);
            dataAddresses.emplace(host, dataAddress);
            workerProcesses.push_back(std::make_unique<WorkerProcess>(directory / name, host.getRawValue(), dataAddress));
        }
        for (const auto& worker : workerProcesses)
        {
            ASSERT_TRUE(worker->waitUntilReachable()) << "a worker did not start, see the logs in " << directory;
        }
    }

    static void TearDownTestSuite()
    {
        workerProcesses.clear();
        hosts.clear();
        dataAddresses.clear();
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        ASSERT_EQ(workerProcesses.size(), 3) << "the workers did not start";
        sinkFile = directory / fmt::format("{}.csv", ::testing::UnitTest::GetInstance()->current_test_info()->name());

        const Schema<UnqualifiedUnboundField, Ordered> schema{
            UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::UINT64},
            UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT64}};
        const Schema<UnqualifiedUnboundField, Ordered> windowedSchema{
            UnqualifiedUnboundField{Identifier::parse("s"), DataType::Type::UINT64}};
        const std::vector<Statement> statements{
            CreateWorkerStatement{
                .host = host("c").getRawValue(), .dataAddress = dataAddresses.at(host("c")), .maxOperators = 10, .downstream = {}, .config = {}},
            CreateWorkerStatement{
                .host = host("b").getRawValue(),
                .dataAddress = dataAddresses.at(host("b")),
                .maxOperators = 0,
                .downstream = {host("c").getRawValue()},
                .config = {}},
            CreateWorkerStatement{
                .host = host("a").getRawValue(),
                .dataAddress = dataAddresses.at(host("a")),
                .maxOperators = 0,
                .downstream = {host("b").getRawValue()},
                .config = {}},
            CreateLogicalSourceStatement{.name = Identifier::parse("numbers"), .schema = schema},
            CreatePhysicalSourceStatement{
                .attachedTo = LogicalSourceName(Identifier::parse("numbers")),
                .sourceType = Identifier::parse("Generator"),
                .host = host("a"),
                .sourceConfig
                = {{Identifier::parse("stop_generator_when_sequence_finishes"), "NONE"},
                   {Identifier::parse("seed"), "1"},
                   {Identifier::parse("generator_rate_type"), "FIXED"},
                   {Identifier::parse("generator_rate_config"), "emit_rate 1000"},
                   {Identifier::parse("generator_schema"), "SEQUENCE UINT64 0 1000000000 1, SEQUENCE UINT64 0 1000000000 1"}},
                .parserConfig = {{Identifier::parse("type"), "CSV"}}},
            CreateSinkStatement{
                .name = Identifier::parse("out"),
                .sinkType = Identifier::parse("File"),
                .schema = schema,
                .host = host("c"),
                .sinkConfig
                = {{Identifier::parse("file_path"), sinkFile.string()},
                   {Identifier::parse("output_format"), "CSV"},
                   /// Successors of c append to the same file, so that the file holds everything that reached the end of the chain.
                   {Identifier::parse("append"), "true"}},
                .formatConfig = {}},
            CreateSinkStatement{
                .name = Identifier::parse("windowed"),
                .sinkType = Identifier::parse("Void"),
                .schema = windowedSchema,
                .host = host("c"),
                .sinkConfig = {},
                .formatConfig = {}}};

        TopologyStatementHandler topologyHandler{nullptr, workers};
        SinkStatementHandler sinkHandler{sinks, RequireHostConfig{}};
        SourceStatementHandler sourceHandler{sources, RequireHostConfig{}};
        handleStatements(statements, topologyHandler, sinkHandler, sourceHandler);

        manager = std::make_unique<QueryManager>(workers, createGRPCBackend());
    }

    void TearDown() override
    {
        for (const auto& id : manager->queries())
        {
            (void)manager->stop(id);
        }
        manager.reset();
        BaseUnitTest::TearDown();
    }

    static const Host& host(const std::string& name) { return hosts.at(name); }

    /// Optimizes and starts `sql` across a, b and c and waits until it runs everywhere.
    void startChain(const std::string& sql)
    {
        const QueryOptimizer optimizer(
            defaultConfiguration<QueryOptimizerConfiguration>(), sources, sinks, workers, std::make_shared<ModelCatalog>());
        plan.emplace(optimizer.optimize(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql)));
        for (const auto* name : {"a", "b", "c"})
        {
            ASSERT_EQ((*plan)[host(name)].size(), 1) << "expected one local plan on " << name;
        }
        const auto started = manager->start(*plan);
        ASSERT_TRUE(started.has_value()) << started.error().front();
        query = *started;
        waitFor({{at("a"), QueryStatus::Running}, {at("b"), QueryStatus::Running}, {at("c"), QueryStatus::Running}});
    }

    [[nodiscard]] LocalQueryRef at(const std::string& name, const std::optional<DistributedQueryId>& distributed = std::nullopt) const
    {
        const auto localQueries = manager->getQuery(distributed.value_or(query))->getLocalQueries();
        return LocalQueryRef{.host = host(name), .queryId = localQueries.at(host(name)).front()};
    }

    [[nodiscard]] QueryStatus statusOf(const LocalQueryRef& ref) const
    {
        const auto status = manager->status(ref.queryId.getDistributedQueryId());
        EXPECT_TRUE(status.has_value());
        const auto& local = status->localStatusSnapshots.at(ref.host).at(ref.queryId);
        if (!local.has_value())
        {
            ADD_FAILURE() << "cannot read the status of the local query on " << ref.host << ": " << local.error();
            return QueryStatus::Failed;
        }
        if (local->metrics.error.has_value())
        {
            NES_ERROR("Local query {} on {} failed: {}", ref.queryId, ref.host, local->metrics.error->what());
        }
        return local->state;
    }

    /// Waits until every local query has reached its expected status.
    void waitFor(const std::unordered_map<LocalQueryRef, QueryStatus>& expected) const
    {
        const auto deadline = std::chrono::steady_clock::now() + STATUS_TIMEOUT;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (std::ranges::all_of(expected, [&](const auto& entry) { return statusOf(entry.first) == entry.second; }))
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for (const auto& [ref, status] : expected)
        {
            EXPECT_EQ(statusOf(ref), status) << "local query on " << ref.host;
        }
    }

    /// Waits until the sink file holds more than `count` rows and returns their first column.
    [[nodiscard]] std::vector<uint64_t> waitForMoreRowsThan(const size_t count) const
    {
        const auto deadline = std::chrono::steady_clock::now() + STATUS_TIMEOUT;
        auto values = readFirstColumn(sinkFile);
        while (values.size() <= count && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            values = readFirstColumn(sinkFile);
        }
        EXPECT_GT(values.size(), count) << "the sink did not receive new rows";
        return values;
    }

    /// Starts a local plan on one worker as a distributed query of its own.
    DistributedQueryId startSuccessor(const std::string& name, const LogicalPlan& successor)
    {
        const auto started = manager->start(DistributedLogicalPlan({{host(name), {successor}}}, successor));
        EXPECT_TRUE(started.has_value()) << started.error().front();
        return *started;
    }

    static std::filesystem::path directory;
    static std::map<std::string, Host> hosts;
    static std::unordered_map<Host, std::string> dataAddresses;
    static std::vector<std::unique_ptr<WorkerProcess>> workerProcesses;

    std::filesystem::path sinkFile;
    std::shared_ptr<SourceCatalog> sources = std::make_shared<SourceCatalog>();
    std::shared_ptr<SinkCatalog> sinks = std::make_shared<SinkCatalog>();
    std::shared_ptr<WorkerCatalog> workers = std::make_shared<WorkerCatalog>();
    std::unique_ptr<QueryManager> manager;
    std::optional<DistributedLogicalPlan> plan;
    DistributedQueryId query{DistributedQueryId::INVALID};
};

std::filesystem::path DrainGrpcTest::directory;
std::map<std::string, Host> DrainGrpcTest::hosts;
std::unordered_map<Host, std::string> DrainGrpcTest::dataAddresses;
std::vector<std::unique_ptr<WorkerProcess>> DrainGrpcTest::workerProcesses;

TEST_F(DrainGrpcTest, DrainsTheWholeChain)
{
    startChain("SELECT * FROM numbers INTO out");
    const auto drainId = manager->drain(query, {at("a"), at("b"), at("c")});
    ASSERT_TRUE(drainId.has_value()) << drainId.error().front();

    waitFor({{at("a"), QueryStatus::Drained}, {at("b"), QueryStatus::Drained}, {at("c"), QueryStatus::Drained}});
}

TEST_F(DrainGrpcTest, ReattachesAfterAnUpstreamDrain)
{
    startChain("SELECT * FROM numbers INTO out");
    const auto drainId = manager->drain(query, {at("a")});
    ASSERT_TRUE(drainId.has_value()) << drainId.error().front();
    waitFor({{at("a"), QueryStatus::Drained}});
    EXPECT_EQ(statusOf(at("b")), QueryStatus::Running);
    EXPECT_EQ(statusOf(at("c")), QueryStatus::Running);

    /// A copy of a's plan sends into b's channel, which b kept open for a new sender.
    const auto& original = (*plan)[host("a")].front();
    const auto successor = startSuccessor("a", LogicalPlan(QueryId::createDistributed(DistributedQueryId("upstream")), original.getRootOperators()));
    waitFor({{at("a", successor), QueryStatus::Running}});

    /// a′ counts from 0 again, so its data has arrived once 0 is in the file twice.
    const auto deadline = std::chrono::steady_clock::now() + STATUS_TIMEOUT;
    while (std::ranges::count(readFirstColumn(sinkFile), 0) < 2 && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(std::ranges::count(readFirstColumn(sinkFile), 0), 2) << "the data of a′ did not reach the sink";
    EXPECT_EQ(statusOf(at("b")), QueryStatus::Running);
    EXPECT_EQ(statusOf(at("c")), QueryStatus::Running);
}

TEST_F(DrainGrpcTest, RedirectsWithoutLossOrDuplicates)
{
    startChain("SELECT * FROM numbers INTO out");
    const auto drainId = manager->drain(query, {at("b"), at("c")});
    ASSERT_TRUE(drainId.has_value()) << drainId.error().front();
    waitFor({{at("b"), QueryStatus::Drained}, {at("c"), QueryStatus::Drained}});
    EXPECT_EQ(statusOf(at("a")), QueryStatus::Running);
    const auto rowsBefore = readFirstColumn(sinkFile).size();

    const auto channel = UUIDToString(generateUUID());
    const auto successor = startSuccessor(
        "c",
        Testing::withNewSourceChannel(
            (*plan)[host("c")].front(), *sources, channel, QueryId::createDistributed(DistributedQueryId("downstream"))));
    waitFor({{at("c", successor), QueryStatus::Running}});
    const auto redirected = manager->redirectSink(at("a"), Testing::networkSinkChannel((*plan)[host("a")].front()), at("c", successor), channel);
    ASSERT_TRUE(redirected.has_value()) << redirected.error();
    (void)waitForMoreRowsThan(rowsBefore + 1000);

    /// Stopping the generator ends the stream to the successor, which then has written everything it received.
    ASSERT_TRUE(manager->stop(query).has_value());
    waitFor({{at("c", successor), QueryStatus::Stopped}});

    auto values = readFirstColumn(sinkFile);
    std::ranges::sort(values);
    ASSERT_FALSE(values.empty());
    const auto duplicate = std::ranges::adjacent_find(values);
    EXPECT_EQ(duplicate, values.end()) << "value " << *duplicate << " reached the sink twice";
    EXPECT_EQ(values.front(), 0);
    EXPECT_EQ(values.back(), values.size() - 1) << "values are missing between 0 and " << values.back();
}

TEST_F(DrainGrpcTest, RejectsADrainWithAWindowDownstreamOfTheCut)
{
    startChain("SELECT s FROM (SELECT SUM(value) AS s FROM numbers WINDOW TUMBLING(id, size 1 sec)) INTO windowed");
    const auto drainId = manager->drain(query, {at("a")});
    ASSERT_FALSE(drainId.has_value());
    EXPECT_EQ(drainId.error().front().code(), ErrorCode::InvalidDrain);

    for (const auto* name : {"a", "b", "c"})
    {
        EXPECT_EQ(statusOf(at(name)), QueryStatus::Running) << name;
    }
}

TEST_F(DrainGrpcTest, ReportsTheErrorCodeOfTheWorker)
{
    startChain("SELECT * FROM numbers INTO out");
    ASSERT_TRUE(manager->drain(query, {at("b"), at("c")}).has_value());
    waitFor({{at("b"), QueryStatus::Drained}, {at("c"), QueryStatus::Drained}});

    /// The workers reject a second drain of b and c, as they are already being drained.
    const auto drainId = manager->drain(query, {at("b"), at("c")});
    ASSERT_FALSE(drainId.has_value());
    EXPECT_TRUE(std::ranges::any_of(drainId.error(), [](const Exception& error) { return error.code() == ErrorCode::InvalidDrainRequest; }))
        << drainId.error().front();
}

TEST_F(DrainGrpcTest, StopsAnUpstreamWhichWaitsToBeRedirected)
{
    startChain("SELECT * FROM numbers INTO out");
    ASSERT_TRUE(manager->drain(query, {at("b"), at("c")}).has_value());
    waitFor({{at("b"), QueryStatus::Drained}, {at("c"), QueryStatus::Drained}});
    /// Let a buffer for a while, so that its sink applies backpressure to the generator.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_TRUE(manager->stop(query).has_value());
    waitFor({{at("a"), QueryStatus::Stopped}});
}

}
