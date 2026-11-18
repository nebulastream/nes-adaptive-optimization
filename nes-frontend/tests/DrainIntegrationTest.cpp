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

#include <any>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Configurations/Util.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/DrainPlanner.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/NetworkSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/NetworkSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
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
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
const Host A("a:8080");
const Host B("b:8080");
const Host C("c:8080");
}

/// Runs the chain a → b → c on three embedded workers, which talk to each other over the network: a generator on a, a relay on b
/// and a sink on c.
class DrainIntegrationTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DrainIntegrationTest.log", LogLevel::LOG_DEBUG);
        /// Routes the traffic between the embedded workers through memory, as several TCP receivers cannot share a process.
        ::enable_memcom();
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        /// Every test uses its own data addresses, because the network services are process-wide singletons.
        static size_t testCounter = 0;
        ++testCounter;
        dataAddresses
            = {{A, fmt::format("a{}:9090", testCounter)},
               {B, fmt::format("b{}:9090", testCounter)},
               {C, fmt::format("c{}:9090", testCounter)}};

        const Schema<UnqualifiedUnboundField, Ordered> schema{
            UnqualifiedUnboundField{Identifier::parse("id"), DataType::Type::UINT64},
            UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::UINT64}};
        const std::vector<Statement> statements{
            CreateWorkerStatement{
                .host = C.getRawValue(), .dataAddress = dataAddresses.at(C), .maxOperators = 1, .downstream = {}, .config = {}},
            CreateWorkerStatement{
                .host = B.getRawValue(),
                .dataAddress = dataAddresses.at(B),
                .maxOperators = 0,
                .downstream = {C.getRawValue()},
                .config = {}},
            CreateWorkerStatement{
                .host = A.getRawValue(),
                .dataAddress = dataAddresses.at(A),
                .maxOperators = 0,
                .downstream = {B.getRawValue()},
                .config = {}},
            CreateLogicalSourceStatement{.name = Identifier::parse("numbers"), .schema = schema},
            CreatePhysicalSourceStatement{
                .attachedTo = LogicalSourceName(Identifier::parse("numbers")),
                .sourceType = Identifier::parse("Generator"),
                .host = A,
                .sourceConfig
                = {{Identifier::parse("stop_generator_when_sequence_finishes"), "NONE"},
                   {Identifier::parse("seed"), "1"},
                   {Identifier::parse("generator_rate_type"), "FIXED"},
                   {Identifier::parse("generator_rate_config"), "emit_rate 1000"},
                   {Identifier::parse("generator_schema"), "SEQUENCE UINT64 0 1000000000 1, SEQUENCE UINT64 0 1000000000 1"}},
                .parserConfig = {{Identifier::parse("type"), "CSV"}}},
            CreateSinkStatement{
                .name = Identifier::parse("out"),
                .sinkType = Identifier::parse("VOID"),
                .schema = schema,
                .host = C,
                .sinkConfig = {},
                .formatConfig = {}}};

        TopologyStatementHandler topologyHandler{nullptr, workers};
        SinkStatementHandler sinkHandler{sinks, RequireHostConfig{}};
        SourceStatementHandler sourceHandler{sources, RequireHostConfig{}};
        handleStatements(statements, topologyHandler, sinkHandler, sourceHandler);

        manager = std::make_unique<QueryManager>(
            workers, createEmbeddedBackend([](const WorkerConfig&) { return defaultConfiguration<SingleNodeWorkerConfiguration>(); }));
        const QueryOptimizer optimizer(
            defaultConfiguration<QueryOptimizerConfiguration>(), sources, sinks, workers, std::make_shared<ModelCatalog>());
        plan.emplace(optimizer.optimize(AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString("SELECT * FROM numbers INTO out")));
        for (const auto& host : {A, B, C})
        {
            ASSERT_EQ((*plan)[host].size(), 1) << "expected one local plan on " << host;
        }

        const auto started = manager->start(*plan);
        ASSERT_TRUE(started.has_value());
        query = *started;
        waitFor({{at(A), QueryStatus::Running}, {at(B), QueryStatus::Running}, {at(C), QueryStatus::Running}});
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

    [[nodiscard]] LocalQueryRef at(const Host& host, const std::optional<DistributedQueryId>& distributed = std::nullopt) const
    {
        const auto localQueries = manager->getQuery(distributed.value_or(query))->getLocalQueries();
        return LocalQueryRef{.host = host, .queryId = localQueries.at(host).front()};
    }

    [[nodiscard]] QueryStatus statusOf(const LocalQueryRef& ref) const
    {
        const auto status = manager->status(ref.queryId.getDistributedQueryId());
        EXPECT_TRUE(status.has_value());
        const auto& local = status->localStatusSnapshots.at(ref.host).at(ref.queryId);
        EXPECT_TRUE(local.has_value());
        if (local->metrics.error.has_value())
        {
            NES_ERROR("Local query {} on {} failed: {}", ref.queryId, ref.host, local->metrics.error->what());
        }
        return local->state;
    }

    /// Waits until every local query has reached its expected status.
    void waitFor(const std::unordered_map<LocalQueryRef, QueryStatus>& expected) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (std::ranges::all_of(expected, [&](const auto& entry) { return statusOf(entry.first) == entry.second; }))
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        for (const auto& [ref, status] : expected)
        {
            EXPECT_EQ(statusOf(ref), status) << "local query on " << ref.host;
        }
    }

    /// Starts a copy of c's local plan, whose network source listens on a new channel, as a new distributed query.
    std::pair<DistributedQueryId, std::string> startSuccessorOnC()
    {
        const auto channel = UUIDToString(generateUUID());
        const auto successor = Testing::withNewSourceChannel(
            (*plan)[C].front(), *sources, channel, QueryId::createDistributed(DistributedQueryId("successor")));
        const auto started = manager->start(DistributedLogicalPlan({{C, {successor}}}, successor));
        EXPECT_TRUE(started.has_value());
        return {*started, channel};
    }

    std::shared_ptr<SourceCatalog> sources = std::make_shared<SourceCatalog>();
    std::shared_ptr<SinkCatalog> sinks = std::make_shared<SinkCatalog>();
    std::shared_ptr<WorkerCatalog> workers = std::make_shared<WorkerCatalog>();
    std::unordered_map<Host, std::string> dataAddresses;
    std::unique_ptr<QueryManager> manager;
    std::optional<DistributedLogicalPlan> plan;
    DistributedQueryId query{DistributedQueryId::INVALID};
};

TEST_F(DrainIntegrationTest, DrainsTheWholeChain)
{
    const auto drainId = manager->drain(query, {at(A), at(B), at(C)});
    ASSERT_TRUE(drainId.has_value()) << drainId.error().front();

    waitFor({{at(A), QueryStatus::Drained}, {at(B), QueryStatus::Drained}, {at(C), QueryStatus::Drained}});
}

TEST_F(DrainIntegrationTest, BuffersUpstreamOfAMidChainDrainUntilItIsRedirected)
{
    const auto drainId = manager->drain(query, {at(B), at(C)});
    ASSERT_TRUE(drainId.has_value()) << drainId.error().front();
    waitFor({{at(B), QueryStatus::Drained}, {at(C), QueryStatus::Drained}});
    EXPECT_EQ(statusOf(at(A)), QueryStatus::Running);

    const auto [successor, channel] = startSuccessorOnC();
    waitFor({{at(C, successor), QueryStatus::Running}});
    const auto redirected = manager->redirectSink(at(A), Testing::networkSinkChannel((*plan)[A].front()), at(C, successor), channel);
    ASSERT_TRUE(redirected.has_value()) << redirected.error();

    /// The generator keeps sending to the successor; neither side fails.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(statusOf(at(A)), QueryStatus::Running);
    EXPECT_EQ(statusOf(at(C, successor)), QueryStatus::Running);
}

}
