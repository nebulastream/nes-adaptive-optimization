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
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/DrainPlanner.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <DrainRequest.hpp>
#include <DrainTestUtil.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SinkDrain.hpp>
#include <Version.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
namespace
{
/// What the fake backends were asked to do, in order.
struct Recorder
{
    std::vector<std::string> calls;
    /// The hosts whose workers reject the drain request.
    std::unordered_set<std::string> rejectDrain;
    std::vector<std::pair<std::string, RedirectTarget>> redirects;
};

class FakeBackend final : public QuerySubmissionBackend
{
public:
    FakeBackend(std::string host, std::shared_ptr<Recorder> recorder) : host(std::move(host)), recorder(std::move(recorder)) { }

    std::expected<QueryId, Exception> start(LogicalPlan plan) override
    {
        return QueryId::create(LocalQueryId(generateUUID()), plan.getQueryId().getDistributedQueryId());
    }

    std::expected<void, Exception> stop(QueryId) override
    {
        recorder->calls.push_back("stop " + host);
        return {};
    }

    std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId queryId) const override
    {
        return LocalQueryStatusSnapshot{.queryId = queryId, .state = QueryStatus::Running, .metrics = {}};
    }

    std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point) const override { return WorkerStatus{}; }

    std::expected<VersionInfo, Exception> version() const override { return VersionInfo{"fake"}; }

    std::expected<void, Exception> drain(QueryId, const DrainRequest&) override
    {
        recorder->calls.push_back("drain " + host);
        if (recorder->rejectDrain.contains(host))
        {
            return std::unexpected(InvalidDrainRequest("rejected by the test"));
        }
        return {};
    }

    std::expected<void, Exception> startDrain(QueryId, DrainId) override
    {
        recorder->calls.push_back("start " + host);
        return {};
    }

    std::expected<void, Exception> redirectSink(QueryId, const std::string& channel, const RedirectTarget& target) override
    {
        recorder->calls.push_back("redirect " + host);
        recorder->redirects.emplace_back(channel, target);
        return {};
    }

private:
    std::string host;
    std::shared_ptr<Recorder> recorder;
};
}

class QueryManagerDrainTest : public Testing::BaseUnitTest, protected Testing::LocalPlanBuilder
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("QueryManagerDrainTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        auto workers = std::make_shared<WorkerCatalog>();
        for (const auto* host : {"a", "b", "c", "target"})
        {
            workers->addWorker(Host(host), std::string(host) + ":9090", CapacityKind::Unlimited{}, {});
        }
        manager = std::make_unique<QueryManager>(
            workers,
            [recorder = recorder](const WorkerConfig& config) -> UniquePtr<QuerySubmissionBackend>
            { return std::make_unique<FakeBackend>(config.host.getRawValue(), recorder); });
    }

    /// Starts a distributed query with one local query per host.
    DistributedQueryId start(const std::string& name, std::unordered_map<std::string, LogicalOperator> roots)
    {
        const auto queryId = QueryId::createDistributed(DistributedQueryId(name));
        std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
        for (const auto& [host, root] : roots)
        {
            localPlans[Host(host)].push_back(plan(root, queryId));
        }
        const auto globalPlan = plan(roots.begin()->second, queryId);
        const auto started = manager->start(DistributedLogicalPlan(std::move(localPlans), globalPlan));
        EXPECT_TRUE(started.has_value());
        return *started;
    }

    /// The chain a → b → c over the channels ab and bc.
    DistributedQueryId startChain()
    {
        return start(
            "chain",
            {{"a", networkSink(localSource(), "ab")},
             {"b", networkSink(networkSource("ab"), "bc")},
             {"c", localSink(networkSource("bc"))}});
    }

    LocalQueryRef localQueryOn(const DistributedQueryId& query, const std::string& host) const
    {
        const auto localQueries = manager->getQuery(query)->getLocalQueries();
        return LocalQueryRef{.host = Host(host), .queryId = localQueries.at(Host(host)).front()};
    }

    [[nodiscard]] size_t indexOf(const std::string& call) const
    {
        return static_cast<size_t>(std::distance(recorder->calls.begin(), std::ranges::find(recorder->calls, call)));
    }

    std::shared_ptr<Recorder> recorder = std::make_shared<Recorder>();
    std::unique_ptr<QueryManager> manager;
};

TEST_F(QueryManagerDrainTest, ConfiguresEveryInvolvedQueryBeforeStartingTheDrain)
{
    const auto query = startChain();
    const auto drainId = manager->drain(query, {localQueryOn(query, "b"), localQueryOn(query, "c")});
    ASSERT_TRUE(drainId.has_value());
    EXPECT_NE(*drainId, INVALID_DRAIN_ID);

    ASSERT_EQ(recorder->calls.size(), 4);
    EXPECT_EQ(recorder->calls.back(), "start b");
    for (const auto* host : {"a", "b", "c"})
    {
        EXPECT_LT(indexOf(std::string("drain ") + host), indexOf("start b"));
    }
}

TEST_F(QueryManagerDrainTest, StopsTheDrainedQueriesIfAQueryRejectsTheDrain)
{
    recorder->rejectDrain = {"c"};
    const auto query = startChain();
    const auto drainId = manager->drain(query, {localQueryOn(query, "b"), localQueryOn(query, "c")});
    ASSERT_FALSE(drainId.has_value());
    ASSERT_EQ(drainId.error().size(), 1);
    EXPECT_EQ(drainId.error().front().code(), ErrorCode::InvalidDrainRequest);

    EXPECT_EQ(indexOf("start b"), recorder->calls.size());
    EXPECT_LT(indexOf("stop b"), recorder->calls.size());
    EXPECT_LT(indexOf("stop c"), recorder->calls.size());
    EXPECT_EQ(indexOf("stop a"), recorder->calls.size());
}

TEST_F(QueryManagerDrainTest, RejectsADrainWhichDoesNotFitTheQuery)
{
    const auto query = startChain();
    const auto drainId = manager->drain(query, {});
    ASSERT_FALSE(drainId.has_value());
    EXPECT_EQ(drainId.error().front().code(), ErrorCode::InvalidDrain);
    EXPECT_TRUE(recorder->calls.empty());
}

TEST_F(QueryManagerDrainTest, CannotDrainAQueryWhosePlansAreUnknown)
{
    const auto local = QueryId::create(LocalQueryId(generateUUID()), DistributedQueryId("restored"));
    QueryManager restored(
        std::make_shared<WorkerCatalog>(),
        [recorder = recorder](const WorkerConfig& config) -> UniquePtr<QuerySubmissionBackend>
        { return std::make_unique<FakeBackend>(config.host.getRawValue(), recorder); },
        QueryManagerState{.queries = {{DistributedQueryId("restored"), DistributedQuery({{Host("a"), {local}}})}}, .localPlans = {}});

    const auto drainId = restored.drain(DistributedQueryId("restored"), {LocalQueryRef{.host = Host("a"), .queryId = local}});
    ASSERT_FALSE(drainId.has_value());
    EXPECT_EQ(drainId.error().front().code(), ErrorCode::InvalidDrain);
}

TEST_F(QueryManagerDrainTest, RedirectsASinkToASourceOfAnotherQuery)
{
    const auto query = startChain();
    const auto successor = start("successor", {{"target", localSink(networkSource("new"))}});

    const auto result = manager->redirectSink(localQueryOn(query, "a"), id("ab"), localQueryOn(successor, "target"), id("new"));
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(recorder->calls, std::vector<std::string>{"redirect a"});
    ASSERT_EQ(recorder->redirects.size(), 1);
    EXPECT_EQ(recorder->redirects.front().first, id("ab"));
    EXPECT_EQ(recorder->redirects.front().second, (RedirectTarget{.connection = Testing::bindOf("new"), .channel = id("new")}));
}

}
