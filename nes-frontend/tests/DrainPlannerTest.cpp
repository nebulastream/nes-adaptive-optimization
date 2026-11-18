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

#include <QueryManager/DrainPlanner.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DrainTestUtil.hpp>
#include <DrainRequest.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SinkDrain.hpp>

namespace NES
{
namespace
{
constexpr auto DRAIN = DrainId(5);

std::vector<std::string> sorted(std::vector<std::string> values)
{
    std::ranges::sort(values);
    return values;
}
}

class DrainPlannerTest : public Testing::BaseUnitTest, protected Testing::LocalPlanBuilder
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("DrainPlannerTest.log", LogLevel::LOG_DEBUG); }

    /// The chain a → b → c over the channels ab and bc. `cHasWindow` puts a window operator into c.
    void chain(const bool cHasWindow = false)
    {
        localQueries.emplace(a, plan(networkSink(localSource(), "ab")));
        localQueries.emplace(b, plan(networkSink(networkSource("ab"), "bc")));
        localQueries.emplace(c, plan(localSink(cHasWindow ? window(networkSource("bc")) : networkSource("bc"))));
    }

    LocalQueryRef a = Testing::localQuery("a");
    LocalQueryRef b = Testing::localQuery("b");
    LocalQueryRef c = Testing::localQuery("c");
    std::unordered_map<LocalQueryRef, LogicalPlan> localQueries;
};

TEST_F(DrainPlannerTest, DrainsAWholeChainWithMarkers)
{
    chain();
    const auto drain = planDrain(DRAIN, localQueries, {a, b, c});

    EXPECT_EQ(drain.starts, std::vector{a});
    ASSERT_EQ(drain.requests.size(), 3);
    EXPECT_EQ(
        drain.requests.at(a),
        (DrainRequest{
            .drainId = DRAIN, .drainsQuery = true, .stopLocalSources = true, .markerChannels = {}, .sinkModes = {{id("ab"), SinkDrainMode::ForwardMarker}}}));
    EXPECT_EQ(
        drain.requests.at(b),
        (DrainRequest{
            .drainId = DRAIN,
            .drainsQuery = true,
            .stopLocalSources = false,
            .markerChannels = {id("ab")},
            .sinkModes = {{id("bc"), SinkDrainMode::ForwardMarker}}}));
    EXPECT_EQ(
        drain.requests.at(c),
        (DrainRequest{.drainId = DRAIN, .drainsQuery = true, .stopLocalSources = false, .markerChannels = {id("bc")}, .sinkModes = {}}));
}

TEST_F(DrainPlannerTest, DetachesFromARunningDownstreamQuery)
{
    chain();
    const auto drain = planDrain(DRAIN, localQueries, {a});

    EXPECT_EQ(drain.starts, std::vector{a});
    ASSERT_EQ(drain.requests.size(), 1);
    EXPECT_EQ(drain.requests.at(a).sinkModes, (std::unordered_map<std::string, SinkDrainMode>{{id("ab"), SinkDrainMode::Detach}}));
}

TEST_F(DrainPlannerTest, LetsARunningUpstreamQueryBufferIfTheDrainStartsMidChain)
{
    chain();
    const auto drain = planDrain(DRAIN, localQueries, {b, c});

    EXPECT_EQ(drain.starts, std::vector{b});
    ASSERT_EQ(drain.requests.size(), 3);
    EXPECT_EQ(
        drain.requests.at(a),
        (DrainRequest{
            .drainId = DRAIN,
            .drainsQuery = false,
            .stopLocalSources = false,
            .markerChannels = {},
            .sinkModes = {{id("ab"), SinkDrainMode::BufferOnDetach}}}));
    EXPECT_TRUE(drain.requests.at(b).stopLocalSources);
    EXPECT_TRUE(drain.requests.at(b).markerChannels.empty());
    EXPECT_EQ(drain.requests.at(c).markerChannels, std::vector<std::string>{id("bc")});
}

TEST_F(DrainPlannerTest, DrainsAMergeOfADrainedAndARunningUpstreamQuery)
{
    const auto other = Testing::localQuery("other");
    localQueries.emplace(a, plan(networkSink(localSource(), "ab")));
    localQueries.emplace(other, plan(networkSink(localSource(), "ob")));
    localQueries.emplace(b, plan(localSink(UnionLogicalOperator::create({networkSource("ab"), networkSource("ob")}))));

    const auto drain = planDrain(DRAIN, localQueries, {a, b});

    const auto startHosts = drain.starts | std::views::transform([](const auto& ref) { return ref.host.getRawValue(); })
        | std::ranges::to<std::vector<std::string>>();
    EXPECT_EQ(sorted(startHosts), (std::vector<std::string>{"a", "b"}));
    EXPECT_TRUE(drain.requests.at(b).stopLocalSources);
    EXPECT_EQ(drain.requests.at(b).markerChannels, std::vector<std::string>{id("ab")});
    EXPECT_EQ(drain.requests.at(other).sinkModes, (std::unordered_map<std::string, SinkDrainMode>{{id("ob"), SinkDrainMode::BufferOnDetach}}));
    EXPECT_FALSE(drain.requests.at(other).drainsQuery);
}

TEST_F(DrainPlannerTest, RejectsADetachUpstreamOfAWindow)
{
    chain(true);
    EXPECT_THROW((void)planDrain(DRAIN, localQueries, {a}), Exception);
    EXPECT_THROW((void)planDrain(DRAIN, localQueries, {b}), Exception);
    /// The window is drained itself, so it flushes instead of seeing the detached channel again.
    EXPECT_NO_THROW((void)planDrain(DRAIN, localQueries, {b, c}));
}

TEST_F(DrainPlannerTest, RejectsDrainsWhichDoNotFitTheLocalQueries)
{
    chain();
    EXPECT_THROW((void)planDrain(DRAIN, localQueries, {}), Exception);
    EXPECT_THROW((void)planDrain(DRAIN, localQueries, {Testing::localQuery("unknown")}), Exception);

    /// The receiver of b's sink is not part of the distributed query.
    localQueries.erase(c);
    EXPECT_THROW((void)planDrain(DRAIN, localQueries, {b}), Exception);
}

TEST_F(DrainPlannerTest, RedirectsToTheBindAddressOfTheTargetSource)
{
    chain();
    const auto target = Testing::localQuery("target");
    localQueries.emplace(target, plan(localSink(networkSource("new"))));

    EXPECT_EQ(planRedirect(localQueries, a, id("ab"), target, id("new")), (RedirectTarget{.connection = Testing::bindOf("new"), .channel = id("new")}));
    EXPECT_THROW((void)planRedirect(localQueries, a, id("unknown"), target, id("new")), Exception);
    EXPECT_THROW((void)planRedirect(localQueries, a, id("ab"), target, id("unknown")), Exception);
    EXPECT_THROW((void)planRedirect(localQueries, Testing::localQuery("unknown"), id("ab"), target, id("new")), Exception);
}

TEST_F(DrainPlannerTest, RejectsARedirectUpstreamOfAWindow)
{
    chain();
    const auto target = Testing::localQuery("target");
    const auto downstream = Testing::localQuery("downstream");
    localQueries.emplace(target, plan(networkSink(networkSource("new"), "td")));
    localQueries.emplace(downstream, plan(localSink(window(networkSource("td")))));

    EXPECT_THROW((void)planRedirect(localQueries, a, id("ab"), target, id("new")), Exception);
}

}
