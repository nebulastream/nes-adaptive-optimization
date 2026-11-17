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


#include <QueryDrainTracker.hpp>

#include <memory>
#include <string>
#include <unordered_set>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <DrainState.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{
namespace
{
const auto QUERY = QueryId::createLocal(LocalQueryId(generateUUID()));
const auto UNKNOWN_QUERY = QueryId::createLocal(LocalQueryId(generateUUID()));
constexpr auto DRAIN = DrainId(3);

/// A query with a local source (origin 1), two network sources and a network sink.
QueryChannels channels()
{
    return QueryChannels{
        .networkSources = {{"upstreamA", OriginId(2)}, {"upstreamB", OriginId(3)}}, .networkSinks = {"downstream"}, .numberOfSources = 3};
}

DrainRequest startHere()
{
    return DrainRequest{
        .drainId = DRAIN,
        .drainsQuery = true,
        .stopLocalSources = true,
        .markerChannels = {"upstreamA"},
        .sinkModes = {{"downstream", SinkDrainMode::Detach}}};
}

DrainRequest bufferOnly()
{
    return DrainRequest{
        .drainId = DRAIN,
        .drainsQuery = false,
        .stopLocalSources = false,
        .markerChannels = {},
        .sinkModes = {{"downstream", SinkDrainMode::BufferOnDetach}}};
}

class QueryDrainTrackerTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("QueryDrainTrackerTest.log", NES::LogLevel::LOG_DEBUG); }

    void SetUp() override { tracker.registerQuery(QUERY, channels(), drainState); }

    void expectInvalid(const DrainRequest& request)
    {
        try
        {
            tracker.configureDrain(QUERY, request);
            ADD_FAILURE() << "The request was accepted";
        }
        catch (const Exception& exception)
        {
            EXPECT_EQ(exception.code(), ErrorCode::InvalidDrainRequest) << exception.what();
        }
        EXPECT_EQ(drainState->getConfiguration(), std::nullopt) << "A rejected request must not configure the drain";
    }

    std::shared_ptr<DrainState> drainState = std::make_shared<DrainState>();
    QueryDrainTracker tracker;
};

TEST_F(QueryDrainTrackerTest, configuresTheDrainAndResolvesTheMarkerChannels)
{
    tracker.configureDrain(QUERY, startHere());
    EXPECT_EQ(
        drainState->getConfiguration(),
        (DrainConfiguration{.drainId = DRAIN, .drainsQuery = true, .stopLocalSources = true, .sinkModes = {{"downstream", SinkDrainMode::Detach}}}));
    EXPECT_EQ(tracker.sourcesEndingWithMarker(QUERY, DRAIN), (std::unordered_set{OriginId(2)}));
}

TEST_F(QueryDrainTrackerTest, rejectsRequestsForUnknownQueries)
{
    try
    {
        tracker.configureDrain(UNKNOWN_QUERY, startHere());
        ADD_FAILURE() << "The request was accepted";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::QueryNotFound);
    }
}

TEST_F(QueryDrainTrackerTest, rejectsChannelsTheQueryDoesNotHave)
{
    auto unknownSource = startHere();
    unknownSource.markerChannels = {"unknown"};
    expectInvalid(unknownSource);

    auto unknownSink = startHere();
    unknownSink.sinkModes = {{"unknown", SinkDrainMode::ForwardMarker}};
    expectInvalid(unknownSink);

    auto sinkAsSource = startHere();
    sinkAsSource.markerChannels = {"downstream"};
    expectInvalid(sinkAsSource);
}

TEST_F(QueryDrainTrackerTest, rejectsInconsistentRequests)
{
    auto invalidId = startHere();
    invalidId.drainId = INVALID_DRAIN_ID;
    expectInvalid(invalidId);

    auto duplicateMarker = startHere();
    duplicateMarker.markerChannels = {"upstreamA", "upstreamA"};
    expectInvalid(duplicateMarker);

    auto drainedQueryBuffers = startHere();
    drainedQueryBuffers.sinkModes = {{"downstream", SinkDrainMode::BufferOnDetach}};
    expectInvalid(drainedQueryBuffers);

    /// Without stopping its local sources, the query would never end, as only one of its three sources ends with a marker
    auto neverEnds = startHere();
    neverEnds.stopLocalSources = false;
    expectInvalid(neverEnds);
}

TEST_F(QueryDrainTrackerTest, rejectsQueriesWhichAreNotDrainedButDrainSourcesOrEndTheirSinks)
{
    auto stopsSources = bufferOnly();
    stopsSources.stopLocalSources = true;
    expectInvalid(stopsSources);

    auto expectsMarker = bufferOnly();
    expectsMarker.markerChannels = {"upstreamA"};
    expectInvalid(expectsMarker);

    auto forwardsMarker = bufferOnly();
    forwardsMarker.sinkModes = {{"downstream", SinkDrainMode::ForwardMarker}};
    expectInvalid(forwardsMarker);

    auto takesNoPart = bufferOnly();
    takesNoPart.sinkModes = {};
    expectInvalid(takesNoPart);
}

TEST_F(QueryDrainTrackerTest, aQueryWhoseSourcesAllEndWithAMarkerNeedNotStopItsSources)
{
    auto downstream = startHere();
    downstream.stopLocalSources = false;
    downstream.markerChannels = {"upstreamA", "upstreamB"};
    downstream.sinkModes = {};
    QueryDrainTracker networkOnly;
    const auto otherState = std::make_shared<DrainState>();
    networkOnly.registerQuery(
        QUERY,
        QueryChannels{.networkSources = channels().networkSources, .networkSinks = {}, .numberOfSources = 2},
        otherState);
    networkOnly.configureDrain(QUERY, downstream);
    EXPECT_TRUE(otherState->getConfiguration().has_value());
    /// The drain does not start there
    EXPECT_THROW((void)networkOnly.sourcesEndingWithMarker(QUERY, DRAIN), Exception);
}

TEST_F(QueryDrainTrackerTest, onlyOneDrainPerQuery)
{
    tracker.configureDrain(QUERY, startHere());
    auto second = startHere();
    second.drainId = DrainId(4);
    EXPECT_THROW(tracker.configureDrain(QUERY, second), Exception);
    EXPECT_EQ(drainState->getConfiguration()->drainId, DRAIN);
}

TEST_F(QueryDrainTrackerTest, startRequiresTheConfiguredDrain)
{
    EXPECT_THROW((void)tracker.sourcesEndingWithMarker(QUERY, DRAIN), Exception);
    tracker.configureDrain(QUERY, startHere());
    EXPECT_THROW((void)tracker.sourcesEndingWithMarker(QUERY, DrainId(4)), Exception);
}

TEST_F(QueryDrainTrackerTest, redirectsOnlyBufferingSinks)
{
    EXPECT_THROW(tracker.redirectSink(QUERY, "downstream", RedirectTarget{.connection = "host:1", .channel = "new"}), Exception);
    tracker.configureDrain(QUERY, bufferOnly());
    EXPECT_THROW(tracker.redirectSink(QUERY, "downstream", RedirectTarget{.connection = "", .channel = "new"}), Exception);
    EXPECT_THROW(tracker.redirectSink(QUERY, "unknown", RedirectTarget{.connection = "host:1", .channel = "new"}), Exception);

    tracker.redirectSink(QUERY, "downstream", RedirectTarget{.connection = "host:1", .channel = "new"});
    EXPECT_EQ(drainState->getRedirect("downstream"), (RedirectTarget{.connection = "host:1", .channel = "new"}));
}

}
}
