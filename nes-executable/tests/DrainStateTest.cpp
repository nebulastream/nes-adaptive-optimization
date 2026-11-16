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

#include <DrainState.hpp>

#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{
class DrainStateTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("DrainStateTest.log", NES::LogLevel::LOG_DEBUG); }
};

DrainConfiguration configuration(const uint64_t drainId)
{
    return DrainConfiguration{
        .drainId = DrainId(drainId),
        .stopLocalSources = false,
        .expectedUpstreamMarkers = 2,
        .sinkModes = {{"forward", SinkDrainMode::ForwardMarker}, {"detach", SinkDrainMode::Detach}}};
}

TEST_F(DrainStateTest, isNotDrainingUntilConfigured)
{
    const DrainState state;
    EXPECT_EQ(state.getConfiguration(), std::nullopt);
    EXPECT_EQ(state.getSinkMode("forward"), std::nullopt);
    EXPECT_EQ(state.getRedirect("forward"), std::nullopt);
}

TEST_F(DrainStateTest, canOnlyBeConfiguredOnce)
{
    DrainState state;
    ASSERT_TRUE(state.configure(configuration(1)));
    EXPECT_FALSE(state.configure(configuration(2)));
    EXPECT_EQ(state.getConfiguration(), configuration(1));
}

TEST_F(DrainStateTest, returnsTheModeOfConfiguredSinksOnly)
{
    DrainState state;
    ASSERT_TRUE(state.configure(configuration(1)));
    EXPECT_EQ(state.getSinkMode("forward"), SinkDrainMode::ForwardMarker);
    EXPECT_EQ(state.getSinkMode("detach"), SinkDrainMode::Detach);
    EXPECT_EQ(state.getSinkMode("unknown"), std::nullopt);
}

TEST_F(DrainStateTest, laterRedirectReplacesEarlierOne)
{
    DrainState state;
    state.setRedirect("channel", RedirectTarget{.connection = "a:1", .channel = "first"});
    state.setRedirect("channel", RedirectTarget{.connection = "b:2", .channel = "second"});
    EXPECT_EQ(state.getRedirect("channel"), (RedirectTarget{.connection = "b:2", .channel = "second"}));
    EXPECT_EQ(state.getRedirect("other"), std::nullopt);
}

/// The worker configures the drain while worker threads read it.
TEST_F(DrainStateTest, concurrentReadersSeeEitherNoneOrTheFullConfiguration)
{
    DrainState state;
    std::vector<std::jthread> readers;
    readers.reserve(4);
    for (size_t i = 0; i < 4; ++i)
    {
        readers.emplace_back(
            [&state]
            {
                for (size_t iteration = 0; iteration < 10000; ++iteration)
                {
                    if (const auto read = state.getConfiguration())
                    {
                        EXPECT_EQ(*read, configuration(1));
                    }
                }
            });
    }
    ASSERT_TRUE(state.configure(configuration(1)));
}

}
}
