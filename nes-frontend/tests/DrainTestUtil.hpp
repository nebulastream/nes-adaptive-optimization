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

#include <algorithm>
#include <any>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/DrainPlanner.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/NetworkSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/NetworkSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES::Testing
{

/// The channel of the network sink of a local plan.
inline std::string networkSinkChannel(const LogicalPlan& plan)
{
    for (const auto& sink : getOperatorByType<SinkLogicalOperator>(plan))
    {
        if (const auto descriptor = sink->getSinkDescriptor())
        {
            if (const auto* config = std::any_cast<NetworkSinkConfig>(&descriptor->getPluginData().getUnderlying()))
            {
                return config->channel;
            }
        }
    }
    throw TestException("the plan has no network sink");
}

/// Replaces the network source of `op`'s subtree by `replacement`.
inline LogicalOperator replaceNetworkSource(const LogicalOperator& op, const LogicalOperator& replacement)
{
    if (const auto source = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        const auto descriptor = source.value()->getSourceDescriptor();
        if (std::any_cast<NetworkSourceConfig>(&descriptor.getPluginData().getUnderlying()) != nullptr)
        {
            return replacement;
        }
        return op;
    }
    std::vector<LogicalOperator> children;
    for (const auto& child : op.getChildren())
    {
        children.push_back(replaceNetworkSource(child, replacement));
    }
    return op.withChildrenUnsafe(std::move(children));
}

/// A copy of `original` whose network source listens on the new channel `channel`.
inline LogicalPlan
withNewSourceChannel(const LogicalPlan& original, const SourceCatalog& sourceCatalog, const std::string& channel, const QueryId& queryId)
{
    const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(original);
    const auto isNetworkSource = [](const auto& source)
    { return std::any_cast<NetworkSourceConfig>(&source->getSourceDescriptor().getPluginData().getUnderlying()) != nullptr; };
    EXPECT_EQ(std::ranges::count_if(sources, isNetworkSource), 1);
    const auto& oldSource = *std::ranges::find_if(sources, isNetworkSource);
    const auto oldDescriptor = oldSource->getSourceDescriptor();
    const auto& oldConfig = std::any_cast<const NetworkSourceConfig&>(oldDescriptor.getPluginData().getUnderlying());

    const auto descriptor = sourceCatalog.getAnonymousSource(
        Identifier::parse("Network"),
        oldDescriptor.getSchema(),
        oldDescriptor.getHost(),
        {{Identifier::parse("type"), "NATIVE"}},
        {{Identifier::parse("channel"), channel}, {Identifier::parse("bind"), oldConfig.bind}});
    EXPECT_TRUE(descriptor.has_value());
    const auto newSource = SourceDescriptorLogicalOperator::create(*descriptor).withTraitSet(oldSource.getTraitSet());

    std::vector<LogicalOperator> roots;
    for (const auto& root : original.getRootOperators())
    {
        roots.push_back(replaceNetworkSource(root, newSource));
    }
    return LogicalPlan(queryId, roots);
}

inline LocalQueryRef localQuery(const std::string& host)
{
    return LocalQueryRef{.host = Host(host), .queryId = QueryId::createLocal(LocalQueryId(generateUUID()))};
}

/// The bind address of the network source with the given channel name.
inline std::string bindOf(const std::string& channel)
{
    return channel + "-receiver:9090";
}

/// Builds local plans out of network sources and sinks, like the query decomposition does, so that tests control the topology.
/// Network channels need UUIDs as ids, so tests name them and look up their ids with `id`.
class LocalPlanBuilder
{
public:
    const std::string& id(const std::string& channel)
    {
        return channelIds.try_emplace(channel, UUIDToString(generateUUID())).first->second;
    }

    [[nodiscard]] LogicalOperator localSource() const
    {
        const auto descriptor = sourceCatalog.getAnonymousSource(
            Identifier::parse("File"),
            schema,
            Host("worker"),
            {{Identifier::parse("type"), "CSV"}},
            {{Identifier::parse("file_path"), "does_not_exist"}});
        EXPECT_TRUE(descriptor.has_value());
        return SourceDescriptorLogicalOperator::create(*descriptor);
    }

    [[nodiscard]] LogicalOperator networkSource(const std::string& channel)
    {
        const auto descriptor = sourceCatalog.getAnonymousSource(
            Identifier::parse("Network"),
            schema,
            Host("worker"),
            {{Identifier::parse("type"), "NATIVE"}},
            {{Identifier::parse("channel"), id(channel)}, {Identifier::parse("bind"), bindOf(channel)}});
        EXPECT_TRUE(descriptor.has_value());
        return SourceDescriptorLogicalOperator::create(*descriptor);
    }

    /// The sinks get their children without schema inference, as the window operator of `window` has no schema.
    [[nodiscard]] LogicalOperator networkSink(const LogicalOperator& child, const std::string& channel)
    {
        const auto descriptor = sinkCatalog.getAnonymousSink(
            schema,
            Identifier::parse("Network"),
            Host("worker"),
            {{Identifier::parse("channel"), id(channel)},
             {Identifier::parse("bind"), "sender:9090"},
             {Identifier::parse("data_endpoint"), bindOf(channel)},
             {Identifier::parse("output_format"), "NATIVE"}},
            {});
        EXPECT_TRUE(descriptor.has_value());
        return SinkLogicalOperator::create(*descriptor).withChildrenUnsafe({child});
    }

    [[nodiscard]] LogicalOperator localSink(const LogicalOperator& child) const
    {
        const auto descriptor = sinkCatalog.getAnonymousSink(schema, Identifier::parse("Void"), Host("worker"), {}, {});
        EXPECT_TRUE(descriptor.has_value());
        return SinkLogicalOperator::create(*descriptor).withChildrenUnsafe({child});
    }

    [[nodiscard]] static LogicalOperator window(const LogicalOperator& child)
    {
        const auto parsed = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(
            "SELECT SUM(a) AS s FROM src WINDOW TUMBLING(a, size 1 sec) INTO snk");
        const auto windows = getOperatorByType<WindowedAggregationLogicalOperator>(parsed);
        EXPECT_EQ(windows.size(), 1);
        return windows.front().withChildrenUnsafe({child});
    }

    static LogicalPlan plan(const LogicalOperator& root, const QueryId& queryId = QueryId::createLocal(LocalQueryId(generateUUID())))
    {
        return LogicalPlan(queryId, {root});
    }

private:
    std::unordered_map<std::string, std::string> channelIds;
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{Identifier::parse("a"), DataType::Type::UINT64}};
};

}
