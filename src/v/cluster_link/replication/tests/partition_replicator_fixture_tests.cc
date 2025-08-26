/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/deps_impl.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/replication/partition_replicator.h"
#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"

using namespace cluster_link::replication;
using BasicConsumerFixture = kafka::client::tests::basic_consumer_fixture;

static constexpr std::chrono::milliseconds fetch_max_wait{10};
static constexpr size_t partition_max_buffered{1024 * 1024}; // 1MB

class ReplicatorFixture : public BasicConsumerFixture {
public:
    void SetUp() override {
        basic_consumer_fixture::SetUp();
        auto consumer = make_consumer();
        _mux_consumer = std::make_unique<mux_remote_consumer>(
          make_consumer(), partition_max_buffered, fetch_max_wait);
        _mux_consumer->start().get();
        // create source and target topics;
        create_topic(model::topic_namespace_view{_source}).get();
        create_topic(model::topic_namespace_view{_target}).get();
        // setup the replicator
        auto [_, partition] = get_leader(_target);
        vassert(partition, "no partition for {}", _target);

        auto source = std::make_unique<remote_partition_source>(
          _source.tp, *_mux_consumer);
        auto sink = std::make_unique<local_partition_sink>(partition);
        _replicator = std::make_unique<partition_replicator>(
          _source, model::term_id{0}, std::move(source), std::move(sink));
        _replicator->start().get();
    }

    void TearDown() override {
        if (_replicator) {
            _replicator->stop().get();
        }
        if (_mux_consumer) {
            _mux_consumer->stop().get();
        }
        basic_consumer_fixture::TearDown();
    }

    // Do nothing, handled by mux consumer
    void StartConsumer() override {}
    void StopConsumer() override {}

protected:
    std::unique_ptr<mux_remote_consumer> _mux_consumer;
    std::unique_ptr<partition_replicator> _replicator;
    model::ntp _source{model::kafka_namespace, "source", 0};
    model::ntp _target{model::kafka_namespace, "target", 0};
};

TEST_P(ReplicatorFixture, TestProduceConsume) { ss::sleep(1s).get(); }

using session_config = kafka::client::tests::session_config;
INSTANTIATE_TEST_SUITE_P(
  ReplicatorFixtureAndSessions,
  ReplicatorFixture,
  testing::Values(
    session_config::with_sessions,
    session_config::without_sessions,
    session_config::toggle_sessions));
