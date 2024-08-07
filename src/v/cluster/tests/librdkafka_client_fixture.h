/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "librdkafka/rdkafkacpp.h"
#include "tests/cluster_test_fixture.h"

ss::logger logger("librdkafka_client_fixture");
class librdkafka_client_fixture : public cluster_test_fixture {
public:
    using kafka_producer_t = typename std::unique_ptr<RdKafka::Producer>;
    using kafka_topic_t = typename std::unique_ptr<RdKafka::Topic>;

    librdkafka_client_fixture()
      : cluster_test_fixture() {
        for (int id = 0; id < 1; id++) {
            create_node_application(model::node_id{id});
        }
        client_thread.start({.name = "librdkafka"}).get();
        populate_client_conf();
        producer = make_producer();
    }

    ~librdkafka_client_fixture() {
        client_thread.stop().get();
        client_conf.reset();
    }

    kafka_producer_t& kafka_producer() { return producer; }

    auto init_transactions(kafka_producer_t& producer, int timeout_ms) {
        return client_thread.submit([&producer, timeout_ms] {
            RdKafka::Error* err = nullptr;
            do {
                if (err) {
                    vlog(logger.warn, "Error in init transactions {}", err->str());
                    delete err;
                }
                err = producer->init_transactions(timeout_ms);
            } while (err != nullptr);
        });
    }

    auto begin_transaction(kafka_producer_t& producer) {
        return client_thread.submit(
          [&producer] { return producer->begin_transaction(); });
    }

    auto
    produce(kafka_producer_t& producer, const std::string& topic, int32_t) {
        return client_thread.submit([&producer, &topic] {
            auto err = RdKafka::ERR__QUEUE_FULL;
            do {
                err = producer->produce(
                  topic,
                  RdKafka::Topic::PARTITION_UA,
                  RdKafka::Producer::RK_MSG_COPY,
                  const_cast<char*>("payload"),
                  7,
                  nullptr,
                  0,
                  0,
                  nullptr,
                  nullptr);
                vlog(logger.info, "Got error from produce: {}", err);
                if (err != RdKafka::ERR_NO_ERROR) {
                    producer->poll(100);
                }
            } while (err != RdKafka::ERR_NO_ERROR);
        });
    }

    auto flush_until_success(kafka_producer_t& producer, int timeout_ms) {
        return client_thread.submit([&producer, timeout_ms] {
            auto err = RdKafka::ERR__STATE;
            do {
                err = producer->flush(timeout_ms);
            } while (err != RdKafka::ERR_NO_ERROR);
        });
    }

    auto commit_transaction(kafka_producer_t& producer, int timeout_ms) {
        return client_thread.submit([&producer, timeout_ms] {
            return producer->commit_transaction(timeout_ms);
        });
    }

private:
    void populate_client_conf() {
        client_conf = std::unique_ptr<RdKafka::Conf>(
          RdKafka::Conf::create((RdKafka::Conf::CONF_GLOBAL)));
        std::string err_msg;
        auto result = client_conf->set(
          "metadata.broker.list", get_kafka_broker_config(), err_msg);
        BOOST_REQUIRE_EQUAL(result, RdKafka::Conf::ConfResult::CONF_OK);
        client_conf->set("transactional.id", "test", err_msg);
        client_conf->set("enable.idempotence", "true", err_msg);
        client_conf->set("debug", "all", err_msg);
        BOOST_REQUIRE_EQUAL(result, RdKafka::Conf::ConfResult::CONF_OK);
    }

    std::unique_ptr<RdKafka::Producer> make_producer() const {
        std::string err_msg;
        return std::unique_ptr<RdKafka::Producer>(
          RdKafka::Producer::create(client_conf.get(), err_msg));
    }

    // Since librdkafka is not future friendly, all requests are routed
    // through a separate std::thread so the reactor is not blocked.
    ssx::singleton_thread_worker client_thread;
    std::unique_ptr<RdKafka::Conf> client_conf;
    std::unique_ptr<RdKafka::Producer> producer;
};
