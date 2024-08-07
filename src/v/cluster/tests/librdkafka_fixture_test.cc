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

#include "librdkafka_client_fixture.h"
#include "test_utils/async.h"

FIXTURE_TEST(test_basic_produce, librdkafka_client_fixture) {
    wait_for_all_members(10s).get();
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"test"}};
    create_topic(tp_ns);
    kafka_producer_t& producer = kafka_producer();
    init_transactions(producer, 100).get();
    vlog(logger.info, "init transactions successful");
    begin_transaction(producer).get();
    vlog(logger.info, "begin transactions successful");
    for (int j = 0; j < 20; j++) {
        for (int i = 0; i < 10; i++) {
            produce(producer, "test", 0).get();
        }
        flush_until_success(producer, 2000).get();
    }
    vlog(logger.info, "produce transactions successful");
    commit_transaction(producer, 100).get();
    vlog(logger.info, "commit transactions successful");
};
