/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/scheduling_policies.h"

#include "datalake/logger.h"
#include "random/generators.h"

#include <seastar/core/sleep.hh>

using namespace std::chrono_literals;

static constexpr auto polling_interval = 1s;

namespace datalake::translation::scheduling {
simple_fcfs_scheduling_policy::simple_fcfs_scheduling_policy(
  size_t max_concurrent_translators, clock::duration translation_time_quota)
  : _max_concurrent_translations(max_concurrent_translators)
  , _translation_time_quota(translation_time_quota) {
    vlog(
      datalake_log.info,
      "created simple_fcfs_scheduling_policy policy with {} translators "
      "and {} time quota",
      max_concurrent_translators,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        translation_time_quota));
}

ss::future<> simple_fcfs_scheduling_policy::schedule_one_translation(
  executor& state, const reservations_tracker& mem_tracker) {
    // check the # of running translators
    while (!state.as.abort_requested() && !state.waiting.empty()
           && !mem_tracker.memory_exhausted()
           && state.running.size() >= _max_concurrent_translations) {
        co_await ss::sleep_abortable(polling_interval, state.as);
    }
    if (state.as.abort_requested() || mem_tracker.memory_exhausted()) {
        co_return;
    }
    // pick the first queued translator.
    if (state.waiting.empty()) {
        co_return;
    }
    state.start_translation(*state.waiting.begin(), _translation_time_quota);
}

ss::future<> simple_fcfs_scheduling_policy::on_resource_exhaustion(
  executor& state, const reservations_tracker& mem_tracker) {
    while (mem_tracker.memory_exhausted() && !state.as.abort_requested()) {
        // pick the earliest scheduled translator and force a flush.
        if (!state.running.empty()) {
            state.stop_translation(*state.running.begin());
        }
        co_await ss::sleep_abortable(5s, state.as);
    }
}

} // namespace datalake::translation::scheduling
