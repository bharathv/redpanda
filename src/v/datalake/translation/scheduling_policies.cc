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

namespace datalake::translation::scheduling {

default_scheduling_policy::default_scheduling_policy(
  config::binding<std::chrono::milliseconds> translator_time_quota)
  : _max_concurrent_translations(
      MAX_CONCURRENT_TRANSLATIONS, "dl/translation/translators")
  , _translator_time_quota(std::move(translator_time_quota)) {}

bool default_scheduling_policy::can_schedule_translators() {
    return _max_concurrent_translations.available_units() > 0;
}

ss::future<> default_scheduling_policy::scheduling_tick(
  scheduling_state& state, ss::abort_source& as) {
    as.check();
    // Check memory usage and if there is a deadlock.
    if (state.memory_exhausted()) {
        // no translator can make progress, for each translator
        // check if it can be flushed.
    }

    // Check if there are any translators that exceeded timeout

    // Check if new translations can be scheduled freely
}

} // namespace datalake::translation::scheduling
