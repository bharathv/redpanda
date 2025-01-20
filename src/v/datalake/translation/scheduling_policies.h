/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/translation/scheduling_api.h"

namespace datalake::translation::scheduling {

class default_scheduling_policy : public scheduling_policy {
public:
    explicit default_scheduling_policy(
      config::binding<std::chrono::milliseconds> translator_time_quota);

    bool can_schedule_translators() final;

    ss::future<> scheduling_tick(scheduling_state&, ss::abort_source&) final;

private:
    static constexpr size_t MAX_CONCURRENT_TRANSLATIONS = 4;
    ssx::semaphore _max_concurrent_translations;
    config::binding<std::chrono::milliseconds> _translator_time_quota;
};

} // namespace datalake::translation::scheduling
