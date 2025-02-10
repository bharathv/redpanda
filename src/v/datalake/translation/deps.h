/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/data_writer_interface.h"
#include "datalake/translation/scheduling.h"

namespace datalake::translation {

class writer_reservations_impl : public writer_mem_tracker {
public:
    explicit writer_reservations_impl(
      scheduling::reservations_tracker& scheduling_reservations,
      ss::abort_source& as)
      : _reservations_tracker(scheduling_reservations)
      , _as(as) {}

    ss::future<> maybe_reserve_memory(size_t bytes) override;
    void update_current_memory_usage(size_t) override;
    void release() override;

private:
    size_t _available_memory{0};
    size_t _total_reserved_memory{0};
    scheduling::reservations_tracker& _reservations_tracker;
    ss::abort_source& _as;
    chunked_vector<ssx::semaphore_units> _reservations;
};

class noop_mem_tracker : public writer_mem_tracker {
public:
    ss::future<> maybe_reserve_memory(size_t bytes) override;
    void update_current_memory_usage(size_t) override;
    void release() override;
};

} // namespace datalake::translation
