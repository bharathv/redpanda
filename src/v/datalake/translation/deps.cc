/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/deps.h"

namespace datalake::translation {

ss::future<> writer_reservations_impl::maybe_reserve_memory(size_t bytes) {
    while (_available_memory < bytes) {
        auto reservation = co_await _reservations_tracker.reserve_memory(_as);
        _available_memory += reservation.count();
        _total_reserved_memory += reservation.count();
        _reservations.push_back(std::move(reservation));
    }
    _available_memory -= bytes;
    co_return;
}

void writer_reservations_impl::update_current_memory_usage(size_t used_bytes) {
    vassert(
      used_bytes <= _total_reserved_memory,
      "Used more bytes {} than reserved {}",
      used_bytes,
      _total_reserved_memory);
    _available_memory = _total_reserved_memory - used_bytes;
}

void writer_reservations_impl::release() {
    _available_memory = 0;
    _total_reserved_memory = 0;
    _reservations.clear();
}

ss::future<> noop_mem_tracker::maybe_reserve_memory(size_t) {
    return ss::make_ready_future<>();
}
void noop_mem_tracker::update_current_memory_usage(size_t) {}
void noop_mem_tracker::release() {}

} // namespace datalake::translation
