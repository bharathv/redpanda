/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/replicate.h"

#include <string_view>

namespace cluster_link::replication {

enum replication_errc : int8_t {
    success = 0,
    misaligned_offset = 1,
    replication_error = 2,
    invalid_data = 3
};

static constexpr std::string_view to_string_view(replication_errc s) {
    switch (s) {
    case replication_errc::success:
        return "success";
    case replication_errc::misaligned_offset:
        return "misaligned_offset";
    case replication_errc::replication_error:
        return "replication_error";
    case replication_errc::invalid_data:
        return "invalid_data";
    }
}

class data_sink {
public:
    virtual ~data_sink() = default;

    virtual kafka::offset last_replicated_offset() const = 0;

    virtual ss::future<replication_errc> replicate(
      kafka::offset expected_offset,
      chunked_vector<model::record_batch> batches)
      = 0;
};

} // namespace cluster_link::replication

template<>
struct fmt::formatter<cluster_link::replication::replication_errc>
  : fmt::formatter<string_view> {
    auto format(
      cluster_link::replication::replication_errc s,
      format_context& ctx) -> decltype(ctx.out()) {
        return fmt::formatter<string_view>::format(to_string_view(s), ctx);
    }
};
