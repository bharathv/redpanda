/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/datalake_usage_api.h"

#include "utils/to_string.h"

namespace kafka {

datalake_usage_api::usage_stats::usage_stats(const usage_stats& other) {
    if (other.topic_stats) {
        topic_stats = other.topic_stats->copy();
    } else {
        topic_stats.reset();
    }
    missing_reason = other.missing_reason;
}

datalake_usage_api::usage_stats& datalake_usage_api::usage_stats::operator=(
  const datalake_usage_api::usage_stats& other) {
    if (this != &other) {
        if (other.topic_stats) {
            topic_stats = other.topic_stats->copy();
        } else {
            topic_stats.reset();
        }
        missing_reason = other.missing_reason;
    }
    return *this;
}

std::ostream& operator<<(
  std::ostream& os, const datalake_usage_api::stats_missing_reason& r) {
    switch (r) {
    case datalake_usage_api::stats_missing_reason::none:
        return os << "none";
    case datalake_usage_api::stats_missing_reason::feature_disabled:
        return os << "feature_disabled";
    case datalake_usage_api::stats_missing_reason::collection_error:
        return os << "collection_error";
    case datalake_usage_api::stats_missing_reason::not_controller_leader:
        return os << "not_controller_leader";
    }
}

std::ostream&
operator<<(std::ostream& os, const datalake_usage_api::topic_usage& u) {
    fmt::print(
      os,
      "{{ topic: {} revision: {} kafka_bytes_processed: {} }}",
      u.topic,
      u.revision,
      u.kafka_bytes_processed);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const datalake_usage_api::usage_stats& u) {
    fmt::print(
      os,
      "{{ topic_stats: {}, missing_stats_reason: {} }}",
      u.topic_stats,
      u.missing_reason);
    return os;
}

} // namespace kafka
