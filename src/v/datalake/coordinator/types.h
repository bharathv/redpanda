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

#include "container/fragmented_vector.h"
#include "datalake/errors.h"
#include "datalake/types.h"
#include "model/fundamental.h"

namespace datalake::coordinator {

struct translated_data_file_entry
  : serde::envelope<
      translated_data_file_entry,
      serde::version<0>,
      serde::compat_version<0>> {
    ::model::topic_partition tp;
    model::file_format file_type;
    ss::sstring file_path;
    // inclusive offset range
    ::model::offset begin_offset;
    ::model::offset end_offset;
    // term of the leader that performed this
    // translation
    ::model::term_id translator_term;

    auto serde_fields() {
        return std::tie(
          tp, file_type, file_path, begin_offset, end_offset, translator_term);
    }
};

struct add_translated_data_file_reply
  : serde::envelope<
      add_translated_data_file_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    add_translated_data_file_reply() = default;
    explicit add_translated_data_file_reply(coordinator_errc err)
      : errc(err) {}

    coordinator_errc errc;

    auto serde_fields() { return std::tie(errc); }
};
struct add_translated_data_files_request
  : serde::envelope<
      add_translated_data_files_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using resp_t = add_translated_data_file_reply;

    add_translated_data_files_request() = default;

    ::model::topic_partition tp;
    chunked_vector<translated_data_file_entry> files;

    const ::model::topic_partition& topic_partition() const { return tp; }

    auto serde_fields() { return std::tie(tp, files); }
};

struct fetch_latest_data_file_reply
  : serde::envelope<
      fetch_latest_data_file_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    fetch_latest_data_file_reply() = default;
    explicit fetch_latest_data_file_reply(coordinator_errc err)
      : errc(err) {}

    std::optional<translated_data_file_entry> entry;

    // If not ok, the request processing has a problem.
    coordinator_errc errc;

    auto serde_fields() { return std::tie(entry); }
};

struct fetch_latest_data_file_request
  : serde::envelope<
      fetch_latest_data_file_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using resp_t = fetch_latest_data_file_reply;

    fetch_latest_data_file_request() = default;

    ::model::topic_partition tp;

    const ::model::topic_partition& topic_partition() const { return tp; }

    auto serde_fields() { return std::tie(tp); }
};

} // namespace datalake::coordinator
