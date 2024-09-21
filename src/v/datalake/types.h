/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

namespace datalake::model {

enum file_format : int8_t { PARQUET, AVRO, JSON };

struct translation_metadata_key
  : serde::envelope<
      translation_metadata_key,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct translation_metadata_value
  : serde::envelope<
      translation_metadata_value,
      serde::version<0>,
      serde::compat_version<0>> {
    // Everything in range (highest_translated_offset, max_translatable_offset]
    // is translatable.
    ::model::offset highest_translated_offset;
    // Translation is attempted periodically based on a topic property that
    // controls the interval. It may happen that the translation is blocked (eg:
    // waiting for resources) and may surpass the interval boundary. Here we
    // track the max translatable offset so the subsequent translation attempt
    // can catchup until this offset.
    ::model::offset max_translatable_offset;

    auto serde_fields() {
        return std::tie(highest_translated_offset, max_translatable_offset);
    }
};
}; // namespace datalake::model
