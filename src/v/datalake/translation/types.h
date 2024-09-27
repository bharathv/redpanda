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

#include "model/fundamental.h"
#include "serde/envelope.h"

namespace datalake::translation {

struct translation_state_key
  : serde::envelope<
      translation_state_key,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct translation_state_value
  : serde::envelope<
      translation_state_value,
      serde::version<0>,
      serde::compat_version<0>> {
    // highest offset that has been successfully translated (inclusive)
    model::offset highest_translated_offset;
    auto serde_fields() { return std::tie(highest_translated_offset); }
};

}; // namespace datalake::translation
