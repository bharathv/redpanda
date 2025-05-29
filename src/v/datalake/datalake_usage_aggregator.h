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

#pragma once

#include "kafka/server/datalake_usage_api.h"

namespace cluster {
class controller;
}

namespace datalake {

class disabled_datalake_usage_api_impl final
  : public kafka::datalake_usage_api {
public:
    disabled_datalake_usage_api_impl(cluster::controller*);

    ss::future<usage_stats> compute_usage(ss::abort_source&) final;

private:
    cluster::controller* _controller{nullptr};
};
} // namespace datalake
