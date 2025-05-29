/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datalake/datalake_usage_aggregator.h"

#include "cluster/controller.h"
#include "datalake/logger.h"

namespace datalake {

disabled_datalake_usage_api_impl::disabled_datalake_usage_api_impl(
  cluster::controller* controller)
  : _controller(controller) {
    vassert(
      _controller,
      "Controller must not be null for disabled datalake usage API");
}

ss::future<kafka::datalake_usage_api::usage_stats>
disabled_datalake_usage_api_impl::compute_usage(ss::abort_source&) {
    usage_stats stats;
    if (!_controller->is_raft0_leader()) {
        stats.missing_reason = kafka::datalake_usage_api::stats_missing_reason::
          not_controller_leader;
    } else {
        stats.missing_reason
          = kafka::datalake_usage_api::stats_missing_reason::feature_disabled;
        vlog(
          datalake_log.debug,
          "Datalake usage API is disabled, returning empty stats");
    }
    return ss::make_ready_future<usage_stats>(std::move(stats));
}

} // namespace datalake
