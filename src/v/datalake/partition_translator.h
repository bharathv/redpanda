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

#include "base/seastarx.h"
#include "model/record_batch_reader.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace datalake::coordinator {
class frontend;
}; // namespace datalake::coordinator

namespace datalake::translation {

class translation_tracking_stm;

class partition_translator {
public:
    explicit partition_translator(
      const model::ntp&,
      ss::shared_ptr<translation_tracking_stm>,
      ss::sharded<coordinator::frontend>*,
      ss::scheduling_group,
      size_t max_buffered_data,
      ssx::semaphore& parallel_translations,
      ss::abort_source&);

    ss::future<> stop();

private:
    ss::future<> do_translation();
    ss::future<model::record_batch_reader> make_reader();
    ss::shared_ptr<translation_tracking_stm> _stm;
    ss::sharded<coordinator::frontend>* _frontend;
    size_t _max_buffered_data;
    ssx::semaphore& _parallel_translations;
    ss::gate _gate;
    ss::abort_source _as;
    ss::optimized_optional<ss::abort_source::subscription> _abort_subscription;
    prefix_logger _logger;
};

} // namespace datalake::translation
