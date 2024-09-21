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

#include "datalake/partition_translator.h"

#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "datalake/translation_tracking_stm.h"
#include "kafka/utils/txn_reader.h"
#include "ssx/future-util.h"

namespace datalake::translation {

constexpr ::model::timeout_clock::duration wait_timeout = 5s;

partition_translator::partition_translator(
  const ::model::ntp& ntp,
  ss::shared_ptr<translation_tracking_stm> stm,
  ss::sharded<coordinator::frontend>* frontend,
  ss::scheduling_group sg,
  size_t max_buffered_data,
  ssx::semaphore& parallel_translations,
  ss::abort_source& as)
  : _stm(std::move(stm))
  , _frontend(frontend)
  , _max_buffered_data(max_buffered_data)
  , _parallel_translations(parallel_translations)
  , _logger(prefix_logger{datalake_log, fmt::format("{}", ntp)}) {
    ssx::repeat_until_gate_closed(_gate, [this, sg] {
        return ss::with_scheduling_group(
          sg, [this] { return do_translation(); });
    });
    _abort_subscription = as.subscribe(
      [this]() noexcept { _as.request_abort(); });
}

ss::future<> partition_translator::stop() {
    auto f = _gate.close();
    _as.request_abort();
    co_await std::move(f);
}

ss::future<> partition_translator::do_translation() {
    while (!_as.abort_requested()) {
        auto last_translated_offset = co_await _stm->highest_translated_offset(
          wait_timeout, _as);
        [[maybe_unused]] auto begin_offset = ::model::next_offset(
          last_translated_offset);
        _as.check();
    }
}

} // namespace datalake::translation
