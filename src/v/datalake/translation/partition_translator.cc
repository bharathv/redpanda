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

#include "datalake/translation/partition_translator.h"

#include "cluster/partition.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "datalake/translation/state_machine.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/utils/txn_reader.h"
#include "ssx/future-util.h"

namespace datalake::translation {

constexpr ::model::timeout_clock::duration wait_timeout = 5s;

partition_translator::partition_translator(
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<coordinator::frontend>* frontend,
  ss::scheduling_group sg,
  size_t reader_max_bytes,
  ssx::semaphore& parallel_translations)
  : _term(partition->raft()->term())
  , _partition(std::move(partition))
  , _frontend(frontend)
  , _max_bytes_per_reader(reader_max_bytes)
  , _parallel_translations(parallel_translations)
  , _logger(prefix_logger{datalake_log, fmt::format("{}", _partition->ntp())}) {
    _stm = _partition->raft()
             ->stm_manager()
             ->get<datalake::translation::translation_stm>();
    ssx::repeat_until_gate_closed(_gate, [this, sg] {
        return ss::with_scheduling_group(sg, [this] { return do_translate(); });
    });
    vlog(_logger.debug, "started partition translator in term {}", _term);
}

ss::future<> partition_translator::stop() {
    vlog(_logger.debug, "stopping partition translator in term {}", _term);
    auto f = _gate.close();
    _as.request_abort();
    co_await std::move(f);
}

ss::future<partition_translator::translation_success>
partition_translator::do_translate_once() {
    if (!_partition->get_ntp_config().iceberg_enabled()) {
        // It is possible the global switch is off.
        co_return translation_success::no;
    }
    if (!co_await reconcile_with_coordinator()) {
        vlog(_logger.warn, "reconciliation with coordinator failed");
        co_return translation_success::no;
    }
    auto last_offset = co_await _stm->highest_translated_offset(wait_timeout);
    vlog(_logger.debug, "last translated offset: {}", last_offset);
    if (!last_offset) {
        co_return translation_success::no;
    }
    auto max_offset = model::prev_offset(_partition->last_stable_offset());
    if (last_offset.value() >= max_offset) {
        // nothing to do
        co_return translation_success::yes;
    }
    // We have some data to translate, make a reader
    // and dispatch to the translator
    // what if we have too little to read, eg: slow moving topic.
    // Currently we rely on user setting the translation interval
    // appropriately to ensure each attempt has something substantial
    // to read. We can add some heuristics to estimate the size and
    // backoff if it is too small and wait for enough data to
    // accumulate.
    auto min_offset = model::next_offset(last_offset.value());
    auto proxy = kafka::make_partition_proxy(_partition);
    auto log_reader = co_await proxy.make_reader(
      {min_offset,
       max_offset,
       0,
       _max_bytes_per_reader,
       ss::default_priority_class(),
       std::nullopt,
       std::nullopt,
       _as});
    auto tracker = kafka::aborted_transaction_tracker::create_default(
      &proxy, std::move(log_reader.ot_state));
    [[maybe_unused]] auto kafka_reader = kafka::read_committed_reader(
      std::move(tracker), std::move(log_reader.reader));

    auto units = co_await ss::get_units(_parallel_translations, 1, _as);

    vlog(
      _logger.info,
      "created reader for range [{}, {}]",
      min_offset,
      max_offset);

    if (!co_await commit_translated_entry(min_offset, max_offset)) {
        co_return translation_success::no;
    }
    // todo: dispatch the reader
    co_return translation_success::yes;
}

ss::future<partition_translator::commit_entry_success>
partition_translator::commit_translated_entry(
  model::offset min, model::offset max) {
    coordinator::add_translated_data_files_request request;
    request.tp = _partition->ntp().tp;
    request.translator_term = _term;
    coordinator::translated_data_file_entry entry;
    entry.tp = _partition->ntp().tp;
    entry.begin_offset = min;
    entry.end_offset = max;
    entry.translator_term = _term;
    entry.translation_result = {};
    request.files.emplace_back(std::move(entry));
    auto result = co_await _frontend->local().add_translated_data_files(
      std::move(request));
    co_return result.errc == coordinator_errc::ok;
}

ss::future<partition_translator::reconcilation_success>
partition_translator::reconcile_with_coordinator() {
    if (_reconcile) {
        auto request = coordinator::fetch_latest_data_file_request{};
        request.tp = _partition->ntp().tp;
        auto resp = co_await _frontend->local().fetch_latest_data_file(request);
        if (resp.errc != coordinator_errc::ok) {
            co_return reconcilation_success::no;
        }
        // No file entry signifies the translation was just enabled.
        // consider everything until current LSO translated and start afresh.
        // There are edge cases when translation is disabled and enabled back to
        // back resulting in offset gaps but that should be ok, we do not have
        // any predicatable guarantees around this.
        auto offset = resp.entry ? resp.entry->end_offset
                                 : _partition->last_stable_offset();
        auto sync_error = co_await _stm->sync_with_coordinator(
          offset, _term, wait_timeout, _as);
        if (sync_error) {
            co_return reconcilation_success::no;
        }
    }
    // Reconciliation is an optimization to avoid wasteful
    // translation work (particularly on term changes). Worst case
    // we do translation work on a stale offset range which then gets
    // rejected on the coordinator commit and we are forced to reconcile
    // again. Here we try to avoid round trips to the coordinator when
    // the translator thinks it is not stale.
    co_return reconcilation_success::yes;
}

ss::future<> partition_translator::do_translate() {
    // todo:  make this a table property.
    static constexpr std::chrono::milliseconds translation_interval{10000};
    auto sleep_duration = translation_interval;
    backoff backoff_policy{translation_interval};
    while (!_as.abort_requested() && _term == _partition->raft()->term()) {
        co_await ss::sleep_abortable(sleep_duration, _as);
        auto success = co_await do_translate_once();
        if (!success) {
            sleep_duration = backoff_policy.next();
            _reconcile = needs_reconciliation::yes;
        } else {
            _reconcile = needs_reconciliation::no;
            sleep_duration = translation_interval;
            backoff_policy.reset();
        }
    }
}

} // namespace datalake::translation
