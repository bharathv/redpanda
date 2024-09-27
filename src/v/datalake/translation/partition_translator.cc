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

static constexpr std::chrono::milliseconds translation_interval{10000};
static constexpr std::chrono::milliseconds translation_jitter{500};
static constexpr int max_attempts = 5;
constexpr ::model::timeout_clock::duration wait_timeout = 5s;

partition_translator::partition_translator(
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<features::feature_table>* features,
  ss::scheduling_group sg,
  size_t reader_max_bytes,
  ssx::semaphore& parallel_translations)
  : _term(partition->raft()->term())
  , _partition(std::move(partition))
  , _frontend(frontend)
  , _features(features)
  , _jitter{translation_interval, translation_jitter}
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

model::offset partition_translator::max_offset_for_translation() const {
    // We factor in LSO to ensure only committed transactional batches are
    // translated and we clamp it to committed offset to avoid translation
    // of majorty appended but not flushed data (eg: acks=0/1/write_caching).
    return std::min(
      model::prev_offset(_partition->last_stable_offset()),
      _partition->committed_offset());
}

std::optional<model::offset> partition_translator::has_new_data_for_translation(
  model::offset max_translated_log_offset) {
    // we store the raft log offsets, convert them to kafka offsets to check
    // if there is any new user data to be translated.
    auto max_readable_log_offset = max_offset_for_translation();
    // todo; this offset translation comparison is a little sketchy as it is
    // not returning the exact Kafka offsets need for the checks to pass.
    // For example if a bunch of configuration batches are appended, that will
    // still trigger this check, so this needs some tightening.
    // the writer still discards them if there are no user batches but it is
    // wasteful to do any work if there are no user batches.
    auto max_translated_kafka_offset = _partition->log()->from_log_offset(
      max_translated_log_offset);
    auto max_readable_kafka_offset = _partition->log()->from_log_offset(
      max_readable_log_offset);
    vlog(
      _logger.trace,
      "has new data for translation: max_readable_log_offset {}, "
      "max_translated_kafka_offset{}, max_readable_kafka_offset: {}",
      max_readable_log_offset,
      max_translated_kafka_offset,
      max_readable_kafka_offset);
    if (max_translated_kafka_offset >= max_readable_kafka_offset) {
        // no new user data
        return std::nullopt;
    }
    return max_readable_log_offset;
}

ss::future<partition_translator::translation_success>
partition_translator::do_translate_once() {
    if (
      !_partition->get_ntp_config().iceberg_enabled()
      || !_features->local().is_active(features::feature::datalake_iceberg)) {
        co_return translation_success::no;
    }
    if (!co_await reconcile_with_coordinator()) {
        vlog(_logger.warn, "reconciliation with coordinator failed");
        co_return translation_success::no;
    }
    auto max_translated_log_offset = co_await _stm->highest_translated_offset(
      wait_timeout);
    if (!max_translated_log_offset) {
        co_return translation_success::no;
    }
    auto translatable_offset = has_new_data_for_translation(
      max_translated_log_offset.value());
    if (!translatable_offset) {
        vlog(
          _logger.trace,
          "translation up to date until log offset: {}, nothing to do",
          max_translated_log_offset);
        co_return translation_success::yes;
    }
    // Start from where we left off.
    auto read_begin_offset = model::next_offset(
      max_translated_log_offset.value());
    auto read_end_offset = translatable_offset.value();
    // We have some data to translate, make a reader
    // and dispatch to the translator
    // what if we have too little to read, eg: slow moving topic.
    // Currently we rely on user setting the translation interval
    // appropriately to ensure each attempt has something substantial
    // to read. We can add some heuristics to estimate the size and
    // backoff if it is too small and wait for enough data to
    // accumulate.
    auto proxy = kafka::make_partition_proxy(_partition);
    auto log_reader = co_await proxy.make_reader(
      {read_begin_offset,
       read_end_offset,
       0,
       _max_bytes_per_reader,
       ss::default_priority_class(),
       std::nullopt,
       std::nullopt,
       _as});

    vlog(
      _logger.debug,
      "translating data in range: [{}, {}]",
      read_begin_offset,
      read_end_offset);
    auto tracker = kafka::aborted_transaction_tracker::create_default(
      &proxy, std::move(log_reader.ot_state));
    [[maybe_unused]] auto kafka_reader = kafka::read_committed_reader(
      std::move(tracker), std::move(log_reader.reader));
    auto units = co_await ss::get_units(_parallel_translations, 1, _as);

    // dispatch_parquet_reader()

    units.return_all();
    if (!co_await commit_translated_entry(read_begin_offset, read_end_offset)) {
        co_return translation_success::no;
    }
    co_return translation_success::yes;
}

ss::future<partition_translator::commit_entry_success>
partition_translator::commit_translated_entry(
  model::offset begin, model::offset end) {
    coordinator::add_translated_data_files_request request;
    request.tp = _partition->ntp().tp;
    request.translator_term = _term;
    coordinator::translated_data_file_entry entry;
    entry.tp = _partition->ntp().tp;
    entry.begin_offset = begin;
    entry.end_offset = end;
    entry.translator_term = _term;
    entry.translation_result = {};
    request.files.emplace_back(std::move(entry));
    vlog(_logger.trace, "Adding tranlsated data file, request: {}", request);
    // We have a separate retry policy to commit the translated state during
    // transient errors like coordinator readership changes.
    backoff retry_policy{2000ms};
    auto result = co_await retry_policy.retry(
      max_attempts,
      [this, request = std::move(request)] {
          return _frontend->local().add_translated_data_files(request.copy());
      },
      [this](coordinator::add_translated_data_files_reply result) {
          return can_continue() && is_retriable(result.errc);
      });
    vlog(_logger.trace, "Adding tranlsated data file, result: {}", result);
    co_return result.errc == coordinator_errc::ok
      && !(co_await _stm->sync_with_coordinator(end, _term, wait_timeout, _as));
}

ss::future<partition_translator::reconcilation_success>
partition_translator::reconcile_with_coordinator() {
    if (_reconcile) {
        auto request = coordinator::fetch_latest_data_file_request{};
        request.tp = _partition->ntp().tp;
        vlog(_logger.trace, "fetch_latest_data_file, request: {}", request);
        auto resp = co_await _frontend->local().fetch_latest_data_file(request);
        vlog(_logger.trace, "fetch_latest_data_file, response: {}", resp);
        if (resp.errc != coordinator_errc::ok) {
            co_return reconcilation_success::no;
        }
        // No file entry signifies the translation was just enabled.
        // consider everything until current LSO translated and start afresh.
        // There are edge cases when translation is disabled and enabled back to
        // back resulting in offset gaps but that should be ok, we do not have
        // any predicatable guarantees around this.
        auto sync_offset = resp.entry ? resp.entry->end_offset
                                      : max_offset_for_translation();
        auto sync_error = co_await _stm->sync_with_coordinator(
          sync_offset, _term, wait_timeout, _as);
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

bool partition_translator::can_continue() const {
    return !_as.abort_requested() && _term == _partition->raft()->term();
}

ss::future<> partition_translator::do_translate() {
    while (can_continue()) {
        co_await ss::sleep_abortable(_jitter.next_jitter_duration(), _as);
        backoff retry_policy{translation_interval};
        auto result = co_await retry_policy.retry(
          max_attempts,
          [this] { return do_translate_once(); },
          [this](translation_success result) {
              auto retry = can_continue() && result == translation_success::no;
              if (retry) {
                  _reconcile = needs_reconciliation::yes;
              }
              return retry;
          });
        if (result) {
            _reconcile = needs_reconciliation::no;
        }
    }
}

} // namespace datalake::translation
