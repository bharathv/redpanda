/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/partition_replicator.h"

#include "ssx/future-util.h"

namespace cluster_link::replication {

partition_replicator::partition_replicator(
  const model::ntp& ntp,
  model::term_id term,
  std::unique_ptr<data_sink> sink,
  size_t max_buffered_bytes)
  : _log(cllog, fmt::format("[{}-term-{}] replicator", ntp, term))
  , _sink(std::move(sink))
  , _memory(
      max_buffered_bytes,
      fmt::format("[{}-term-{}] replicator_memory", ntp, term)) {}

ss::future<> partition_replicator::start(notification_cb cb) {
    _log.trace("Starting replicator");
    _subscription = _subscribers.register_cb(std::move(cb));
    ssx::repeat_until_gate_closed(_gate, [this] { return do_replicate(); });
    return ss::now();
}

ss::future<> partition_replicator::stop() {
    _log.trace("Stopping replicator");
    if (_subscription) {
        _subscribers.unregister_cb(*_subscription);
    }
    _pending_cv.broken();
    _memory.broken();
    _pending.clear();
    return _gate.close();
}

void partition_replicator::do_notify(kafka::offset offset, notification n) {
    if (_gate.is_closed()) {
        return;
    }
    _subscribers.notify(offset, n);
}

void partition_replicator::enqueue_for_replication(
  chunked_vector<model::record_batch> batches) noexcept {
    if (batches.empty() || _gate.is_closed()) [[unlikely]] {
        return;
    }
    auto current = _memory.current();
    auto begin_offset = model::offset_cast(batches.front().base_offset());
    if (current == 0) {
        do_notify(begin_offset, notification::queue_full);
        return;
    }
    auto total_bytes = std::accumulate(
      batches.begin(),
      batches.end(),
      size_t(0),
      [](size_t acc, const model::record_batch& b) {
          return acc + b.size_bytes();
      });
    // We may be over the limit but thats okay we cannot perfectly align with
    // with mem limit across multiple batches.
    auto units = ss::try_get_units(_memory, std::min(total_bytes, current));
    if (!units) {
        do_notify(begin_offset, notification::queue_full);
        return;
    }
    for (auto& batch : batches) {
        _pending.push_back(std::move(batch));
    }
    if (_pending_units.count()) {
        _pending_units.adopt(std::move(units.value()));
    } else {
        _pending_units = std::move(units.value());
    }
    _pending_cv.signal();
    return;
}

void partition_replicator::handle_replication_result(
  kafka::offset begin, kafka::offset end, replication_errc result) {
    vlog(
      _log.debug,
      "Replicated batches from {} to {} with result: {}",
      begin,
      end,
      result);

    switch (result) {
    case replication_errc::success:
    case replication_errc::invalid_data:
        // Successfully replicated, we can release the memory units.
        // We ignore invalid data and move on.
        break;
    case replication_errc::misaligned_offset:
    case replication_errc::replication_error:
        // reset any inflight data and let the source know to backoff
        // In case of replication errors there is going to be a step down
        // soon-ish, so we don't need to do anything special but incase
        // of misaligned offsets we keep retrying in the hope the the order
        // of batches will be restored soon-ish. In case this is sticky error,
        // we just keep retrying forever, so we may need some special handling
        // to accept gaps in the future.
        _pending.clear();
        _pending_units.return_all();
        _pending_cv.signal();
        do_notify(begin, notification::reset_source);
        break;
    }
    // Successfully replicated, we can release the memory units.
}

ss::future<> partition_replicator::do_replicate() {
    // let the data stream begin from source.
    auto holder = _gate.hold();
    while (!_gate.is_closed()) {
        co_await _pending_cv.wait([this] { return !_pending.empty(); });
        auto to_replicate = std::exchange(_pending, {});
        auto units = std::exchange(_pending_units, {});
        vassert(!to_replicate.empty(), "No batches to replicate");
        auto base_offset = model::offset_cast(
          to_replicate.front().base_offset());
        auto end_offset = model::offset_cast(to_replicate.back().last_offset());
        auto result = co_await _sink->replicate(
          base_offset, std::move(to_replicate));
        handle_replication_result(base_offset, end_offset, result);
    }
}

} // namespace cluster_link::replication
