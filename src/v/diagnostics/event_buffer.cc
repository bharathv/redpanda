/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "diagnostics/event_buffer.h"

namespace diagnostics {

// NOLINTNEXTLINE(*-non-const-global-variables)
thread_local event_buffer* event_buffer::_local_instance = nullptr;

event_buffer::event_buffer(size_t capacity)
  : _events(capacity) {}

ss::future<> event_buffer::start() {
    _local_instance = this;
    _sweep_timer.set_callback([this] { do_sweep(); });
    _sweep_timer.arm_periodic(k_sweep_interval);
    return ss::make_ready_future<>();
}

ss::future<> event_buffer::stop() {
    _local_instance = nullptr;
    _sweep_timer.cancel();
    _guards.clear();
    return _gate.close();
}

void event_buffer::record(diagnostic_event event) {
    if (_gate.is_closed()) {
        return;
    }
    _events.push_back(std::move(event));
}

bool event_buffer::record_throttled(
  std::string_view key,
  diagnostic_event event,
  std::chrono::steady_clock::duration cooldown) {
    if (_gate.is_closed()) {
        return false;
    }
    auto now = std::chrono::steady_clock::now();
    ss::sstring k(key);
    auto it = _throttle_map.find(k);
    if (it != _throttle_map.end() && (now - it->second) < cooldown) {
        return false;
    }
    _throttle_map[k] = now;
    _events.push_back(std::move(event));
    return true;
}

std::vector<diagnostic_event>
event_buffer::query(const event_filter& filter) const {
    std::vector<diagnostic_event> result;
    for (const auto& ev : _events) {
        if (filter.since && ev.timestamp < *filter.since) {
            continue;
        }
        if (filter.min_severity && ev.severity < *filter.min_severity) {
            continue;
        }
        if (
          filter.subsystem_filter && ev.subsystem != *filter.subsystem_filter) {
            continue;
        }
        result.push_back(ev);
    }
    return result;
}

size_t event_buffer::size() const { return _events.size(); }

size_t event_buffer::capacity() const { return _events.capacity(); }

event_buffer* event_buffer::local_instance() noexcept {
    return _local_instance;
}

void event_buffer::register_guard(sweepable& guard) {
    if (!_gate.is_closed()) {
        _guards.push_back(guard);
    }
}

void event_buffer::do_sweep() {
    if (_gate.is_closed()) {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    for (auto& guard : _guards) {
        auto ev = guard.sweep(now);
        if (ev) {
            _events.push_back(std::move(*ev));
        }
    }
}

// --- stuck_guard ---

stuck_guard::stuck_guard(
  std::chrono::steady_clock::duration threshold,
  ss::noncopyable_function<diagnostic_event()> event_factory)
  : _registered_at(std::chrono::steady_clock::now())
  , _threshold(threshold)
  , _event_factory(std::move(event_factory)) {
    if (auto* buf = event_buffer::local_instance()) {
        buf->register_guard(*this);
    }
}

std::optional<diagnostic_event>
stuck_guard::sweep(std::chrono::steady_clock::time_point now) {
    if ((now - _registered_at) >= _threshold) {
        // Re-arm: shift the baseline so we emit again after another interval.
        _registered_at = now;
        return _event_factory();
    }
    return std::nullopt;
}

} // namespace diagnostics
