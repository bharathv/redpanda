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

#pragma once

#include "diagnostics/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

#include <boost/circular_buffer.hpp>
#include <boost/intrusive/list.hpp>
#include <fmt/format.h>

#include <optional>
#include <unordered_map>

namespace diagnostics {

class event_buffer;

// -----------------------------------------------------------------------
// Progress metric types — pluggable, each defines unit + value.
// -----------------------------------------------------------------------

struct bytes_progress {
    static constexpr std::string_view unit{"bytes"};
    int64_t value{0};
};

struct count_progress {
    static constexpr std::string_view unit{"partitions"};
    int64_t value{0};
};

struct pct_progress {
    static constexpr std::string_view unit{"percent"};
    float value{0.0f};
};

// -----------------------------------------------------------------------
// sweepable — base for all guards that participate in the sweep timer.
// -----------------------------------------------------------------------

/// Base class for guards registered in the event_buffer's sweep list.
/// Uses auto_unlink so destruction removes the guard from the list.
class sweepable
  : public boost::intrusive::list_base_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
public:
    virtual ~sweepable() noexcept = default;

    /// Called by sweep timer. Returns event to record, or nullopt to skip.
    virtual std::optional<diagnostic_event>
    sweep(std::chrono::steady_clock::time_point now) = 0;
};

// -----------------------------------------------------------------------
// stuck_guard — emits once after threshold, then stops.
// -----------------------------------------------------------------------

class stuck_guard final : public sweepable {
public:
    stuck_guard(
      std::chrono::steady_clock::duration threshold,
      ss::noncopyable_function<diagnostic_event()> event_factory);

    std::optional<diagnostic_event>
    sweep(std::chrono::steady_clock::time_point now) override;

private:
    std::chrono::steady_clock::time_point _registered_at;
    std::chrono::steady_clock::duration _threshold;
    ss::noncopyable_function<diagnostic_event()> _event_factory;
};

// -----------------------------------------------------------------------
// progress_guard<P> — periodic progress events with typed before/after.
// -----------------------------------------------------------------------

/// Emits progress events every `interval` with before/after snapshots.
/// P is a progress metric type with `static constexpr string_view unit`
/// and a `value` field.
///
/// Each sweep reads the current value via the reader, compares to the
/// previous snapshot, and emits:
///   "progress: before=X after=Y unit=Z elapsed=Ns"
///
/// Usage:
///   int64_t moved = 0;
///   diagnostics::progress_guard<bytes_progress> guard(
///       std::chrono::seconds(60),
///       [&] { return bytes_progress{moved}; },
///       base_event);
template<typename P>
class progress_guard final : public sweepable {
public:
    progress_guard(
      std::chrono::steady_clock::duration interval,
      ss::noncopyable_function<P()> reader,
      diagnostic_event base_event);

    std::optional<diagnostic_event>
    sweep(std::chrono::steady_clock::time_point now) override;

private:
    std::chrono::steady_clock::time_point _last_emit;
    std::chrono::steady_clock::duration _interval;
    ss::noncopyable_function<P()> _reader;
    diagnostic_event _base_event;
    P _prev_snapshot{};
};

// -----------------------------------------------------------------------
// event_filter
// -----------------------------------------------------------------------

struct event_filter {
    std::optional<std::chrono::system_clock::time_point> since;
    std::optional<severity> min_severity;
    std::optional<subsystem> subsystem_filter;
};

// -----------------------------------------------------------------------
// event_buffer
// -----------------------------------------------------------------------

class event_buffer final : public ss::peering_sharded_service<event_buffer> {
public:
    static constexpr size_t k_default_capacity = 10000;
    static constexpr auto k_sweep_interval = std::chrono::seconds(10);

    explicit event_buffer(size_t capacity = k_default_capacity);

    ss::future<> start();
    ss::future<> stop();

    void record(diagnostic_event event);

    bool record_throttled(
      std::string_view key,
      diagnostic_event event,
      std::chrono::steady_clock::duration cooldown = std::chrono::seconds(30));

    std::vector<diagnostic_event> query(const event_filter& filter) const;

    size_t size() const;
    size_t capacity() const;

    static event_buffer* local_instance() noexcept;

    void register_guard(sweepable& guard);

private:
    void do_sweep();

    using guard_list_t = boost::intrusive::
      list<sweepable, boost::intrusive::constant_time_size<false>>;

    static thread_local event_buffer* _local_instance;

    boost::circular_buffer<diagnostic_event> _events;
    std::unordered_map<ss::sstring, std::chrono::steady_clock::time_point>
      _throttle_map;
    guard_list_t _guards;
    ss::timer<ss::lowres_clock> _sweep_timer;
    ss::gate _gate;
};

// -----------------------------------------------------------------------
// Convenience free functions.
// -----------------------------------------------------------------------

inline void emit(diagnostic_event event) {
    if (auto* buf = event_buffer::local_instance()) {
        buf->record(std::move(event));
    }
}

inline void emit_throttled(
  std::string_view key,
  diagnostic_event event,
  std::chrono::steady_clock::duration cooldown = std::chrono::seconds(30)) {
    if (auto* buf = event_buffer::local_instance()) {
        buf->record_throttled(key, std::move(event), cooldown);
    }
}

// -----------------------------------------------------------------------
// Template implementations.
// -----------------------------------------------------------------------

template<typename P>
progress_guard<P>::progress_guard(
  std::chrono::steady_clock::duration interval,
  ss::noncopyable_function<P()> reader,
  diagnostic_event base_event)
  : _last_emit(std::chrono::steady_clock::now())
  , _interval(interval)
  , _reader(std::move(reader))
  , _base_event(std::move(base_event)) {
    _prev_snapshot = _reader();
    if (auto* buf = event_buffer::local_instance()) {
        buf->register_guard(*this);
    }
}

template<typename P>
std::optional<diagnostic_event>
progress_guard<P>::sweep(std::chrono::steady_clock::time_point now) {
    if ((now - _last_emit) < _interval) {
        return std::nullopt;
    }
    _last_emit = now;

    auto current = _reader();
    auto before_val = _prev_snapshot.value;
    auto after_val = current.value;
    _prev_snapshot = current;

    auto elapsed_s
      = std::chrono::duration_cast<std::chrono::seconds>(_interval).count();
    auto rate = elapsed_s > 0
                  ? static_cast<double>(after_val - before_val) / elapsed_s
                  : 0.0;

    auto ev = _base_event;
    ev.timestamp = diagnostic_event::clock_type::now();
    ev.payload = error_event{
      .payload = generic_error{
        .message = fmt::format(
          "progress: before={} after={} unit={} rate={:.1f}/s",
          before_val,
          after_val,
          P::unit,
          rate)}};
    return ev;
}

} // namespace diagnostics
