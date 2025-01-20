/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/translation/scheduling_api.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace datalake::translation::scheduling {

std::ostream& operator<<(std::ostream& os, translator_status state) {
    switch (state) {
    case translator_status::waiting_for_data:
        return os << "waiting_for_data";
    case translator_status::ready_to_translate:
        return os << "ready_to_translate";
    case translator_status::translating:
        return os << "translating";
    }
}

class default_reservations_tracker : public reservations_tracker {
public:
    explicit default_reservations_tracker(
      size_t total_memory, size_t memory_block_size)
      : _total_memory_blocks(
          static_cast<size_t>(total_memory / memory_block_size))
      , _available_memory_blocks{_total_memory_blocks, "dl/translation/memory"}
      , _memory_block_size(memory_block_size) {
        vassert(
          _total_memory_blocks > 0,
          "Need atleast one block of memory for translation to make progress, "
          "total_memory: {}, block_size: {}",
          total_memory,
          memory_block_size);
    }

    bool memory_exhausted() const override {
        return _available_memory_blocks.available_units() <= 0;
    }

    size_t total_memory_blocks() const override { return _total_memory_blocks; }

    ss::future<reservation> reserve_memory(ss::abort_source& as) override {
        auto units = co_await ss::get_units(
          _available_memory_blocks, BLOCKS_PER_RESERVATION, as);
        co_return reservation{
          .reserved_bytes = BLOCKS_PER_RESERVATION * _memory_block_size,
          .reservation = std::move(units)};
    }

private:
    static constexpr size_t BLOCKS_PER_RESERVATION = 1;
    // note: the semaphore should be alive until all the reserved units are
    // deposited back.
    size_t _total_memory_blocks;
    ssx::semaphore _available_memory_blocks;
    size_t _memory_block_size;
};

std::ostream& operator<<(std::ostream& o, const translator& state) {
    fmt::print(
      o,
      "{{ id: {}, debug: {}, target_max_lag_ms: {} }}",
      state.id(),
      state.debug(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.max_target_lag()));
    return o;
}

translator_state::translator_state(
  std::unique_ptr<translator> translator, translator_status initial_status)
  : _translator(std::move(translator))
  , _current_status(initial_status)
  , _last_translation_attempt_time(clock::now())
  , _last_successful_translation_finish_time(clock::now()) {}

std::ostream& operator<<(std::ostream& o, const translator_state& state) {
    fmt::print(
      o,
      "{{ translator: {}, current_status: {}, ms_since_successful_translation: "
      "{}}}",
      *(state._translator),
      state._current_status,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.remaining_lag_duration()));
    return o;
}

bool translator_state::requires_rescheduling() const {
    return ((_current_status == translator_status::ready_to_translate)
            && (!_backoff_until || _backoff_until.value() < clock::now()))
           || (_inflight_translation_state && _inflight_translation_state->deadline >= clock::now());
}

ss::future<bool> translator_state::has_new_data_to_translate() {
    auto fut = co_await ss::coroutine::as_future(
      _translator->has_new_data_to_translate());
    if (fut.failed()) {
        vlog(
          datalake_log.warn,
          "[{}] failed to check for new data to translate: {}",
          *this,
          fut.get_exception());
        co_return false;
    }
    co_return fut.get();
}

clock::duration translator_state::remaining_lag_duration() const {
    return _last_successful_translation_finish_time
           + _translator->max_target_lag() - clock::now();
}

ss::future<> translator_state::stop() {
    auto f = _gate.close();
    if (_inflight_translation_state) {
        _inflight_translation_state->expiration_timer.cancel();
        _inflight_translation_state->as.request_abort();
    }
    co_await std::move(f);
    co_await _translator->stop();
}

scheduling_state::scheduling_state(
  std::unique_ptr<reservations_tracker> reservations,
  config::binding<std::chrono::milliseconds> translator_poller_frequency)
  : _reservations(std::move(reservations))
  , _translator_poller_frequency(std::move(translator_poller_frequency)) {
    _translator_poller.set_callback([this] {
        ssx::spawn_with_gate(
          _gate, [this] { return poll_translators_for_updates(); });
    });
    _translator_poller.arm_periodic(_translator_poller_frequency());
    _translator_poller_frequency.watch([this] {
        if (_gate.is_closed()) {
            return;
        }
        _translator_poller.rearm_periodic(_translator_poller_frequency());
    });
}

auto scheduling_state::translators_with_status(translator_status status) {
    return _translators | std::views::filter([&status](auto& state) {
               return state.second._current_status == status;
           });
}

ss::future<> scheduling_state::poll_translators_for_updates() {
    auto holder = _gate.hold();
    auto units = co_await _lock.hold_read_lock();
    auto waiters = translators_with_status(translator_status::waiting_for_data);
    auto state_changed = false;
    co_await ss::max_concurrent_for_each(
      waiters, 32, [&state_changed](auto& entry) mutable {
          auto& state = entry.second;
          return state.has_new_data_to_translate().then(
            [&state, &state_changed](bool has_new_data) {
                if (
                  has_new_data
                  && state._current_status
                       == translator_status::waiting_for_data) {
                    state.set_status(translator_status::ready_to_translate);
                    state_changed = true;
                }
            });
      });
    if (state_changed) {
        _state_changed.signal();
        co_return;
    }
    // Check if there are any translators that expired backoff and are ready
    auto ready_translators = _translators | std::views::filter([](auto& state) {
                                 return state.second.requires_rescheduling();
                             });
    if (!std::ranges::empty(ready_translators)) {
        _state_changed.signal();
    }
}

bool scheduling_state::memory_exhausted() const {
    return _reservations->memory_exhausted();
}

size_t scheduling_state::total_memory_blocks() const {
    return _reservations->total_memory_blocks();
}

ss::future<reservation> scheduling_state::reserve_memory(ss::abort_source& as) {
    if (memory_exhausted()) {
        _state_changed.signal();
    }
    return _reservations->reserve_memory(as);
}

bool scheduling_state::requires_rescheduling() {
    auto waiting_translators
      = _translators | std::views::filter([](auto& state) {
            return state.second.requires_rescheduling();
        });
    return memory_exhausted() || !std::ranges::empty(waiting_translators);
}

ss::future<>
scheduling_state::register_translator(std::unique_ptr<translator> translator) {
    auto holder = _gate.hold();
    vassert(
      !_translators.contains(translator->id()),
      "duplicate translator registration with id: {}",
      translator->id());

    co_await translator->start();
    auto has_data_to_translate
      = co_await translator->has_new_data_to_translate();
    auto initial_status = has_data_to_translate
                            ? translator_status::ready_to_translate
                            : translator_status::waiting_for_data;
    {
        auto units = co_await _lock.hold_write_lock();
        _translators.emplace(std::make_pair(
          translator->id(),
          translator_state{std::move(translator), initial_status}));
    }
    if (has_data_to_translate) {
        _state_changed.signal();
    }
}

ss::future<> scheduling_state::deregister_translator(translator_id id) {
    auto holder = _gate.hold();
    auto it = _translators.find(id);
    if (it == _translators.end()) {
        co_return;
    }
    auto units = co_await _lock.hold_write_lock();
    auto translator = std::move(it->second);
    _translators.erase(it);
    units.return_all();
    co_await translator.stop();
}

void scheduling_state::translate_in_background(
  translator_state& state,
  ssx::semaphore_units units,
  std::optional<clock::time_point> deadline) {
    auto holder = _gate.hold();
    auto state_holder = state._gate.hold();
    vassert(
      !state._inflight_translation_state,
      "Duplicate translation attempt on translator: {}",
      state);
    state.set_status(translator_status::translating);
    state._backoff_until.reset();
    state._inflight_translation_state
      = std::make_unique<translator_state::inflight_translation_state>();
    state._inflight_translation_state->units = std::move(units);
    state._inflight_translation_state->deadline = deadline;
    if (deadline) {
        state._inflight_translation_state->expiration_timer.set_callback(
          [this]() { _state_changed.signal(); });
        state._inflight_translation_state->expiration_timer.arm(
          deadline.value());
    }
    ssx::spawn_with_gate(
      state._gate, [this, holder = std::move(holder), &state]() mutable {
          return state._translator
            ->translate_now(
              *this,
              state._inflight_translation_state->as,
              state._inflight_translation_state->accumulated_bytes_so_far)
            .then_wrapped(
              [this, &state, holder = std::move(holder)](auto result) {
                  state._inflight_translation_state->expiration_timer.cancel();
                  state._inflight_translation_state.reset();
                  state.set_status(translator_status::waiting_for_data);
                  if (result.failed()) {
                      vlog(
                        datalake_log.warn,
                        "[{}] Exception running translation: {}",
                        state,
                        result.get_exception());
                  } else if (result.get()) {
                      // has more data
                      state.set_status(translator_status::ready_to_translate);
                  }
                  _state_changed.signal();
              });
      });
}

ss::future<> scheduling_state::shutdown() {
    auto f = _gate.close();
    auto translators = std::exchange(_translators, {});
    co_await ss::max_concurrent_for_each(
      translators, 32, [](auto& entry) mutable { return entry.second.stop(); });
    _state_changed.broken();
    _translator_poller.cancel();
    co_await std::move(f);
}

scheduler::scheduler(
  std::unique_ptr<reservations_tracker> reservations,
  std::unique_ptr<scheduling_policy> scheduling_policy,
  config::binding<std::chrono::milliseconds> idle_translator_polling_interval)
  : _state(std::move(reservations), std::move(idle_translator_polling_interval))
  , _scheduling_policy(std::move(scheduling_policy)) {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              if (!ssx::is_shutdown_exception(e)) {
                  vlog(
                    datalake_log.warn,
                    "Exception encountered in scheduling loop : {}",
                    e);
              }
          });
    });
}

ss::future<> scheduler::scheduling_loop() {
    while (!_as.abort_requested() && !_gate.is_closed()) {
        co_await _state._state_changed.wait([this] {
            return _scheduling_policy->can_schedule_translators()
                   && _state.requires_rescheduling();
        });
        auto units = co_await _state._lock.hold_read_lock();
        co_await _scheduling_policy->scheduling_tick(_state, _as);
    }
}

ss::future<>
scheduler::register_translator(std::unique_ptr<translator> translator) {
    auto holder = _gate.hold();
    ssx::spawn_with_gate(_gate, [&translator] { return translator->start(); });
    co_await _state.register_translator(std::move(translator));
}

ss::future<> scheduler::deregister_translator(translator_id id) {
    auto holder = _gate.hold();
    co_await _state.deregister_translator(id);
}

ss::future<> scheduler::stop() {
    _as.request_abort();
    auto f = _gate.close();
    co_await _state.shutdown();
    co_await std::move(f);
}

} // namespace datalake::translation::scheduling
