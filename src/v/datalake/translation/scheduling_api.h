/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "ssx/semaphore.h"
#include "utils/named_type.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

#include <cstdint>
#include <ostream>

namespace datalake::translation::scheduling {

using clock = ss::lowres_clock;

struct reservation {
    size_t reserved_bytes{0};
    ssx::semaphore_units reservation;
};

class reservations_tracker {
public:
    reservations_tracker() = default;
    reservations_tracker(const reservations_tracker&) = delete;
    reservations_tracker& operator=(const reservations_tracker&) = delete;
    reservations_tracker(reservations_tracker&&) = default;
    reservations_tracker& operator=(reservations_tracker&&) = default;
    virtual ~reservations_tracker() = default;

    virtual size_t total_memory_blocks() const;
    virtual ss::future<reservation> reserve_memory(ss::abort_source&);
    virtual bool memory_exhausted() const;

    static std::unique_ptr<reservations_tracker>
    make_default(size_t total_memory, size_t memory_block_size);
};

enum class translator_status : int8_t {
    waiting_for_data = 0,
    ready_to_translate = 1,
    translating = 2,
};

std::ostream& operator<<(std::ostream&, translator_status);

using translator_id = named_type<int64_t, struct translator_id_type>;
class translator {
public:
    translator() = default;
    translator(const translator&) = delete;
    translator& operator=(const translator&) = delete;
    translator(translator&&) = delete;
    translator& operator=(translator&&) = delete;
    virtual ~translator() = default;

    virtual translator_id id() const = 0;
    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;
    virtual clock::duration max_target_lag() const = 0;
    virtual ss::sstring debug() const = 0;

    // todo: make this an async API and integrate with state changed
    // notification
    ss::future<bool> has_new_data_to_translate();
    ss::future<bool> translate_now(
      reservations_tracker&,
      ss::abort_source&,
      size_t& accumulated_bytes_so_far);

    friend std::ostream& operator<<(std::ostream&, const translator&);
};

class scheduling_policy;
class scheduling_state;
class scheduler;

class translator_state {
public:
    explicit translator_state(
      std::unique_ptr<translator> translator, translator_status initial_status);
    translator_state(const translator_state&) = delete;
    translator_state& operator=(const translator_state&) = delete;
    translator_state(translator_state&&) = default;
    translator_state& operator=(translator_state&&) = default;
    ~translator_state() = default;

    void set_status(translator_status status) {
        // todo: add some checks on validity of state transition
        _current_status = status;
    }

    ss::future<> stop();

    friend std::ostream& operator<<(std::ostream&, const translator_state&);

private:
    friend scheduler;
    friend scheduling_state;

    ss::future<bool> has_new_data_to_translate();
    clock::duration remaining_lag_duration() const;
    bool requires_rescheduling() const;
    struct inflight_translation_state {
        std::optional<clock::time_point> deadline;
        ss::abort_source as;
        ssx::semaphore_units units;
        ss::timer<clock> expiration_timer;
        size_t accumulated_bytes_so_far{0};
    };
    std::unique_ptr<translator> _translator;
    std::unique_ptr<inflight_translation_state> _inflight_translation_state;
    translator_status _current_status;
    clock::time_point _last_translation_attempt_time;
    clock::time_point _last_successful_translation_finish_time;
    std::optional<clock::time_point> _backoff_until;
    ss::gate _gate;
};

using translators = chunked_hash_map<translator_id, translator_state>;

class scheduling_state : public reservations_tracker {
public:
    explicit scheduling_state(
      std::unique_ptr<reservations_tracker> reservations,
      config::binding<std::chrono::milliseconds> translator_poller_frequency);
    scheduling_state(const scheduling_state&) = delete;
    scheduling_state& operator=(const scheduling_state&) = delete;
    scheduling_state(scheduling_state&&) = default;
    scheduling_state& operator=(scheduling_state&&) = delete;
    ~scheduling_state() = default;

    bool memory_exhausted() const override;
    size_t total_memory_blocks() const override;
    bool requires_rescheduling();
    ss::future<reservation> reserve_memory(ss::abort_source&) override;
    ss::future<> register_translator(std::unique_ptr<translator>);
    ss::future<> deregister_translator(translator_id);
    ss::future<> shutdown();

private:
    friend class scheduling_policy;
    friend class abort_policy;
    friend class scheduler;

    ss::future<> poll_translators_for_updates();
    void translate_in_background(
      translator_state&,
      ssx::semaphore_units,
      std::optional<clock::time_point> deadline);
    auto translators_with_status(translator_status);

    translators _translators;
    std::unique_ptr<reservations_tracker> _reservations;
    ss::condition_variable _state_changed;
    ss::basic_rwlock<clock> _lock;
    ss::timer<clock> _translator_poller;
    config::binding<std::chrono::milliseconds> _translator_poller_frequency;
    ss::gate _gate;
};

class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&&) = delete;
    scheduling_policy& operator=(scheduling_policy&&) = delete;
    virtual ~scheduling_policy() = default;

    virtual bool can_schedule_translators() = 0;

    virtual ss::future<>
    scheduling_tick(scheduling_state&, ss::abort_source&) = 0;

    static std::unique_ptr<scheduling_policy> make_default();
};

class scheduler {
public:
    explicit scheduler(
      std::unique_ptr<reservations_tracker>,
      std::unique_ptr<scheduling_policy>,
      config::binding<std::chrono::milliseconds>
        idle_translator_polling_interval);
    ss::future<> register_translator(std::unique_ptr<translator>);
    ss::future<> deregister_translator(translator_id);
    ss::future<> stop();

private:
    ss::future<> scheduling_loop();
    scheduling_state _state;
    std::unique_ptr<scheduling_policy> _scheduling_policy;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace datalake::translation::scheduling
