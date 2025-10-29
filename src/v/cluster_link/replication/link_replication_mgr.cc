/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/link_replication_mgr.h"

#include "base/units.h"
#include "cluster_link/logger.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "utils/to_string.h"

using namespace std::chrono_literals;

namespace cluster_link::replication {

link_replication_manager::link_replication_manager(
  ss::scheduling_group sg,
  std::unique_ptr<link_configuration_provider> config_provider,
  std::unique_ptr<data_source_factory> source_factory,
  std::unique_ptr<data_sink_factory> sink_factory,
  std::optional<replication_probe::configuration> cfg_probe)
  : _sg(sg)
  , _config_provider(std::move(config_provider))
  , _source_factory(std::move(source_factory))
  , _sink_factory(std::move(sink_factory))
  , _cfg_probe{std::move(cfg_probe)}
  , _link_data_probe{nullptr} {}

ss::future<> link_replication_manager::start(link_data_probe_ptr ldp) {
    set_data_probe(std::move(ldp));
    co_await _source_factory->start();
    ssx::repeat_until_gate_closed(_gate, [this] {
        return reconcile().handle_exception([](const std::exception_ptr& e) {
            auto level = ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                                       : ss::log_level::error;
            vlogl(cllog, level, "reconciliation loop failed: {}", e);
        });
    });
    ssx::repeat_until_gate_closed(_gate, [this] {
        return maybe_sync_start_offsets().handle_exception(
          [](const std::exception_ptr& e) {
              auto level = ssx::is_shutdown_exception(e) ? ss::log_level::trace
                                                         : ss::log_level::warn;
              vlogl(cllog, level, "Error in maybe_sync_start_offsets: {}", e);
          });
    });
}

ss::future<> link_replication_manager::stop() {
    vlog(cllog.trace, "Stopping link replication manager");
    // to avoid further submissions to the queue.
    _as.request_abort();
    _pending_cv.broken();
    auto gate_f = _gate.close();
    // no new replicators can be added / removed past this point.
    chunked_vector<ss::future<>> stop_futures;
    stop_futures.reserve(_replicators.size());
    // stop any pending replicators
    for (auto& [_, replicator] : _replicators) {
        stop_futures.push_back(replicator->stop());
    }
    co_await ss::when_all_succeed(stop_futures.begin(), stop_futures.end());
    _replicators.clear();
    co_await std::move(gate_f);
    co_await _source_factory->stop();
    unset_data_probe();
    vlog(cllog.trace, "Link replication manager stopped");
}

void link_replication_manager::set_data_probe(link_data_probe_ptr ldp) {
    _link_data_probe = ldp;

    for (auto& replicator : _replicators | std::views::values) {
        replicator->set_data_probe(ldp);
    }
}

void link_replication_manager::unset_data_probe() {
    _link_data_probe = nullptr;
    for (auto& replicator : _replicators | std::views::values) {
        replicator->unset_data_probe();
    }
}

std::ostream&
operator<<(std::ostream& os, link_replication_manager::op_type op) {
    switch (op) {
    case link_replication_manager::op_type::start:
        return os << "start";
    case link_replication_manager::op_type::stop:
        return os << "stop";
    default:
        return os << "unknown";
    }
}

fmt::iterator
link_replication_manager::ntp_target_state::format_to(fmt::iterator out) const {
    return fmt::format_to(out, "{{op={}, term: {}}}", op, term);
}

void link_replication_manager::ntp_target_state::merge(
  const ::model::ntp& ntp, ntp_target_state new_desired) {
    auto can_merge = !new_desired.term || !term || new_desired.term >= term;
    if (!can_merge) {
        vlog(
          cllog.debug,
          "Ignoring attempt to change desired state for {} from {} to {} as "
          "it has a lower term",
          ntp,
          *this,
          new_desired);
        return;
    }
    vlog(
      cllog.debug,
      "Merging desired state for {} from {} to {}",
      ntp,
      *this,
      new_desired);
    op = new_desired.op;
    term = new_desired.term;
}

void link_replication_manager::start_replicator(
  model::ntp ntp, model::term_id term) {
    if (_gate.is_closed()) {
        return;
    }
    vlog(cllog.debug, "Enqueuing start action for {} term {}", ntp, term);
    auto& rec_state = _pending[ntp];
    rec_state.set_desired(ntp, {.op = op_type::start, .term = term});
    _pending_cv.signal();
}

void link_replication_manager::stop_replicator(
  model::ntp ntp, std::optional<model::term_id> term) {
    if (_gate.is_closed()) {
        return;
    }
    vlog(cllog.debug, "Enqueuing stop action for {} term {}", ntp, term);
    auto& rec_state = _pending[ntp];
    rec_state.set_desired(ntp, {.op = op_type::stop, .term = term});
    _pending_cv.signal();
}

void link_replication_manager::stop_replicators(
  std::optional<::model::topic> topic) {
    if (_gate.is_closed()) {
        return;
    }
    for (const auto& [ntp, _] : _replicators) {
        if (!topic || ntp.tp.topic == *topic) {
            stop_replicator(ntp, std::nullopt);
        }
    }
}

chunked_hash_map<::model::ntp, partition_offsets_report>
link_replication_manager::get_partition_offsets_report() const {
    chunked_hash_map<::model::ntp, partition_offsets_report> results;
    results.reserve(_replicators.size());

    for (const auto& [ntp, replicator] : _replicators) {
        results.emplace(ntp, replicator->get_partition_offsets_report());
    }
    return results;
}

std::optional<partition_offsets_report>
link_replication_manager::get_partition_offsets_report(
  const ::model::ntp& ntp) const {
    auto it = _replicators.find(ntp);
    if (it == _replicators.end()) {
        return std::nullopt;
    }
    return it->second->get_partition_offsets_report();
}

bool link_replication_manager::has_pending_actions() {
    return std::ranges::any_of(_pending, [](const auto& pair) {
        const auto& [_, rec_state] = pair;
        return rec_state.needs_reconciliation()
               || rec_state.reconciliation_complete();
    });
}

ss::future<> link_replication_manager::reconcile() {
    co_await _pending_cv.wait([this] {
        return has_pending_actions()
               && _max_reconciliations.available_units() > 0;
    });
    if (_gate.is_closed()) {
        co_return;
    }
    vlog(
      cllog.debug,
      "Starting reconciliation loop, pending actions: {}",
      _pending.size());
    // cleanup completed reconciliations
    for (auto it = _pending.begin(); it != _pending.end();) {
        if (it->second.reconciliation_complete()) {
            it = _pending.erase(it);
        } else {
            ++it;
        }
    }
    // launch new reconciliations
    for (auto& [ntp, rec_state] : _pending) {
        if (!rec_state.needs_reconciliation()) {
            continue;
        }
        auto units = ss::try_get_units(_max_reconciliations, 1);
        if (!units) {
            // try when available.
            co_return;
        }
        ssx::spawn_with_gate(
          _gate, [this, ntp, u = std::move(units.value())]() mutable {
              return reconcile_ntp_once(std::move(ntp), std::move(u));
          });
    }
}

ss::future<> link_replication_manager::reconcile_ntp_once(
  ::model::ntp ntp, ssx::semaphore_units units) {
    auto it = _pending.find(ntp);
    if (it == _pending.end()) {
        co_return;
    }
    auto& state = it->second;
    vlog(cllog.debug, "Reconciling ntp {}", ntp);
    vassert(
      state.needs_reconciliation(),
      "Attempting to reconcile ntp {} which is already in progress",
      ntp);
    // mark it in progress
    // this is the target state for this iteration of reconciliation
    state.in_progress = std::exchange(state.desired, std::nullopt);
    auto target_state = state.in_progress.value();
    auto replicator_it = _replicators.find(ntp);
    auto has_running_replicator = replicator_it != _replicators.end();
    if (has_running_replicator) {
        // check if it needs to be stopped first
        auto replicator_term = replicator_it->second->term();
        auto needs_stop = false;
        switch (target_state.op) {
        case op_type::start:
            // if there is a start from higher term, we nee to restart
            needs_stop = target_state.term
                         && (replicator_term < *target_state.term);
            break;
        case op_type::stop:
            // If the stop is from higher or equal term, we need to stop
            needs_stop = !target_state.term
                         || (replicator_term <= *target_state.term);
            break;
        }
        if (needs_stop) {
            auto stop_it = _replicators.extract(replicator_it);
            try {
                co_await stop_it.second->stop();
            } catch (const std::exception& e) {
                vlog(
                  cllog.warn,
                  "Error stopping replicator for {}: {}, moving on",
                  ntp,
                  e);
            }
            has_running_replicator = false;
        }
    }
    // Check if we need to start a replicator
    if (!has_running_replicator && target_state.op == op_type::start) {
        vassert(
          !_replicators.contains(ntp), "Replicator for {} already exists", ntp);
        vassert(
          target_state.term.has_value(),
          "Cannot start replicator for {} without a term",
          ntp);
        auto term = target_state.term.value();
        auto source = _source_factory->make_source(ntp);
        auto sink = _sink_factory->make_sink(ntp);
        auto replicator = std::make_unique<partition_replicator>(
          ntp,
          term,
          *_config_provider,
          std::move(source),
          std::move(sink),
          _sg,
          _cfg_probe,
          _link_data_probe);
        auto [r_it, _] = _replicators.emplace(ntp, std::move(replicator));
        try {
            co_await r_it->second->start();
        } catch (const std::exception& e) {
            vlog(cllog.warn, "Error starting replicator for {}: {}", ntp, e);
            // this initiates a step down and a subsequent stop
            r_it->second->notify_sink_on_failure(term);
        }
    }
    state.in_progress.reset();
    units.release();
    _pending_cv.signal();
}

ss::future<> link_replication_manager::maybe_sync_start_offsets() {
    auto h = _gate.hold();
    ssx::async_counter cnt;
    auto ntps = _replicators | std::views::keys
                | std::ranges::to<chunked_vector<::model::ntp>>();
    co_await ssx::async_for_each_counter(
      cnt, std::move(ntps), [this](const ::model::ntp& ntp) {
          auto it = _replicators.find(ntp);
          if (it != _replicators.end()) {
              it->second->maybe_synchronize_start_offset();
          }
      });

    co_await ss::sleep_abortable(start_offset_synch_interval, _as);
}
} // namespace cluster_link::replication
