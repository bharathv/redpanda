/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "cluster/fwd.h"
#include "cluster/producer_state.h"
#include "cluster/rm_stm_types.h"
#include "cluster/state_machine_registry.h"
#include "cluster/topic_table.h"
#include "cluster/tx_utils.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine.h"
#include "storage/snapshot.h"
#include "utils/available_promise.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"
#include "utils/tracking_allocator.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <string_view>
#include <system_error>

namespace mt = util::mem_tracked;

struct rm_stm_test_fixture;

namespace cluster {

/**
 * RM stands for resource manager. There is a RM for each partition. It's a
 * part of the transactional sub system.
 *
 * Resource manager:
 *   - tracks all transactions affecting a partition
 *   - maintains the last stable offset
 *   - keeps a list of the aborted transactions
 *   - enforces monotonicity of the sequential numbers
 *   - fences against old epochs
 */
class rm_stm final : public raft::persisted_stm<> {
public:
    static constexpr std::string_view name = "rm_stm";

    using producers_t
      = mt::map_t<absl::btree_map, model::producer_id, tx::producer_ptr>;

    explicit rm_stm(
      ss::logger&,
      raft::consensus*,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<tx::producer_state_manager>&,
      std::optional<model::vcluster_id>);

    ss::future<checked<model::term_id, tx_errc>> begin_tx(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id);
    ss::future<tx_errc> commit_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    ss::future<tx_errc> abort_tx(
      model::producer_identity, model::tx_seq, model::timeout_clock::duration);
    /**
     * Returns the next after the last one decided. If there are no ongoing
     * transactions this will return next offset to be applied to the the stm.
     */
    model::offset last_stable_offset();
    ss::future<fragmented_vector<tx::tx_range>>
      aborted_transactions(model::offset, model::offset);

    /**
     * Returns highest producer ID of any batch that has been applied to this
     * partition. Note that the corresponding transactions may or may not have
     * been committed or aborted; the only certainty of this ID is that it has
     * been used.
     *
     * Callers should be wary to either ensure that the stm is synced before
     * calling, or ensure that the producer_id doesn't need to reflect batches
     * later than the max_collectible_offset.
     */
    model::producer_id highest_producer_id() const;

    model::offset max_collectible_offset() override {
        const auto lso = last_stable_offset();
        if (lso < model::offset{0}) {
            return model::offset{};
        }
        /**
         * Since the LSO may be equal to `_next` we must return offset which has
         * already been decided and applied hence we subtract one from the last
         * stable offset.
         */
        return model::prev_offset(lso);
    }

    storage::stm_type type() override {
        return storage::stm_type::transactional;
    }

    ss::future<fragmented_vector<model::tx_range>>
    aborted_tx_ranges(model::offset from, model::offset to) override {
        return aborted_transactions(from, to);
    }

    model::control_record_type
    parse_tx_control_batch(const model::record_batch&) override;

    kafka_stages replicate_in_stages(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    ss::future<result<kafka_result>> replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options);

    ss::future<ss::basic_rwlock<>::holder> prepare_transfer_leadership();

    ss::future<> stop() override;

    ss::future<> start() override;

    void testing_only_disable_auto_abort() { _is_autoabort_enabled = false; }

    ss::future<result<tx::transaction_set>> get_transactions();

    ss::future<std::error_code> mark_expired(model::producer_identity pid);

    ss::future<> remove_persistent_state() override;

    uint64_t get_local_snapshot_size() const override;

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    const producers_t& get_producers() const { return _producers; }

protected:
    ss::future<> apply_raft_snapshot(const iobuf&) final;

private:
    void setup_metrics();
    ss::future<> do_remove_persistent_state();
    ss::future<fragmented_vector<tx::tx_range>>
      do_aborted_transactions(model::offset, model::offset);
    tx::producer_ptr create_or_get_producer(model::producer_identity);
    void cleanup_producer_state(model::producer_identity);
    ss::future<> reset_producers();

    ss::future<checked<model::term_id, tx_errc>> do_begin_tx(
      model::producer_identity,
      model::tx_seq,
      std::chrono::milliseconds,
      model::partition_id,
      tx::producer_ptr);

    ss::future<tx_errc> do_commit_tx(
      model::producer_identity,
      model::tx_seq,
      model::timeout_clock::duration,
      tx::producer_ptr);
    ss::future<tx_errc> do_abort_tx(
      model::producer_identity,
      std::optional<model::tx_seq> expected_seq,
      tx::producer_ptr,
      model::timeout_clock::duration);
    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot> take_local_snapshot() override;
    ss::future<raft::stm_snapshot> do_take_local_snapshot(uint8_t version);
    ss::future<std::optional<tx::abort_snapshot>>
      load_abort_snapshot(tx::abort_index);
    ss::future<> save_abort_snapshot(tx::abort_snapshot);

    ss::future<result<kafka_result>> do_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> transactional_replicate(
      model::batch_identity, model::record_batch_reader);

    ss::future<result<kafka_result>> do_sync_and_transactional_replicate(
      tx::producer_ptr, model::batch_identity, model::record_batch_reader);

    ss::future<result<kafka_result>> do_transactional_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader);

    ss::future<result<kafka_result>> idempotent_replicate(
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<result<kafka_result>> do_idempotent_replicate(
      model::term_id,
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units&);

    ss::future<result<kafka_result>> do_sync_and_idempotent_replicate(
      tx::producer_ptr,
      model::batch_identity,
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>,
      ssx::semaphore_units);

    ss::future<result<kafka_result>> replicate_msg(
      model::record_batch_reader,
      raft::replicate_options,
      ss::lw_shared_ptr<available_promise<>>);

    ss::future<bool> sync(model::timeout_clock::duration);
    constexpr bool check_tx_permitted() { return true; }

    ss::future<> do_abort_expired_txes();
    ss::future<> try_abort_old_tx(tx::producer_ptr);
    ss::future<tx_errc> do_try_abort_old_tx(tx::producer_ptr);

    abort_origin get_abort_origin(tx::producer_ptr, model::tx_seq) const;

    ss::future<> apply(const model::record_batch&) override;
    void apply_fence(model::producer_identity, model::record_batch);
    void apply_control(
      const model::record_batch&,
      model::producer_identity,
      model::control_record_type);
    void apply_data(const model::record_batch&);

    ss::future<> reduce_aborted_list();
    ss::future<> offload_aborted_txns();
    void abort_old_txes();

    /**
     * Return when the committed offset has been established when STM starts.
     */
    ss::future<model::offset> bootstrap_committed_offset();

    util::mem_tracker _tx_root_tracker{"tx-mem-root"};
    // The state of this state machine maybe change via two paths
    //
    //   - by reading the already replicated commands from raft and
    //     applying them in sequence (the classic RSM approach)
    //
    //   - by applying a command before replicating it accepting the
    //     risk that the replication may fail
    //
    // It's error prone to let this two stream of changes to modify
    // the same state e.g. in that case the pre-replicated command
    // may override legit state, fail and cause an anomaly.
    //
    // We use a segregated state to avoid this problem and reconcile the
    // different part of the state when it's needed. log_state is used
    // to replay replicated commands and mem_state to keep the effect of
    // not replicated yet commands.

    template<class T>
    using allocator = util::tracking_allocator<T>;

    // todo :rename to aborted snapshot tracker
    struct log_state {
        fragmented_vector<tx::tx_range> aborted;
        fragmented_vector<tx::abort_index> abort_indexes;
        tx::abort_snapshot last_abort_snapshot{.last = model::offset(-1)};
    };

    kafka::offset from_log_offset(model::offset old_offset) const;
    model::offset to_log_offset(kafka::offset new_offset) const;

    chunked_vector<tx::producer_ptr> get_expired_producers() const;

    uint8_t active_snapshot_version();

    template<class T>
    void fill_snapshot_wo_seqs(T&);

    bool is_transaction_partitioning() const {
        return _feature_table.local().is_active(
          features::feature::transaction_partitioning);
    }

    // Defines the commit offset range for the stm bootstrap.
    // Set on first apply upcall and used to identify if the
    // stm is still replaying the log.
    std::optional<model::offset> _bootstrap_committed_offset;
    ss::basic_rwlock<> _state_lock;
    bool _is_abort_idx_reduction_requested{false};

    log_state _log_state;
    ss::timer<tx::clock_type> auto_abort_timer;
    std::chrono::milliseconds _sync_timeout;
    std::chrono::milliseconds _tx_timeout_delay;
    std::chrono::milliseconds _abort_interval_ms;
    uint32_t _abort_index_segment_size;
    bool _is_autoabort_enabled{true};
    bool _is_autoabort_active{false};
    bool _is_tx_enabled{false};
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    storage::snapshot_manager _abort_snapshot_mgr;
    absl::flat_hash_map<std::pair<model::offset, model::offset>, uint64_t>
      _abort_snapshot_sizes{};
    ss::sharded<features::feature_table>& _feature_table;
    config::binding<std::chrono::seconds> _log_stats_interval_s;
    prefix_logger _ctx_log;
    ss::sharded<tx::producer_state_manager>& _producer_state_manager;
    std::optional<model::vcluster_id> _vcluster_id;

    // All producers, idempotent and transactional
    // Entries in this map are keyed by "producer_id". This ensures that
    // only a single active producer epoch exists for a given id. This
    // behavior is used to implement fencing for transactional producers
    // (latest epoch wins). For idempotent producers, every session has a unique
    // producer_id (with epoch=0) which ensures that concurrent idempotent
    // sessions have different entries in this map.
    producers_t _producers;

    // Producers that have currently active transactions on this partition.
    // Entries are sorted by begin offset of the transaction, earliest first.
    using active_transactions_t = intrusive_list<
      tx::producer_state,
      &tx::producer_state::_active_transactions_hook>;
    active_transactions_t _active_transactional_producers;

    metrics::internal_metric_groups _metrics;
    ss::abort_source _as;
    ss::gate _gate;
    // Highest producer ID applied to this stm.
    model::producer_id _highest_producer_id;
    // for monotonicity of computed LSO.
    model::offset _last_known_lso{-1};

    friend struct ::rm_stm_test_fixture;
};

class rm_stm_factory : public state_machine_factory {
public:
    rm_stm_factory(
      bool enable_transactions,
      bool enable_idempotence,
      ss::sharded<tx_gateway_frontend>&,
      ss::sharded<cluster::tx::producer_state_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::topic_table>&);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;

private:
    bool _enable_transactions;
    bool _enable_idempotence;
    ss::sharded<tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<cluster::tx::producer_state_manager>& _producer_state_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<topic_table>& _topics;
};

} // namespace cluster
