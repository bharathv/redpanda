/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/errc.h"
#include "cluster/tx_errc.h"
#include "kafka/protocol/errors.h"

namespace kafka {

constexpr error_code map_topic_error_code(cluster::errc code) {
    switch (code) {
    case cluster::errc::success:
        return error_code::none;
    case cluster::errc::topic_invalid_config:
        return error_code::invalid_config;
    case cluster::errc::topic_invalid_partitions:
    case cluster::errc::topic_invalid_partitions_core_limit:
    case cluster::errc::topic_invalid_partitions_memory_limit:
    case cluster::errc::topic_invalid_partitions_fd_limit:
    case cluster::errc::topic_invalid_partitions_decreased:
        return error_code::invalid_partitions;
    case cluster::errc::topic_invalid_replication_factor:
        return error_code::invalid_replication_factor;
    case cluster::errc::notification_wait_timeout:
        return error_code::request_timed_out;
    case cluster::errc::not_leader_controller:
    case cluster::errc::no_leader_controller:
        return error_code::not_controller;
    case cluster::errc::topic_already_exists:
        return error_code::topic_already_exists;
    case cluster::errc::topic_not_exists:
        return error_code::unknown_topic_or_partition;
    case cluster::errc::source_topic_still_in_use:
        return error_code::cluster_authorization_failed;
    case cluster::errc::timeout:
        return error_code::request_timed_out;
    case cluster::errc::invalid_topic_name:
        return error_code::invalid_topic_exception;
    case cluster::errc::no_eligible_allocation_nodes:
        return error_code::broker_not_available;
    case cluster::errc::not_leader:
        return error_code::not_coordinator;
    case cluster::errc::invalid_request:
        return error_code::invalid_request;
    case cluster::errc::throttling_quota_exceeded:
        return error_code::throttling_quota_exceeded;
    case cluster::errc::no_update_in_progress:
        return error_code::no_reassignment_in_progress;
    case cluster::errc::topic_disabled:
    case cluster::errc::resource_is_being_migrated:
    case cluster::errc::partition_disabled:
        return error_code::policy_violation;
    case cluster::errc::replication_error:
    case cluster::errc::shutting_down:
    case cluster::errc::join_request_dispatch_error:
    case cluster::errc::source_topic_not_exists:
    case cluster::errc::seed_servers_exhausted:
    case cluster::errc::auto_create_topics_exception:
    case cluster::errc::partition_not_exists:
    case cluster::errc::partition_already_exists:
    case cluster::errc::waiting_for_recovery:
    case cluster::errc::waiting_for_reconfiguration_finish:
    case cluster::errc::update_in_progress:
    case cluster::errc::user_exists:
    case cluster::errc::user_does_not_exist:
    case cluster::errc::invalid_producer_epoch:
    case cluster::errc::sequence_out_of_order:
    case cluster::errc::generic_tx_error:
    case cluster::errc::node_does_not_exists:
    case cluster::errc::invalid_node_operation:
    case cluster::errc::invalid_configuration_update:
    case cluster::errc::topic_operation_error:
    case cluster::errc::allocation_error:
    case cluster::errc::partition_configuration_revision_not_updated:
    case cluster::errc::partition_configuration_in_joint_mode:
    case cluster::errc::partition_configuration_leader_config_not_committed:
    case cluster::errc::partition_configuration_differs:
    case cluster::errc::data_policy_already_exists:
    case cluster::errc::data_policy_not_exists:
    case cluster::errc::waiting_for_partition_shutdown:
    case cluster::errc::error_collecting_health_report:
    case cluster::errc::leadership_changed:
    case cluster::errc::feature_disabled:
    case cluster::errc::unknown_update_interruption_error:
    case cluster::errc::cluster_already_exists:
    case cluster::errc::no_partition_assignments:
    case cluster::errc::failed_to_create_partition:
    case cluster::errc::partition_operation_failed:
    case cluster::errc::transform_invalid_create:
    case cluster::errc::transform_invalid_environment:
    case cluster::errc::transform_does_not_exist:
    case cluster::errc::transform_invalid_update:
    case cluster::errc::transform_invalid_source:
    case cluster::errc::trackable_keys_limit_exceeded:
    case cluster::errc::invalid_partition_operation:
    case cluster::errc::concurrent_modification_error:
    case cluster::errc::transform_count_limit_exceeded:
    case cluster::errc::role_exists:
    case cluster::errc::role_does_not_exist:
    case cluster::errc::inconsistent_stm_update:
    case cluster::errc::waiting_for_shard_placement_update:
    case cluster::errc::producer_ids_vcluster_limit_exceeded:
    case cluster::errc::validation_of_recovery_topic_failed:
    case cluster::errc::replica_does_not_exist:
    case cluster::errc::invalid_data_migration_state:
    case cluster::errc::data_migration_already_exists:
    case cluster::errc::data_migration_not_exists:
    case cluster::errc::data_migration_invalid_resources:
    case cluster::errc::unknown_producer_id:
        break;
    }
    return error_code::unknown_server_error;
}

constexpr error_code map_tx_errc(cluster::tx::errc ec) {
    switch (ec) {
    case cluster::tx::errc::none:
        return error_code::none;
    case cluster::tx::errc::leader_not_found:
    case cluster::tx::errc::shard_not_found:
    case cluster::tx::errc::partition_not_found:
    case cluster::tx::errc::stm_not_found:
    case cluster::tx::errc::partition_not_exists:
    case cluster::tx::errc::not_coordinator:
    case cluster::tx::errc::stale:
        return error_code::not_coordinator;
    case cluster::tx::errc::coordinator_not_available:
        return error_code::coordinator_not_available;
    case cluster::tx::errc::pid_not_found:
        return error_code::unknown_producer_id;
    case cluster::tx::errc::timeout:
        return error_code::request_timed_out;
    case cluster::tx::errc::conflict:
        return error_code::invalid_txn_state;
    case cluster::tx::errc::fenced:
    case cluster::tx::errc::invalid_producer_epoch:
        return error_code::invalid_producer_epoch;
    case cluster::tx::errc::invalid_txn_state:
        return error_code::invalid_txn_state;
    case cluster::tx::errc::tx_id_not_found:
    case cluster::tx::errc::tx_not_found:
        return error_code::transactional_id_not_found;
    case cluster::tx::errc::invalid_producer_id_mapping:
        return error_code::invalid_producer_id_mapping;
    case cluster::tx::errc::partition_disabled:
        return error_code::replica_not_available;
    case cluster::tx::errc::concurrent_transactions:
        return error_code::concurrent_transactions;
    case cluster::tx::errc::preparing_rebalance:
    case cluster::tx::errc::rebalance_in_progress:
        return error_code::rebalance_in_progress;
    case cluster::tx::errc::coordinator_load_in_progress:
        return error_code::coordinator_load_in_progress;
    case cluster::tx::errc::request_rejected:
    case cluster::tx::errc::unknown_server_error:
        return error_code::unknown_server_error;
    case cluster::tx::errc::invalid_timeout:
        return error_code::invalid_transaction_timeout;
    }
}

} // namespace kafka
