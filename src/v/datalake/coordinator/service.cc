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

#include "datalake/coordinator/service.h"

#include "datalake/coordinator/frontend.h"

namespace datalake::coordinator::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<frontend>* frontend)
  : impl::datalake_coordinator_rpc_service(sg, smp_sg)
  , _frontend(frontend) {}

ss::future<add_translated_data_files_reply> service::add_translated_data_files(
  add_translated_data_files_request request, ::rpc::streaming_context&) {
    return _frontend->local().add_translated_data_files(
      std::move(request), frontend::local_only::yes);
}

ss::future<fetch_latest_data_file_reply> service::fetch_latest_data_file(
  fetch_latest_data_file_request request, ::rpc::streaming_context&) {
    return _frontend->local().fetch_latest_data_file(
      std::move(request), frontend::local_only::yes);
}

}; // namespace datalake::coordinator::rpc
