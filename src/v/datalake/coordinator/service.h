// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "datalake/coordinator/rpc_service.h"
#include "datalake/fwd.h"

namespace datalake::coordinator::rpc {
class service final : public impl::datalake_coordinator_rpc_service {
public:
    service(
      ss::scheduling_group, ss::smp_service_group, ss::sharded<frontend>*);

    ss::future<add_translated_data_files_reply> add_translated_data_files(
      add_translated_data_files_request, ::rpc::streaming_context&) override;

    ss::future<fetch_latest_data_file_reply> fetch_latest_data_file(
      fetch_latest_data_file_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<frontend>* _frontend;
};
} // namespace datalake::coordinator::rpc
