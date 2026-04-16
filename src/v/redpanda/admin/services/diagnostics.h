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

#include "diagnostics/event_buffer.h"
#include "proto/redpanda/core/admin/v2/diagnostics.proto.h"

namespace admin {

class diagnostics_service_impl : public proto::admin::diagnostics_service {
public:
    explicit diagnostics_service_impl(ss::sharded<diagnostics::event_buffer>&);

    ss::future<proto::admin::get_event_log_response> get_event_log(
      serde::pb::rpc::context, proto::admin::get_event_log_request) override;

    ss::future<proto::admin::get_diagnostics_response> get_diagnostics(
      serde::pb::rpc::context, proto::admin::get_diagnostics_request) override;

private:
    proto::admin::diagnostic_event
    to_proto(const diagnostics::diagnostic_event&) const;

    ss::sharded<diagnostics::event_buffer>& _event_buffer;
};

} // namespace admin
