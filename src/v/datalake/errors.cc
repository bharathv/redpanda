// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/errors.h"

#include <iostream>

namespace datalake {

std::ostream& operator<<(std::ostream& o, const coordinator_errc& errc) {
    switch (errc) {
    case coordinator_errc::ok:
        o << "coordinator_errc::ok";
        break;
    case coordinator_errc::coordinator_topic_not_exists:
        o << "coordinator_errc::coordinator_topic_not_exists";
        break;
    case coordinator_errc::not_leader:
        o << "coordinator_errc::not_leader";
        break;
    case coordinator_errc::timeout:
        o << "coordinator_errc::timeout";
        break;
    case coordinator_errc::fenced:
        o << "coordinator_errc::fenced";
        break;
    case coordinator_errc::stale:
        o << "coordinator_errc::stale";
        break;
    case coordinator_errc::concurrent_requests:
        o << "coordinator_errc::concurrent_requests";
        break;
    }
    return o;
}

} // namespace datalake
