/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

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
