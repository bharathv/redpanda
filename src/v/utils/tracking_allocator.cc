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

#include "utils/tracking_allocator.h"

#include "utils/human.h"

#include <sstream>

namespace util {

ss::lw_shared_ptr<mem_tracker> mem_tracker::create_child(ss::sstring label) {
    auto child = ss::make_lw_shared<mem_tracker>(label);
    _children.push_back(*child);
    return child;
}

// Consumption of self and all children.
int64_t mem_tracker::consumption() const {
    int64_t total = _consumption;
    for (const auto& tracker : _children) {
        total += tracker.consumption();
    }
    return total;
}

ss::sstring mem_tracker::debug(int level) const {
    std::stringstream result;
    for (int i = 0; i <= level; i++) {
        result << "| ";
    }
    result << "\n";
    for (int i = 0; i < level; i++) {
        result << "| ";
    }
    result << _label << ": " << human::bytes(static_cast<double>(consumption()))
           << '\n';
    for (const auto& tracker : _children) {
        result << tracker.debug(level + 1);
    }
    return result.str();
}
}; // namespace util