/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "topic_table.h"

#include <boost/iterator/iterator_adaptor.hpp>

namespace cluster {

class topics_state_changed_exception final : public std::exception {
    static constexpr std::string_view ex
      = "topics map updated, expected revision: {}, current revision: {}";

public:
    explicit topics_state_changed_exception(
      model::revision_id expected, model::revision_id current) noexcept
      : _msg(fmt::format(ex, expected, current)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

using underlying_const_it = topic_table::underlying_t::const_iterator;

class topics_iterator
  : public boost::iterator_adaptor<topics_iterator, underlying_const_it> {
    using base = boost::iterator_adaptor<topics_iterator, underlying_const_it>;

public:
    explicit topics_iterator(
      const topic_table& topic_table, underlying_const_it base)
      : topics_iterator::iterator_adaptor_(base)
      , _topic_table(topic_table)
      , _stable_topics_map_revision(_topic_table.get().topics_map_revision()) {}

private:
    void check_revision() const {
        if (unlikely(
              _stable_topics_map_revision
              != _topic_table.get().topics_map_revision())) {
            throw topics_state_changed_exception(
              _stable_topics_map_revision,
              _topic_table.get().topics_map_revision());
        }
    }

    void advance(base::difference_type diff) {
        check_revision();
        std::advance(base_reference(), diff);
    }

    void increment() {
        check_revision();
        base_reference()++;
    }

    base::reference dereference() const {
        check_revision();
        return *base_reference();
    }

    friend class boost::iterator_core_access;
    std::reference_wrapper<topic_table const> _topic_table;
    model::revision_id _stable_topics_map_revision;
};

inline auto topics_begin(const topic_table& table) {
    return topics_iterator(table, table.topics_map().begin());
}

inline auto topics_end(const topic_table& table) {
    return topics_iterator(table, table.topics_map().end());
}

} // namespace cluster