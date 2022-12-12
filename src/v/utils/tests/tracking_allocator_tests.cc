// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/tracking_allocator.h"

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <boost/test/unit_test.hpp>

using util::mem_tracker;
template<class T>
using allocator = util::tracking_allocator<T>;

BOOST_AUTO_TEST_CASE(allocator_basic) {
    auto root = ss::make_lw_shared<mem_tracker>("root");
    allocator<int> alloc{root};
    auto* p = alloc.allocate(2);
    BOOST_REQUIRE_EQUAL(root->consumption(), 2 * sizeof(int));

    // create a child
    auto child1 = root->create_child("child1");
    allocator<int64_t> alloc1(child1);
    auto* p1 = alloc1.allocate(3);

    BOOST_REQUIRE_EQUAL(child1->consumption(), 3 * sizeof(int64_t));
    BOOST_REQUIRE_EQUAL(
      root->consumption(), 2 * sizeof(int) + 3 * sizeof(int64_t));

    // deallocate
    alloc1.deallocate(p1, 3);
    BOOST_REQUIRE_EQUAL(child1->consumption(), 0);
    BOOST_REQUIRE_EQUAL(root->consumption(), 2 * sizeof(int));
    alloc.deallocate(p, 2);
    BOOST_REQUIRE_EQUAL(root->consumption(), 0);
}

BOOST_AUTO_TEST_CASE(allocator_list_vector) {
    auto tracker = ss::make_lw_shared<mem_tracker>("vec");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        allocator<int> alloc{tracker};
        std::vector<int, allocator<int>> v{alloc};
        v.push_back(1);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), sizeof(int));
        v.push_back(2);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 2 * sizeof(int));
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        allocator<int> alloc{tracker};
        std::list<int, allocator<int>> v{alloc};
        v.push_back(1);
        BOOST_REQUIRE_GE(tracker->consumption(), 0);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
}

template<template<class...> class T>
void test_mem_tracked_map() {
    auto tracker = ss::make_lw_shared<mem_tracker>("map");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        auto map = util::mem_tracked::map<T, int, int>(tracker);
        map[1] = 2;
        BOOST_REQUIRE_GE(tracker->consumption(), 0);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
}

template<template<class...> class T>
void test_mem_tracked_set() {
    auto tracker = ss::make_lw_shared<mem_tracker>("map");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        auto set = util::mem_tracked::set<T, int>(tracker);
        set.insert(1);
        BOOST_REQUIRE_GT(tracker->consumption(), 0);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
}

BOOST_AUTO_TEST_CASE(allocator_stl) {
    test_mem_tracked_map<std::map>();
    test_mem_tracked_map<std::unordered_map>();
    test_mem_tracked_map<absl::flat_hash_map>();
    test_mem_tracked_map<absl::node_hash_map>();

    test_mem_tracked_set<std::set>();
    test_mem_tracked_set<std::unordered_set>();
    test_mem_tracked_set<absl::btree_set>();
    test_mem_tracked_set<absl::flat_hash_set>();
    test_mem_tracked_set<absl::node_hash_set>();
}
