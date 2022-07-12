// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde.h"

#include <seastar/core/lowres_clock.hh>

namespace serde {

// Copied over from std::chrono::is_clock<T>.
// Not supported in libc++.
template<class T>
concept is_clock = requires {
    typename T::rep;
    typename T::period;
    typename T::duration;
    typename T::time_point;
    T::is_steady;
    T::now();
};

// Only system clocks time points are serializable. This is a loose check
// but good enough, we assume any impls with to_time_t and from_time_t
// are system clocks.
template<class T>
concept is_system_clock = is_clock<typename T::clock> && requires {
    T::clock::to_time_t;
    T::clock::from_time_t;
};

// A serializable time_point type. Generally time points have a risk of
// clock synchronization issues when deserialized on node with a clock drift.
// Rather than letting users serialize arbitrary time_points, we only allow
// serializing this special wrapped type making it easy for us to audit the
// usage.
// A time_point is serialized as the underlying duration epoch.

// Example Usage:
//
// struct container : serde::envelope<container, version<0>> {
//   serde::time_point t = ss::lowres_system_clock::now();
//   ....
// }
// Since serde::time_point itself is a serializable envelope, nothing extra
// needs to be done while serializing/deserializing the outer container.
//
// struct another: serde::envelope<another, version<0>> {
//   ss::lowres_system_clock::time_point tp;
//
//   auto serde_write(iobuf& out) {
//       write(out, serde::time_point(tp));
//   }
//
//   auto serde_read(iobuf_parser& in, header& h) {
//        tp = read_nested<serde::time_point<decltype<tp>>>(in,
//        h._bytes_left_limit);
//   }
// }
//
// When using the underlying timepoint directly, we can serialize it by
// constructing an intermediate serde::time_point type.

template<is_system_clock time_point_type = ss::lowres_system_clock::time_point>
struct time_point : envelope<time_point<time_point_type>, version<0>> {
    using duration = typename time_point_type::duration;
    using clock = typename time_point_type::clock;
    time_point_type _time_point;

    time_point() = default;

    time_point(time_point_type t)
      : _time_point(std::move(t)) {}

    time_point& operator=(time_point_type&& t) {
        _time_point = std::move(t);
        return *this;
    }

    operator time_point_type() const { return _time_point; }

    const time_point_type& operator()() const { return _time_point; }

    auto serde_write(iobuf& out) { write(out, _time_point.time_since_epoch()); }

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        auto dur = read_nested<duration>(in, h._bytes_left_limit);
        _time_point = std::chrono::time_point<clock, duration>(std::move(dur));
    }
};

} // namespace serde