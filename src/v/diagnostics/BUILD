load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "fwd",
    hdrs = ["fwd.h"],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "types",
    hdrs = ["types.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event_buffer",
    srcs = ["event_buffer.cc"],
    hdrs = ["event_buffer.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@boost//:circular_buffer",
        "@boost//:intrusive",
        "@fmt",
        "@seastar",
    ],
)
