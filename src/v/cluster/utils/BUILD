load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "notifications_api",
    hdrs = [
        "notifications.h",
    ],
    include_prefix = "cluster/utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:notification",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "default_notifications_impl",
    srcs = [
        "notifications_impl.cc",
    ],
    hdrs = [
        "notifications_impl.h",
    ],
    include_prefix = "cluster/utils",
    visibility = ["//visibility:public"],
    deps = [
        ":notifications_api",
        "//src/v/cluster",
        "//src/v/raft",
        "@seastar",
    ],
)
