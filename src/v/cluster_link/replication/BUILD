load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "deps",
    hdrs = [
        "deps.h",
    ],
    include_prefix = "cluster_link/replication",
    visibility = ["__subpackages__"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/raft",
    ],
)

redpanda_cc_library(
    name = "partition_replicator",
    srcs = [
        "partition_replicator.cc",
    ],
    hdrs = [
        "partition_replicator.h",
    ],
    include_prefix = "cluster_link/replication",
    visibility = ["__subpackages__"],
    deps = [
        ":deps",
        "//src/v/cluster_link:logger",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:named_type",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)
