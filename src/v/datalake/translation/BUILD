load("//bazel:build.bzl", "redpanda_cc_library")


redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/serde",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "state_machine.cc",
    ],
    hdrs = [
        "state_machine.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/cluster",
        "//src/v/datalake:logger",
        "//src/v/raft",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "partition_translator",
    srcs = [
        "partition_translator.cc",
    ],
    hdrs = [
        "partition_translator.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        ":stm",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/base",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:future_util",
        "//src/v/datalake:types",
        "//src/v/datalake:logger",
        "//src/v/utils:prefix_logger",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/kafka/server",
        "//src/v/datalake/coordinator:frontend",
        "@seastar",
    ],
)
