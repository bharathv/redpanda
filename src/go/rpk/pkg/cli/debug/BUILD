load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "debug",
    srcs = [
        "debug.go",
        "explore.go",
        "info.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/debug/bundle",
        "//src/go/rpk/pkg/cli/debug/explore",
        "//src/go/rpk/pkg/cli/debug/remotebundle",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
