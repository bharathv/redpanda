load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "explore",
    srcs = [
        "chart.go",
        "dashboard.go",
        "eventlog.go",
        "eventtail.go",
        "metrics.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/explore",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_fatih_color//:color",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
