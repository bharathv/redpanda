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

#include "redpanda/cluster_config_schema_util.h"

#include <iostream>

namespace po = boost::program_options;

int main(int ac, char* av[]) {
    po::options_description desc("Allowed options");
    desc.add_options()("help", "Allowed options")(
      "config_schema_json", "Generates JSON schema for cluster configuration");
    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
    } else if (vm.count("config_schema_json")) {
        std::cout << util::generate_json_schema(config::configuration())._res
                  << "\n";
    }
    return 0;
}