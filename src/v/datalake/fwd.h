/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

namespace datalake {
namespace coordinator {
class frontend;
};

namespace translation {
class translation_stm;
class partition_translator;
}; // namespace translation

class datalake_manager;
} // namespace datalake
