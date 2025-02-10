/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/translation/scheduling.h"

namespace datalake::translation {

class partition_translator_v2 : public scheduling::translator {
public:
    explicit partition_translator_v2();

    const scheduling::translator_id& id() const override;
    ss::future<> init(
      scheduling::scheduling_notifications&,
      scheduling::reservations_tracker&) override;
    ss::future<> close() noexcept override;
    scheduling::translation_status status() const override;
    void start_translation(scheduling::clock::duration time_slice) override;
    void stop_translation() override;

private:
};
} // namespace datalake::translation
