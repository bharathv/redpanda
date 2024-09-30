/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "datalake/errors.h"
#include "datalake/fwd.h"
#include "model/record_batch_reader.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace datalake::translation {

class partition_translator {
public:
    explicit partition_translator(
      ss::lw_shared_ptr<cluster::partition> partition,
      ss::sharded<coordinator::frontend>* frontend,
      ss::scheduling_group sg,
      size_t reader_max_bytes,
      ssx::semaphore& parallel_translations);

    ss::future<> stop();

private:
    struct backoff {
        static constexpr std::chrono::milliseconds initial_sleep{300};
        explicit backoff(std::chrono::milliseconds max)
          : max_sleep(max) {}
        std::chrono::milliseconds next() {
            current_sleep = std::min(
              std::max(initial_sleep, current_sleep * 2), max_sleep);
            return current_sleep;
        }
        void reset() { current_sleep = std::chrono::milliseconds{1}; }
        std::chrono::milliseconds current_sleep{1};
        std::chrono::milliseconds max_sleep;
    };
    using reconcilation_success = ss::bool_class<struct reconcilation_success>;
    ss::future<reconcilation_success> reconcile_with_coordinator();
    ss::future<> do_translate();
    using translation_success = ss::bool_class<struct translation_success>;
    ss::future<translation_success> do_translate_once();
    ss::future<model::record_batch_reader> make_reader();
    using commit_entry_success = ss::bool_class<struct commit_entry_success>;
    ss::future<commit_entry_success>
    commit_translated_entry(model::offset min, model::offset max);
    model::term_id _term;
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<translation_stm> _stm;
    ss::sharded<coordinator::frontend>* _frontend;
    // Maximum number of bytes read in one go of translation.
    // Memory usage tracking is not super sophisticated here, so we assume
    // all data batches from the reader are buffered in the writer until
    // they are flushed to disk. This is also factored into determining
    // how many parallel translations can run at one point as we operate under
    // a memory budget for all translations (semaphore below).
    size_t _max_bytes_per_reader;
    ssx::semaphore& _parallel_translations;
    using needs_reconciliation = ss::bool_class<struct needs_reconciliation>;
    needs_reconciliation _reconcile{needs_reconciliation::yes};
    ss::gate _gate;
    ss::abort_source _as;
    prefix_logger _logger;
};

} // namespace datalake::translation
