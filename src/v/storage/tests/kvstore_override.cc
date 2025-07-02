#include "cluster/types.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/disk_log_impl.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

FIXTURE_TEST(kv_override, storage_test_fixture) {
    auto cfg = default_log_config("/tmp/zafin/");
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    static const bytes node_uuid_key = "node_uuid";


    // 313b065a-f26a-4524-bde1-a9fed0e76e7e - 3
    // 595878ac-9c7c-4139-9f33-f2ef6f372cf7 - 1
    // 41a1a41e-f948-45ab-8f9c-fabbdc1e522f - 0

    const bytes invariants_key{"configuration_invariants"};
    model::node_uuid node_uuid(
      uuid_t::from_string("313b065a-f26a-4524-bde1-a9fed0e76e7e"));

    // INFO  2023-06-16 03:56:46,097 [shard 0] cluster - members_manager.cc:329
    // - Node UUID {8e9752a6-9795-4b08-9084-4fbd6f5d34c1} has node ID {3}
    kvstore
      .put(
        storage::kvstore::key_space::controller,
        node_uuid_key,
        serde::to_iobuf(node_uuid))
      .get();

    cluster::configuration_invariants invariants(model::node_id(3), 28);

    auto invariants_buffer = reflection::to_iobuf(std::move(invariants));

    kvstore
      .put(
        storage::kvstore::key_space::controller,
        invariants_key,
        std::move(invariants_buffer))
      .get();

    mgr.stop().get();
}

