import os
import time
import random
import json
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import produce_until_segments
from rptest.services.redpanda import SISettings
from rptest.services.cluster import cluster
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.utils.functional import flat_map, flatten
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types, filesystem_catalog_type


class IcebergUsageTest(RedpandaTest):
    """
    Test to check iceberg usage with multiple topics having iceberg enabled.
    """
    def __init__(self, test_ctx, *args, **kwargs):
        extra_rp_conf = dict(iceberg_enabled=True,
                             iceberg_catalog_commit_interval_ms=1000,
                             enable_usage=True,
                             usage_num_windows=30,
                             usage_window_width_interval_sec=1)
        self.test_ctx = test_ctx
        super(IcebergUsageTest,
              self).__init__(test_context=test_ctx,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf=extra_rp_conf,
                             *args,
                             **kwargs)

    def setUp(self):
        pass

    def get_iceberg_usage(self):
        """
        Get iceberg usage from the datalake /usage end point.
        """
        reported_usages = flat_map(
            lambda node: self.redpanda._admin.get_usage(node),
            self.redpanda.nodes)

        self.logger.info(f"Reported usages: {reported_usages}")

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK],
            catalog_type=[filesystem_catalog_type()])
    def test_iceberg_usage_basic(self, cloud_storage_type, query_engine,
                                 catalog_type):
        """
        Test that verifies basic iceberg usage with multiple topics having iceberg enabled.
        Tests storage usage tracking without cluster restarts.
        """
        # Create topics, mix of iceberg and non iceberg enabled
        ib_topics = [f"iceberg_topic_{i}" for i in range(random.randint(1, 5))]
        no_iceberg_topics = [
            f"no_iceberg_topic_{i}" for i in range(random.randint(1, 5))
        ]

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              include_query_engines=[query_engine],
                              catalog_type=catalog_type) as dl:

            for topic in ib_topics:
                dl.create_iceberg_enabled_topic(topic,
                                                partitions=3,
                                                replicas=3,
                                                target_lag_ms=10000)

            for topic in no_iceberg_topics:
                dl.create_iceberg_enabled_topic(topic,
                                                partitions=3,
                                                iceberg_mode="disabled")

            self.get_iceberg_usage()

            for topic in ib_topics:
                dl.produce_to_topic(topic, 100, 100)
                dl.wait_for_translation(topic, 100)

            time.sleep(5)  # Allow some time for data to be uploaded

            self.get_iceberg_usage()
