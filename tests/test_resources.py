from __future__ import annotations

import pytest
from glue_sdk.cache.shared_data_service import SharedDataService
from glue_sdk import SdkManager
from botocore.client import BaseClient
from glue_sdk import SdkManager, SdkConf
from glue_sdk.sdk.sdk_cache import SdkCache
from glue_sdk.sdk.sdk_opensearch import SdkOpenSearch
from glue_sdk.cache.shared_data_service import SharedDataService
from glue_sdk.opensearch.opensearch_client import OpenSearchClient


def test_cache_container(sdk: SdkManager):

    cache: SdkCache = sdk.cache
    cache_obj: SharedDataService = cache.cache_obj
    assert isinstance(cache_obj, SharedDataService)


def test_opensearch(sdk: SdkManager):
    op_client: SdkOpenSearch = sdk.opensearch
    op_factory = op_client.client_factory
    conf_from_op = sdk.container.opensearch.config()
    print(conf_from_op, 55555)
    opensearch_config = sdk.opensearch.config
    assert opensearch_config == {
        "index_name": "",
        "index_mapping_s3": "",
        "opensearch_connection_name": "",
        "opensearch_secret_name": "my_secret",
        "opensearch_host": "localhost",
        "opensearch_batch_size_bytes": "10m",
        "opensearch_batch_size_entries": 100,
        "opensearch_mapping_unique_id": None,
        "pushdown": True,
        "es_nodes_wan_only": "true",
        "opensearch_port": 9200,
        "test": 124,
    }
    assert isinstance(op_factory, OpenSearchClient)


def test_clients(sdk: SdkManager) -> None:
    container = sdk.container
    glue_client = container.general.glue_client()
    assert isinstance(glue_client, BaseClient)

    s3_client = container.general.s3_client()
    assert isinstance(s3_client, BaseClient)

    secret_client = container.general.secret_client()
    assert isinstance(secret_client, BaseClient)


def test_core_container(sdk: SdkManager) -> None:
    from glue_sdk.spark.clients.spark_client import SparkClient
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession
    from pyspark.context import SparkContext

    container = sdk.container
    spark_client = container.core.spark_client()
    assert isinstance(spark_client, SparkClient)

    spark_session = container.core.spark_session()
    assert isinstance(spark_session, SparkSession)

    spark_context = container.core.spark_context()
    assert isinstance(spark_context, SparkContext)

    glue_context = container.core.glue_context()
    assert isinstance(glue_context, GlueContext)

    spark_client.stop()


if __name__ == "__main__":
    pytest.main()
