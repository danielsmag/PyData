from __future__ import annotations
from opensearchpy import OpenSearch
import pytest
from glue_sdk.cache.shared_data_service import SharedDataService
from glue_sdk.containers import ApplicationContainer
from botocore.client import BaseClient
from glue_sdk import SdkManager, SdkConf
from glue_sdk.sdk.sdk_cache import SdkCache


conf = SdkConf()
conf.set_services_to_use(USE_CACHE=True, USE_DATA_CATALOG=True, USE_OPENSEARCH=True)
sdk = SdkManager(config=conf)
sdk.initialize()
container: ApplicationContainer = sdk.container
container.config.override({"test": True})


def test_cache_container():
    from glue_sdk.cache.shared_data_service import SharedDataService

    cache: SdkCache = sdk.cache
    cache_obj: SharedDataService = cache.cache_obj
    assert isinstance(cache_obj, SharedDataService)


# def test_opensearch():
#     op_client: OpenSearch = sdk.opensearch.client
#     assert isinstance(op_client, OpenSearch)


def test_clients() -> None:
    conf = SdkConf()
    # conf.set_services_to_use(
    #     USE_CACHE=False,
    #     USE_DATA_CATALOG=False,
    #     USE_OPENSEARCH=True,
    #     USE_GLUE=True,
    #     USE_SPARK=True
    # )
    glue_client = container.general.glue_client()
    assert isinstance(glue_client, BaseClient)

    s3_client = container.general.s3_client()
    assert isinstance(s3_client, BaseClient)

    secret_client = container.general.secret_client()
    assert isinstance(secret_client, BaseClient)


def test_core_container() -> None:
    from glue_sdk.spark.clients.spark_client import SparkClient
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession
    from pyspark.context import SparkContext

    spark_client = container.core.spark_client()
    assert isinstance(spark_client, SparkClient)

    spark_session = container.core.spark_session()
    assert isinstance(spark_session, SparkSession)

    spark_context = container.core.spark_context()
    assert isinstance(spark_context, SparkContext)

    glue_context = container.core.glue_context()
    assert isinstance(glue_context, GlueContext)

    # spark_client.stop()


if __name__ == "__main__":
    pytest.main()
