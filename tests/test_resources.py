import pytest
from dependency_injector import providers
from glue_sdk.containers.opensearch_container import OpenSearchContainer
from glue_sdk.containers.data_catalog_container import DataCatalogContainer
from glue_sdk.containers.aurora_pg_container import AuroraPgContainer
from glue_sdk.containers.data_builders_container import DataBuilderContainer
from glue_sdk.containers.general_container import General
from glue_sdk.services.shared_memory_service import SharedDataService
from glue_sdk.core.app_settings import Settings, Settings, get_settings,get_settings_from_s3
from glue_sdk.clients.glue_client import GlueClient
from glue_sdk.clients.s3_client import S3Client
from glue_sdk.clients.secret_client import SecretClient
from glue_sdk.clients.spark_client import SparkClient
from awsglue.job import Job
from glue_sdk.containers import ApplicationContainer
from glue_sdk.decorators import s3_client,glue_client,secret_client
from botocore.client import BaseClient
app_settings: Settings = get_settings_from_s3(
    env="test",
    prefix="",
    version="1.0.0"
)
container = ApplicationContainer()
container.config.override({'test': True})


def test_cache_container():
    from glue_sdk.services import SharedDataService,ICache
    cache = container.cache.cache()
    assert isinstance(cache,SharedDataService)
    assert isinstance(cache,ICache)
    

def test_clients():
    from glue_sdk.clients.base_aws_client import BaseAWSClient
    
    glue_client = container.clients.glue_client()
    assert isinstance(glue_client,BaseClient)

    s3_client = container.clients.s3_client()
    assert isinstance(s3_client,BaseClient)
    
    secret_client = container.clients.secret_client()
    assert isinstance(secret_client,BaseClient)
    
def test_core_container():
    from glue_sdk.clients import SparkClient
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession 
    from pyspark.context import SparkContext  
   
    spark_client = container.core.spark_client()
    assert isinstance(spark_client,SparkClient)
    
    spark_session = container.core.spark_session()
    assert isinstance(spark_session,SparkSession)
    
    spark_context = container.core.spark_context()
    assert isinstance(spark_context,SparkContext)
    
    glue_context = container.core.glue_context()
    assert isinstance(glue_context,GlueContext)
   
if __name__ == "__main__":
    pytest.main()