import pytest
from glue_sdk.core.app_settings import Settings, Settings, get_settings_from_s3
from glue_sdk.containers import ApplicationContainer
from botocore.client import BaseClient

app_settings: Settings = get_settings_from_s3(
    env="test",
    prefix="",
    version="1.0.0"
)
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../glue_sdk/src')))
from glue_sdk import SdkManager


sdk = SdkManager()
sdk.initialize()
sdk.config.set_services_to_use(
    USE_CACHE=True
)


container: 'ApplicationContainer' = sdk.container
container.config.override({'test': True})


def test_cache_container():
    from glue_sdk.services import SharedDataService,ICache
    cache = container.cache.cache()
    assert isinstance(cache,SharedDataService)
    assert isinstance(cache,ICache)
    

def test_clients():
    
    
    glue_client = container.core.glue_client()
    assert isinstance(glue_client,BaseClient)

    s3_client = container.general.s3_client()
    assert isinstance(s3_client,BaseClient)
    
    secret_client = container.general.secret_client()
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