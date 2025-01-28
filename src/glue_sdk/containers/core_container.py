from dependency_injector import containers, providers
from ..spark.clients.spark_client import SparkClient 
from awsglue.job import Job

class Core(containers.DeclarativeContainer):

    config = providers.Configuration()
    clients = providers.Dependency()
    
    dynamic_configs_spark_client = providers.Factory(
        provides=lambda: {}
    )
    
    spark_client = providers.Singleton(
        provides=SparkClient, 
        env=config.env,
        dynamic_configs_spark_client=dynamic_configs_spark_client
        )
    
    spark_session = providers.Singleton(
        provides=spark_client.provided.spark_session
    )
    
    spark_context = providers.Singleton(
        provides=spark_client.provided.spark_context
  
    )
    
    glue_context = providers.Singleton(
        provides=spark_client.provided.glue_context
    )
    
    glue_job = providers.Singleton(
        provides=lambda glue_context: Job(glue_context), glue_context=glue_context
    )
    
