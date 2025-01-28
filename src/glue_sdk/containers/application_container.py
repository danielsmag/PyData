from dependency_injector import containers, providers

from glue_sdk.containers.opensearch_container import OpenSearchContainer
from glue_sdk.containers.data_catalog_container import DataCatalogContainer
from glue_sdk.containers.aurora_pg_container import AuroraPgContainer
from glue_sdk.containers.data_builders_container import DataBuilderContainer
from glue_sdk.containers.general_container import General
from glue_sdk.containers.clients_container import Clients
from glue_sdk.services.shared_memory_service import SharedDataService
from glue_sdk.core.app_settings import get_settings
from glue_sdk.containers.spark_container import SparkContainer
from glue_sdk.clients import SparkClient

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
    

class CacheContainer(containers.DeclarativeContainer):
    config  = providers.Configuration()
    cache = providers.Singleton(
        provides=SharedDataService,
        cache_timeout=None
    )
    
class ApplicationContainer(containers.DeclarativeContainer):

    wiring_config = containers.WiringConfiguration(
    packages=["glue_sdk"] 
    )  
    
    config  = providers.Configuration()
    
    app_settings = providers.Singleton(provides=get_settings)
    
    clients  = providers.Container(
        container_cls=Clients,
        config=config,
    )
    
    core = providers.Container(
        container_cls=Core,
        config=config,
        clients=clients
    )

    spark = providers.Container(
        container_cls=SparkContainer,
        core=core,
        config=config,
        app_settings=app_settings
    )
    
    general = providers.Container(
        container_cls=General,
        config=config,
        core=core
    )

    cache = providers.Container(
        container_cls=CacheContainer,
        config=config
    )
    
    data_catalog = providers.Container(
        container_cls=DataCatalogContainer,
        config=config,
        clients=clients,
        core=core,
        cache=cache
    )
    
    opensearch = providers.Container(
        container_cls=OpenSearchContainer,
        config=config.opensearch,
        core=core,
        app_settings=app_settings
    )
    
    aurora_pg = providers.Container(
        container_cls=AuroraPgContainer,
        config=config.aurora_pg,
        core=core,
        app_settings=app_settings
    )
    
    data_builders = providers.Container(
        container_cls=DataBuilderContainer,
        config=config,
        core=core,
        cache=cache,
        data_catalog_container=data_catalog,
        aurora_pg_container=aurora_pg,
        spark_container=spark
    )