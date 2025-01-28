from dependency_injector import containers, providers

from .opensearch_container import OpenSearchContainer
from .data_catalog_container import DataCatalogContainer
from .core_container import CoreContainer
from .cache_container import CacheContainer
from .spark_container import SparkContainer
from .aurora_pg_container import AuroraPgContainer
from .data_builders_container import DataBuilderContainer

class ApplicationContainer(containers.DeclarativeContainer):

    wiring_config = containers.WiringConfiguration(
    packages=["glue_sdk"] 
    )  
    
    config  = providers.Configuration()
    
       
    core = providers.Container(
        container_cls=CoreContainer,
        config=config
    )

    spark = providers.Container(
        container_cls=SparkContainer,
        core=core,
        config=config,
        app_settings=app_settings
    )
    
    general = providers.Container(
        container_cls=SparkContainer,
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