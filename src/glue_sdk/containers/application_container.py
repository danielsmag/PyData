from dependency_injector import containers, providers

from .opensearch_container import OpenSearchContainer
from .data_catalog_container import DataCatalogContainer
from .core_container import Core
from .cache_container import Cache
from .clients_container import Clients

from glue_sdk.containers.aurora_pg_container import AuroraPgContainer
from glue_sdk.containers.data_builders_container import DataBuilderContainer
from glue_sdk.containers.general_container import General


from glue_sdk.core.app_settings import get_settings
from glue_sdk.containers.spark_container import SparkContainer


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
        container_cls=Cache,
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