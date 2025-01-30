from dependency_injector import containers, providers

from .opensearch_container import OpenSearchContainer
from .data_catalog_container import DataCatalogContainer
from .core_container import CoreContainer
from .cache_container import CacheContainer
from .spark_container import SparkContainer
from .aurora_pg_container import AuroraPgContainer
from .data_builders_container import DataBuilderContainer
from ..core.shared import AwsServicesToUse

class ApplicationContainer(containers.DeclarativeContainer):

    wiring_config = containers.WiringConfiguration(
    packages=["glue_sdk"] 
    )  
    
    aws_services_to_use = providers.Object(provides=AwsServicesToUse())
    
    config  = providers.Configuration()
    
       
    core = providers.Container(
        container_cls=CoreContainer,
        config=config
    )

    spark = providers.Container(
        container_cls=SparkContainer,
        core=core,
        config=config
    )
    
    general = providers.Container(
        container_cls=SparkContainer,
        config=config,
        core=core
    )

    cache = providers.Container(
        container_cls=CacheContainer,
        config=config
    ) if aws_services_to_use().USE_CACHE else None
    
    data_catalog = providers.Container(
        container_cls=DataCatalogContainer,
        config=config,
        core=core,
        cache=cache
    ) if aws_services_to_use().USE_DATA_CATALOG else None
    
    opensearch = providers.Container(
        container_cls=OpenSearchContainer,
        config=config.opensearch,
        core=core
    ) if aws_services_to_use().USE_OPENSEARCH else None
    
    aurora_pg = providers.Container(
        container_cls=AuroraPgContainer,
        config=config.aurora_pg,
        core=core
    ) if aws_services_to_use().USE_AURORA_PG else None 
    
    
    data_builders = providers.Container(
        container_cls=DataBuilderContainer,
        config=config,
        core=core,
        cache=cache,
        data_catalog_container=data_catalog,
        aurora_pg_container=aurora_pg,
        spark_container=spark
    ) if aws_services_to_use().USE_DATA_BUILDERS else None