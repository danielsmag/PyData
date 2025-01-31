from dependency_injector import containers, providers

from .opensearch_container import OpenSearchContainer
from .data_catalog_container import DataCatalogContainer
from .core_container import CoreContainer
from .cache_container import CacheContainer
from .spark_container import SparkContainer
from .aurora_pg_container import AuroraPgContainer
from .data_builders_container import DataBuilderContainer
from .general_container import GeneralContainer

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
        config=config
    )
    
    general = providers.Container(
        container_cls=GeneralContainer,
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
        core=core
    ) 
            
    opensearch = providers.Container(
        container_cls=OpenSearchContainer,
        config=config.opensearch,
        core=core
    ) 
    
    aurora_pg = providers.Container(
        container_cls=AuroraPgContainer,
        config=config.aurora_pg,
        core=core
    ) 
    
    
    # data_builders = providers.Container(
    #     container_cls=DataBuilderContainer,
    #     config=config,
    #     custom_dependencies=providers.DependenciesContainer(
    #         data_catalog_service=data_catalog.provided.data_catalog_service if data_catalog != providers.Object(None) else None, # type: ignore
    #         aurora_pg_service=aurora_pg.provided.aurora_pg_service if aurora_pg != providers.Object(None) else None, # type: ignore
    #         cache=cache.provided.cache if cache != providers.Object(None) else None, # type: ignore
    #         spark_base_service=spark.provided.base_service if spark != providers.Object(None) else None # type: ignore
    #     )
    # ) if aws_services_to_use().USE_DATA_BUILDERS else providers.Object(None)
 
    
    # print("Initializing aurora_pg ", aurora_pg,DataBuilderContainer)
    data_builders = providers.Container(
        container_cls=DataBuilderContainer,
        config=config,
        core=core,
        cache=cache ,
        data_catalog_container=data_catalog ,
        aurora_pg_container=aurora_pg ,
        spark_container=spark
    )
    
    