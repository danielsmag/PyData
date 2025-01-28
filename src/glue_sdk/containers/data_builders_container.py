from dependency_injector import containers, providers

from ..builders import DataLoader,DataWriter

class DataBuilderContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    core = providers.DependenciesContainer()
    cache = providers.DependenciesContainer()
    data_catalog_container = providers.DependenciesContainer()
    aurora_pg_container = providers.DependenciesContainer()
    spark_container = providers.DependenciesContainer()
    
    data_loader = providers.Factory(
        provides=DataLoader,
        data_catalog_service=data_catalog_container.data_catalog_service,
        aurora_pg_service=aurora_pg_container.aurora_pg_service,
        cache=cache.cache
    )
    
    data_writer = providers.Factory(
        provides=DataWriter,
        data_catalog_service=data_catalog_container.data_catalog_service,
        aurora_pg_service=aurora_pg_container.aurora_pg_service,
        spark_base_service=spark_container.base_service,
        cache=cache.cache
    )
 
    
    