from dependency_injector import containers, providers
from ..core.shared import ServicesEnabled
from ..builders import DataLoader,DataWriter

class DataBuilderContainer(containers.DeclarativeContainer):
    
    config = providers.Configuration()
    core = providers.DependenciesContainer()
    cache = providers.DependenciesContainer()
    data_catalog_container = providers.DependenciesContainer()
    aurora_pg_container = providers.DependenciesContainer()
    spark_container = providers.DependenciesContainer()
    custom_dependencies = providers.DependenciesContainer()
    aws_services_to_use = providers.Object(provides=ServicesEnabled())
    # config = providers.Configuration()
    # core = providers.Dependency()  # Instead of DependenciesContainer
    # cache = providers.Dependency() if aws_services_to_use().USE_CACHE else None
    # data_catalog_container = providers.Dependency() if aws_services_to_use().USE_DATA_CATALOG else None
    # aurora_pg_container = providers.Dependency() if aws_services_to_use().USE_AURORA_PG else None
    # # spark_container = providers.Dependency()
    
    # print("aws_services_to_use",aws_services_to_use)
    
    data_loader = providers.Factory(
        provides=DataLoader,
        data_catalog_service=data_catalog_container.data_catalog_service ,
        aurora_pg_service=aurora_pg_container.aurora_pg_service ,
        cache=cache.cache 
    )
    
    # data_loader = providers.Factory(
    #     provides=DataLoader,
    #     data_catalog_service=(
    #         data_catalog_container.provided.data_catalog_service()
    #         if hasattr(data_catalog_container, "provided") else None
    #     ),
    #     aurora_pg_service=(
    #         aurora_pg_container.provided.aurora_pg_service()
    #         if hasattr(aurora_pg_container, "provided") else None
    #     ),
    #     cache=(
    #         cache.provided.cache()
    #         if hasattr(cache, "provided") else None
    #     )
    # )
    
    
    # data_loader = providers.Factory(
    #     DataLoader,
    #     data_catalog_service=providers.Callable(
    #         lambda: custom_dependencies.data_catalog_service() if custom_dependencies.data_catalog_service else None
    #     ),
    #     aurora_pg_service=providers.Callable(
    #         lambda: custom_dependencies.aurora_pg_service() if custom_dependencies.aurora_pg_service else None
    #     ),
    #     cache=providers.Callable(
    #         lambda: custom_dependencies.cache() if custom_dependencies.cache else None
    #     )
    # )
    
    # data_writer = providers.Factory(
    #     DataWriter,
    #     data_catalog_service=providers.Callable(
    #         lambda: dependencies.data_catalog_service() if dependencies.data_catalog_service else None
    #     ),
    #     aurora_pg_service=providers.Callable(
    #         lambda: dependencies.aurora_pg_service() if dependencies.aurora_pg_service else None
    #     ),
    #     spark_base_service=providers.Callable(
    #         lambda: dependencies.spark_base_service() if dependencies.spark_base_service else None
    #     ),
    #     cache=providers.Callable(
    #         lambda: dependencies.cache() if dependencies.cache else None
    #     )
    # )
    
    # print(data_loader,"data_loader")
    data_writer = providers.Factory(
        provides=DataWriter,
        data_catalog_service=data_catalog_container.data_catalog_service,
        aurora_pg_service=aurora_pg_container.aurora_pg_service,
        spark_base_service=spark_container.base_service,
        cache=cache.cache
    )
 
    
    