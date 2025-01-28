from dependency_injector import containers, providers

from glue_sdk.services.data_catalog_service import DataCatalogService

class DataCatalogContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    core = providers.DependenciesContainer()
    cache = providers.DependenciesContainer()
    clients = providers.DependenciesContainer()
    
    
    data_catalog_service = providers.Factory(
        provides=DataCatalogService,
        glue_context=core.glue_context,
        glue_client=clients.glue_client,
        aws_region=config.aws_region
    )

  
 
    
    