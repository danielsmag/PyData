from dependency_injector import containers, providers
from glue_sdk.services import SparkBaseService



class SparkContainer(containers.DeclarativeContainer):
    
    core = providers.DependenciesContainer()
    config = providers.Configuration()       
    app_settings = providers.Dependency()
    spark_client = providers.Dependency()
   
    
    base_service= providers.Factory(
        provides=SparkBaseService
    )
         
    