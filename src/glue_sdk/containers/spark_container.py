from dependency_injector import containers, providers

from ..spark import SparkBaseService


class SparkContainer(containers.DeclarativeContainer):
    
    core = providers.DependenciesContainer()
    config = providers.Configuration()       
    spark_client = providers.Dependency()
   
    
    base_service= providers.Factory(
        provides=SparkBaseService
    )
         
    