from dependency_injector import containers, providers
from ..general import (
                        S3Client,
                        SecretClient,
                        SecretService,
                        S3Service,
                        GlueClient
                       )


__all__: list[str] = ['General']

class General(containers.DeclarativeContainer):
    config = providers.Configuration()
    core = providers.DependenciesContainer()
    
    secret_client = providers.Callable(
        provides=lambda: SecretClient().create_client()
    )
    
    s3_client = providers.Callable(
        provides=lambda: S3Client().create_client()
    )
    
    secret_service = providers.Factory(
        provides=SecretService,
        secret_client=secret_client
    )
    
    s3_service = providers.Factory(
        provides=S3Service,
        s3_client=s3_client
    )
    
    glue_client_factory = providers.Factory(
        provides=GlueClient,
        
    )
    
    glue_client = providers.Factory(
       provides=lambda glue: glue.client,
        glue=glue_client_factory
    )
    
   
    
    