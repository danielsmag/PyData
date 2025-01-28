from dependency_injector import containers, providers
from glue_sdk.clients import GlueClient
from glue_sdk.clients import S3Client
from glue_sdk.clients import SecretClient



class Clients(containers.DeclarativeContainer):
    config = providers.Configuration()
    
    glue_client_factory = providers.Factory(
        provides=GlueClient,
        
    )
    
    glue_client = providers.Factory(
       provides=lambda glue: glue.client,
        glue=glue_client_factory
    )
    
    s3_client = providers.Callable(
        provides=lambda: S3Client().create_client()
    )
    
    secret_client = providers.Callable(
        provides=lambda: SecretClient().create_client()
    )