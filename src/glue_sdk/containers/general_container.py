from dependency_injector import containers, providers
from glue_sdk.clients.secret_client import SecretClient
from glue_sdk.services.secret_service import SecretService
from glue_sdk.services.s3_service import S3Service

__all__: list[str] = ['General']

class General(containers.DeclarativeContainer):
    config = providers.Configuration()
    core = providers.DependenciesContainer()
    clients = providers.Dependency()
     
    secret_service = providers.Factory(
        provides=SecretService,
        secret_client=clients.provided.secret_client
    )
    
    s3_service = providers.Factory(
        provides=S3Service,
        s3_client=clients.provided.s3_client
    )