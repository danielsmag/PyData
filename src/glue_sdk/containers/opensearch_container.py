from dependency_injector import containers, providers
from typing import List

from ..opensearch import (
    OpenSearchClient,
    OpenSearchService,
    OpenSearchGlueWorker,
    OpenSearchPySparkWorker
)


__all__:List[str] = ["OpenSearchContainer"]
class OpenSearchContainer(containers.DeclarativeContainer):
    
    core = providers.DependenciesContainer()
    config = providers.Configuration()       
    app_settings = providers.Dependency()
    
    opensearch_secrets = providers.Callable(
        provides=lambda secret_service, secret_name: secret_service.fetch_secrets(secret_name),
        secret_service=core.secret_service,
        secret_name=app_settings.provided.opensearch.opensearch_secret_name
    )

    opensearch_client_factory  = providers.Factory(
        provides=OpenSearchClient,
        opensearch_host=config.opensearch_host,
        opensearch_user=opensearch_secrets.provided.username,
        opensearch_pass=opensearch_secrets.provided.password,
    )

    # opensearch_client = providers.Resource(
    #     provides=lambda client_wrapper: client_wrapper.create_client(),
    #     client_wrapper=opensearch_client_factory
    # )
    opensearch_client = providers.Resource(
        provides=opensearch_client_factory.provided.client,
    )
    
    opensearch_pyspark_worker = providers.Factory(
        provides=OpenSearchPySparkWorker,
        host=config.opensearch_host,
        username=opensearch_secrets.provided.username,
        password=opensearch_secrets.provided.password,
        opensearch_config=config
    )
    
    opensearch_glue_worker = providers.Factory(
        provides=OpenSearchPySparkWorker,
        glue_context=core.glue_context,
        opensearch_config=config
    )
    
    opensearch_service = providers.Factory(
        provides=OpenSearchService,
        s3_client=core.s3_client,
        opensearch_config=config,
        glue_context=core.glue_context,
        opensearch_client=opensearch_client,
        opensearch_pyspark_worker=opensearch_pyspark_worker,
        worker2=opensearch_glue_worker
    )