from dependency_injector import containers, providers
from typing import List

from glue_sdk.opensearch import (
    OpenSearchClient,
    OpenSearchService,
    OpenSearchGlueWorker,
    OpenSearchPySparkWorker,
)

__all__: List[str] = ["OpenSearchContainer"]


class OpenSearchContainer(containers.DeclarativeContainer):

    core = providers.DependenciesContainer()
    config = providers.Configuration()
    general = providers.DependenciesContainer()

    secrets = providers.Callable(
        provides=lambda secret_service, secret_name: secret_service.fetch_secrets(
            secret_name
        ),
        secret_service=general.secret_service,
        secret_name=config.opensearch_secret_name,
    )

    client_factory = providers.Factory(
        provides=OpenSearchClient,
        opensearch_host=config.opensearch_host,
        opensearch_user=secrets.provided.username,
        opensearch_pass=secrets.provided.password,
        opensearch_port=config.opensearch_port,
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        timeout=config.timeout,
        max_retries=config.max_retries,
        retry_on_timeout=config.retry_on_timeout,
    )

    client = providers.Resource(
        provides=lambda client_wrapper: client_wrapper.create_client(),
        client_wrapper=client_factory,
    )
    # opensearch_client = providers.Resource(
    #     provides=opensearch_client_factory.provided.client,
    # )

    pyspark_worker = providers.Factory(
        provides=OpenSearchPySparkWorker,
        host=config.opensearch_host,
        username=secrets.provided.username,
        password=secrets.provided.password,
        opensearch_config=config,
    )

    glue_worker = providers.Factory(
        provides=OpenSearchGlueWorker,
        glue_context=core.glue_context,
        opensearch_config=config,
    )

    service = providers.Factory(
        provides=OpenSearchService,
        opensearch_config=config,
        opensearch_client=client,
        opensearch_pyspark_worker=pyspark_worker,
        opensearch_glue_worker=glue_worker,
    )
