from dependency_injector import containers, providers
from ..core.shared import AwsServicesToUse
from ..aurora_pg import (
        AuroraPgClient,
        GlueAuroraPgWorker,
        Psycopg2AuroraPgWorker,
        PySparkAuroraPgWorker,
        AuroraPgService
)

__all__: list[str] = ['AuroraPgContainer']

class AuroraPgContainer(containers.DeclarativeContainer):
    
    core = providers.DependenciesContainer()
    config = providers.Configuration()       
    
    aws_services_to_use = providers.Object(AwsServicesToUse())
    
    
    aurora_pg_secrets = providers.Callable(
        provides=lambda secret_service, secret_name: secret_service.fetch_secrets(secret_name),
        secret_service=core.secret_service,
        secret_name=config.secret
    ) 

    aurora_pg_client_factory  = providers.Factory(
        provides=AuroraPgClient,
        db_host=config.db_host,
        db_port=config.db_port,
        db_user=aurora_pg_secrets.provided.username if aws_services_to_use().use_aurora_pg else None,
        db_password=aurora_pg_secrets.provided.password if aws_services_to_use().use_aurora_pg else None,
        db_name=config.db_name,
        sslmode=config.sslmode
    ) if aws_services_to_use().use_aurora_pg else None

    aurora_pg_client = providers.Resource(
        provides=lambda client_wrapper: client_wrapper.create_client(),
        client_wrapper=aurora_pg_client_factory
    ) if aws_services_to_use().use_aurora_pg else None
    
    glue_aurora_pg_worker = providers.Factory(
        provides=GlueAuroraPgWorker,
        glue_context=core.glue_context,
        config=config,
        aurora_pg_client=aurora_pg_client,
        connection_name=config.connection_name
    ) if aws_services_to_use().use_aurora_pg else None
    
    pyspark_aurora_pg_worker = providers.Factory(
        provides=PySparkAuroraPgWorker,
        config=config,
        spark=core.spark_session,
        jdbc_url=config.jdbc_url,
        db_user=aurora_pg_secrets.provided.username  if aws_services_to_use().use_aurora_pg else None,
        db_password=aurora_pg_secrets.provided.password if aws_services_to_use().use_aurora_pg else None
    ) if aws_services_to_use().use_aurora_pg else None
    
    python_aurora_pg_worker = providers.Factory(
        provides=Psycopg2AuroraPgWorker,
        config=config,
        aurora_pg_client=aurora_pg_client,
    )  if aws_services_to_use().use_aurora_pg else None
    
    aurora_pg_service = providers.Factory(
        provides=AuroraPgService,
        config=config,
        # aurora_pg_client=aurora_pg_client,
        connection_name=config.connection_name  if aws_services_to_use().use_aurora_pg else None,
        python_worker=python_aurora_pg_worker  if aws_services_to_use().use_aurora_pg else None,
        pyspark_worker=pyspark_aurora_pg_worker  if aws_services_to_use().use_aurora_pg else None,
        glue_worker=glue_aurora_pg_worker  if aws_services_to_use().use_aurora_pg else None
    )