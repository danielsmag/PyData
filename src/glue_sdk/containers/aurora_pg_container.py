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
        db_user=aurora_pg_secrets.provided.username,
        db_password=aurora_pg_secrets.provided.password,
        db_name=config.db_name,
        sslmode=config.sslmode
    )

    aurora_pg_client = providers.Resource(
        provides=lambda client_wrapper: client_wrapper.create_client(),
        client_wrapper=aurora_pg_client_factory
    ) 
    
    glue_aurora_pg_worker = providers.Factory(
        provides=GlueAuroraPgWorker,
        glue_context=core.glue_context,
        config=config,
        aurora_pg_client=aurora_pg_client,
        connection_name=config.connection_name
    ) 
    
    pyspark_aurora_pg_worker = providers.Factory(
        provides=PySparkAuroraPgWorker,
        config=config,
        spark=core.spark_session,
        jdbc_url=config.jdbc_url,
        db_user=aurora_pg_secrets.provided.username  ,
        db_password=aurora_pg_secrets.provided.password,
    ) 
    
    python_aurora_pg_worker = providers.Factory(
        provides=Psycopg2AuroraPgWorker,
        config=config,
        aurora_pg_client=aurora_pg_client,
    )  
    
    aurora_pg_service = providers.Factory(
        provides=AuroraPgService,
        config=config,
       
        connection_name=config.connection_name ,
        python_worker=python_aurora_pg_worker ,
        pyspark_worker=pyspark_aurora_pg_worker,
        glue_worker=glue_aurora_pg_worker 
    )