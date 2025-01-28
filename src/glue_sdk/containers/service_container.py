# from dependency_injector import containers, providers
# from clients.glue_client import GlueClient
# from clients.s3_client import S3Client
# from clients.secret_client import SecretClient
# from clients.opensearch_client import OpenSearchClient
# from clients.spark_client import SparkClient

# from services.opensearch_service import OpenSearchService
# from services.data_catalog_service import DataCatalogService
# from services.data_catalog_loader import DataCatalogLoader
# from services.shared_memory_service import SharedDataService
# from services.secret_service import SecretService

# from core.app_settings import get_settings 
# from awsglue.job import Job

# from containers.opensearch_container import OpenSearchContainer

# class ServiceContainer(containers.DeclarativeContainer):
   
#     wiring_config = containers.WiringConfiguration(packages=["src"])  

#     config  = providers.Configuration()
#     # main app config
#     app_settings = providers.Singleton(
#         provides=get_settings
#     )
    
#     cache = providers.Singleton(
#         provides=SharedDataService,
#         cache_timeout=None
#     )
    
#     # # spark resources
#     spark_client = providers.Singleton(provides=SparkClient, env=config.provided.env)
#     spark_session = providers.Singleton(
#         provides=lambda spark_client: spark_client.get_spark_session(), spark_client=spark_client
#     )
    
#     #glue resources
#     glue_context = providers.Singleton(
#         provides=lambda spark_client: spark_client.get_glue_context(), spark_client=spark_client
#     )
    
#     glue_job = providers.Singleton(
#         provides=lambda glue_context: Job(glue_context), glue_context=glue_context
#     )
    
#     #clients:
#     glue_client = providers.Callable(
#         provides=lambda: GlueClient().create_client()
#     )
    
#     s3_client = providers.Callable(
#         provides=lambda: S3Client().create_client()
#     )
    
#     secret_client = providers.Callable(
#         provides=lambda: SecretClient().create_client()
#     )
    
#     secret_service = providers.Factory(
#         provides=SecretService,
#         secret_client=secret_client
#     )
    
#     data_catalog_service = providers.Factory(
#         provides=DataCatalogService,
#         glue_context=glue_context,
#         glue_client = glue_client,
#         aws_region=config.provided.aws_region
#     )

#     data_catalog_loader = providers.Factory(
#         provides=DataCatalogLoader,
#         data_catalog_service = data_catalog_service,
#         cache=cache
#     )
    
#     opensearch_subcontainer = providers.Object(None)
#     if app_settings.provided.enable_opensearch_service:
#         opensearch_subcontainer = providers.Container(
#                     OpenSearchContainer,  
#                     config=config,
#                     app_settings=app_settings,
#                     s3_client=s3_client,
#                     secret_service=secret_service,
#                     glue_context=glue_context
#                 )
#     # # opensearch_service = providers.Callable(
#     #     provides=lambda subcontainer: subcontainer.opensearch_service() if subcontainer else None,
#     #     subcontainer=opensearch_subcontainer
#         # )
  
#     opensearch_secrets = providers.Callable(
#         provides=lambda secret_service: secret_service.fetch_secrets(secret_name=config.provided.opensearch_secret_names),
#         secret_service=secret_service
#     )
   
#     opensearch_client = providers.Factory(
#         provides=OpenSearchClient,
#         opensearch_host=app_settings.provided.opensearch_host,
#         opensearch_user=opensearch_secrets.provided.username,
#         opensearch_pass=opensearch_secrets.provided.password,
#     )
    
#     opensearch_service = providers.Factory(
#         provides=OpenSearchService,
#         s3_client=s3_client,
#         opensearch_config=app_settings,
#         glue_context=glue_context,
#         opensearch_client=opensearch_client
#     )
    
    
   

   

   