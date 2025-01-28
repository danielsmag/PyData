from dependency_injector import containers, providers
from ..cache.services.shared_data_service import SharedDataService


class Cache(containers.DeclarativeContainer):
    config  = providers.Configuration()
    cache = providers.Singleton(
        provides=SharedDataService,
        cache_timeout=None
    )
    