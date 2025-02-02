from __future__ import annotations
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from functools import cached_property

from ..core.decorators.decorators import singleton
from ..core.shared import ServicesEnabled,SharedUtilsSettings
from .sdk_config import SdkConf
from ..core.master import MasterConfig
from ..core.utils.utils import SingletonMeta

if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from .sdk_opensearch import SdkOpenSearch
    from .sdk_cache import SdkCache


class SdkManagerError(Exception):
    pass


class SdkManager(metaclass=SingletonMeta):
    _master_config: Optional[MasterConfig]
    _opensearch: Optional[SdkOpenSearch]
    services_enabled: ServicesEnabled
    _sdk_cache: Optional[SdkCache]
    _config: Optional[SdkConf]

    def __init__(self, config: Optional[SdkConf] = None) -> None:
        self._master_config = None
        self._opensearch = None
        self.services_enabled = ServicesEnabled()
        self.shared_settings = SharedUtilsSettings()
        self.config = config

    def initialize(self) -> None:
        from ..containers import ApplicationContainer
        self.container = ApplicationContainer()
        self.container.reset_override()
        if self.services_enabled.USE_SPARK:
            self.container.core.dynamic_configs_spark_client.override(
                provider=self.config._spark_conf
            )
                
    @property
    def config(self) -> SdkConf:
        """Returns the SDK configuration, initializing it if necessary."""
        if not self._config:
            self.config = SdkConf()
        if not self._config:
            raise SdkManagerError("Cant initialize SdkConfig")
        return self._config

    @config.setter
    def config(self, v: Optional[SdkConf]) -> None:
        if isinstance(v, SdkConf):
            self._config = v
        # else:
        #     raise SdkManagerError("Invalid configuration type")

    @property
    def container(self) -> ApplicationContainer:
        """Returns the application container, ensuring it has been initialized."""
        if not self.shared_settings.container:
            raise SdkManagerError("U must call initialize func firstly")
        return self.shared_settings.container

    @container.setter
    def container(self, container:ApplicationContainer) -> None:
        self.shared_settings.container = container
    
    @property
    def opensearch(self) -> SdkOpenSearch:
        """Returns the OpenSearch SDK, initializing it if necessary."""
        from .sdk_opensearch import SdkOpenSearch

        if not self.config.USE_OPENSEARCH:
            raise SdkManagerError("U have to enable OpenSearch resource")
        if not self._opensearch:
            self._opensearch = SdkOpenSearch(container=self.container)
        if not self._opensearch:
            raise SdkManagerError("opensearch sdk is not initialized")
        return self._opensearch

    @cached_property
    def cache(self) -> SdkCache:
        from .sdk_cache import SdkCache

        if not self.services_enabled.USE_CACHE:
            raise SdkManagerError("U have to enable Cache resource")
        self._sdk_cache = SdkCache(container=self.container)
        return self._sdk_cache
