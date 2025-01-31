from __future__ import annotations
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from functools import cached_property

from ..core.decorators.decorators import singleton
from ..core.shared import ServicesEnabled
from .sdk_config import SdkConfig
from ..core.master import MasterConfig

if TYPE_CHECKING:
    from ..containers import ApplicationContainer
    from .sdk_opensearch import SdkOpenSearch
    from .sdk_cache import SdkCache


class SdkManagerError(Exception):
    pass


@singleton
class SdkManager:
    _master_config: Optional[MasterConfig]
    _container: Optional[ApplicationContainer]
    _opensearch: Optional[SdkOpenSearch]
    services_enabled: ServicesEnabled
    _sdk_cache: Optional[SdkCache]

    def __init__(self, config: Optional[SdkConfig] = None) -> None:
        self._master_config = None
        self._container = None
        self._opensearch = None
        self.services_enabled = ServicesEnabled()

        if isinstance(config, SdkConfig):
            self._config: SdkConfig = config
        else:
            self._config = SdkConfig()

    def initialize(self) -> None:
        from ..containers import ApplicationContainer

        self._container = ApplicationContainer()

        if self.services_enabled.USE_SPARK:
            self.container.core.dynamic_configs_spark_client.override(
                provider=self.config._spark_conf
            )

    @property
    def config(self) -> SdkConfig:
        """Returns the SDK configuration, initializing it if necessary."""
        if not self._config:
            self._config = SdkConfig()
        if not self._config:
            raise SdkManagerError("Cant initialize SdkConfig")
        return self._config

    @config.setter
    def config(self, v: Optional[SdkConfig]) -> None:
        if isinstance(v, SdkConfig):
            self._config = v
        else:
            raise SdkManagerError("Invalid configuration type")

    @property
    def container(self) -> ApplicationContainer:
        """Returns the application container, ensuring it has been initialized."""
        if not self._container:
            raise SdkManagerError("U must call initialize func firstly")
        return self._container

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
