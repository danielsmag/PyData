from __future__ import annotations
from ..core.shared import ServicesEnabled
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from ..core.master import MasterConfig
from ..core.decorators.decorators import singleton


class SdkConfigError(Exception):
    pass


class SdkConfig:
    def __init__(self, services_enabled: Optional["ServicesEnabled"] = None) -> None:
        self.services_enabled: ServicesEnabled = services_enabled or ServicesEnabled()
        self._conf: Optional[Dict] = None
        self._spark_conf: Dict = {}

    @validate_call
    def set_services_to_use(
        self,
        USE_OPENSEARCH: bool = False,
        USE_DATA_CATALOG: bool = False,
        USE_AURORA_PG: bool = False,
        USE_CACHE: bool = True,
        USE_DATA_BUILDERS: bool = True,
        USE_GLUE: bool = True,
        USE_SPARK: bool = True,
        USE_EMR: bool = False,
    ) -> None:

        self.services_enabled.update_values(
            USE_OPENSEARCH=USE_OPENSEARCH,
            USE_DATA_CATALOG=USE_DATA_CATALOG,
            USE_AURORA_PG=USE_AURORA_PG,
            USE_CACHE=USE_CACHE,
            USE_DATA_BUILDERS=USE_DATA_BUILDERS,
            USE_GLUE=USE_GLUE,
            USE_SPARK=USE_SPARK,
            USE_EMR=USE_EMR,
        )

    @validate_call
    def set_config_application(self, config_data: Dict = {}) -> None:
        """Set main configuration for the SDK."""
        validate_config = MasterConfig(**config_data)
        self._master_config: MasterConfig = validate_config
        self._conf = validate_config.model_dump()

    @validate_call
    def set_spark_conf(self, config_data: Dict = {}) -> None:
        """Set all Spark configuration before loading the Spark client."""
        self._spark_conf = config_data

    @property
    def USE_OPENSEARCH(self) -> bool:
        return self.services_enabled.USE_OPENSEARCH

    @property
    def USE_AURORA_PG(self) -> bool:
        return self.services_enabled.USE_AURORA_PG

    @property
    def USE_CACHE(self) -> bool:
        return self.services_enabled.USE_CACHE

    @property
    def USE_DATA_CATALOG(self) -> bool:
        return self.services_enabled.USE_DATA_CATALOG

    @property
    def USE_DATA_BUILDERS(self) -> bool:
        return self.services_enabled.USE_DATA_BUILDERS

    @property
    def USE_GLUE(self) -> bool:
        return self.services_enabled.USE_GLUE

    @property
    def USE_SPARK(self) -> bool:
        return self.services_enabled.USE_SPARK

    @property
    def USE_EMR(self) -> bool:
        return self.services_enabled.USE_EMR
