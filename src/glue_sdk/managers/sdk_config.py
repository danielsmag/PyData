from __future__ import annotations
from ..core.shared import AwsServicesToUse
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from ..core.master import MasterConfig 
from ..core.decorators.decorators import singleton

class SdkConfigError(Exception):
    pass

class SdkConfig:
    def __init__(self,
                aws_services_to_use: Optional['AwsServicesToUse'] = None
                ) -> None:
        self.aws_services_to_use: 'AwsServicesToUse' = aws_services_to_use or AwsServicesToUse()
        self._conf: Optional[Dict] = None
        self._spark_conf: Dict = {}
        
    @validate_call
    def set_services_to_use(self,
        USE_OPENSEARCH: bool = False,
        USE_DATA_CATALOG: bool = False,
        USE_AURORA_PG: bool = False,
        USE_CACHE: bool = True,
        USE_DATA_BUILDERS: bool = True,
        USE_GLUE: bool = True,
        USE_SPARK: bool = True,
        USE_EMR: bool = False,
    ) -> None:
        """Set AWS services to use in the SDK."""
        self.aws_services_to_use.USE_OPENSEARCH = USE_OPENSEARCH
        self.aws_services_to_use.USE_DATA_CATALOG = USE_DATA_CATALOG
        self.aws_services_to_use.USE_AURORA_PG = USE_AURORA_PG
        self.aws_services_to_use.USE_CACHE = USE_CACHE
        self.aws_services_to_use.USE_DATA_BUILDERS = USE_DATA_BUILDERS
        self.aws_services_to_use.USE_GLUE = USE_GLUE
        self.aws_services_to_use.USE_SPARK = USE_SPARK
        self.aws_services_to_use.USE_EMR = USE_EMR

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
        return self.aws_services_to_use.USE_OPENSEARCH

    @property
    def USE_AURORA_PG(self) -> bool:
        return self.aws_services_to_use.USE_AURORA_PG

    @property
    def USE_CACHE(self) -> bool:
        return self.aws_services_to_use.USE_CACHE

    @property
    def USE_DATA_CATALOG(self) -> bool:
        return self.aws_services_to_use.USE_DATA_CATALOG

    @property
    def USE_DATA_BUILDERS(self) -> bool:
        return self.aws_services_to_use.USE_DATA_BUILDERS

    @property
    def USE_GLUE(self) -> bool:
        return self.aws_services_to_use.USE_GLUE

    @property
    def USE_SPARK(self) -> bool:
        return self.aws_services_to_use.USE_SPARK

    @property
    def USE_EMR(self) -> bool:
        return self.aws_services_to_use.USE_EMR

 
