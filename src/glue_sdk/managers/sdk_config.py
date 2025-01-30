from ..core.decorators.decorators import singelton
from ..core.shared import AwsServicesToUse
from pydantic import validate_call
from typing import Any, Dict, Optional, TYPE_CHECKING
from ..core.master import MasterConfig 
 


class SdkConfigError(Exception):
    pass

@singelton
class SdkConfig:
    def __init__(self,
                 aws_services_to_use:Optional['AwsServicesToUse'] = None
                 ) -> None:
        self.aws_services_to_use: 'AwsServicesToUse' = AwsServicesToUse()
        self._conf: Optional[Dict]= None
        self._spark_conf: Dict = {}
        
        
    @validate_call
    def set_services_to_use(self,
        USE_OPENSEARCH: bool = False,
        USE_DATA_CATALOG: bool = False,
        USE_AURORA_PG: bool = False,
        USE_CACHE: bool = True
    ) -> None:
        self.USE_OPENSEARCH=USE_OPENSEARCH
        self.USE_DATA_CATALOG=USE_DATA_CATALOG
        self.USE_AURORA_PG=USE_AURORA_PG
        self.USE_CACHE=USE_CACHE
    
    @validate_call
    def set_config_application(self,config_data: Dict = {}) -> None:
        """Set main config for sdk """
        validate_config = MasterConfig(**config_data)
        self._master_config = validate_config
        validated_dict: Dict = validate_config.model_dump()
        self._conf = validated_dict
        # self.container.config.from_dict(validated_dict)
    
    @validate_call
    def set_spark_conf(self,config_data: Dict = {}) -> None:
        """
        set all spark config before load spark client
        """
        self._spark_conf = config_data

    @property
    def USE_OPENSEARCH(self) -> bool:
        return self.aws_services_to_use.USE_OPENSEARCH
    
    @USE_OPENSEARCH.setter
    @validate_call
    def USE_OPENSEARCH(self,v: bool) -> None:
        self.aws_services_to_use.USE_OPENSEARCH = v
       
    @property
    def USE_AURORA_PG(self) -> bool:
        return self.aws_services_to_use.USE_AURORA_PG
    
    @USE_AURORA_PG.setter
    @validate_call
    def USE_AURORA_PG(self,v: bool) -> None:
        self.aws_services_to_use.USE_AURORA_PG  = v
        
    @property
    def USE_CACHE(self) -> bool:
        return self.aws_services_to_use.USE_CACHE
    
    @USE_CACHE.setter
    @validate_call
    def USE_CACHE(self,v: bool) -> None:
        self.aws_services_to_use.USE_CACHE  = v    
    
    
    @property
    def USE_DATA_CATALOG(self) -> bool:
        return self.aws_services_to_use.USE_DATA_CATALOG
    
    @USE_DATA_CATALOG.setter
    @validate_call
    def USE_DATA_CATALOG(self,v: bool) -> None:
        self.aws_services_to_use.USE_DATA_CATALOG = v
    
    