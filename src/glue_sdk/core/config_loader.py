import yaml
from typing import Any, Dict, List
from .utils.utils import get_yaml_from_s3
from .logging.logger import logger


class ConfigLoader:
    _config: Dict[str, Any] = {}

    @classmethod
    def load_config(cls, filepath: str) -> None:
        with open(file=filepath, mode='r') as file:
            cls._config = yaml.safe_load(file)

    @classmethod
    def set_config(cls, file: Dict) -> None:
        cls._config = file
    
    @classmethod
    def load_config_yaml_from_s3(cls,prefix: str) -> None:
        yaml_file: Dict= get_yaml_from_s3(config_file_path=prefix)
        if not yaml_file:
            raise Exception("load_config_yaml_from_s3 no yaml file")
        for k,v in yaml_file.items():
            logger.debug(f"key:{k} value: {v}")
        cls._config = yaml_file
        
    @classmethod
    def get_value(cls, key: str, default: Any = None) -> Any:
        keys: List[str] = key.split(sep='.')
        value: Any = cls._config    
        for k in keys:
            if not isinstance(value, dict):
                return default
            if k not in value:
                return default
            value = value[k]
            logger.debug(f"for key {k} get_value return {value}")
        return value if value is not None else default

    @classmethod
    def get_config(cls) -> Dict:
        return cls._config
    