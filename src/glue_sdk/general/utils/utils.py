import os
import json
from typing import List,Any, Dict
import datetime
from ..clients.s3_client import S3Client
from botocore.client import BaseClient
from ...core.logging.logger import logger
from ..services.s3_service import S3Service

__all__: List[str] = [
                    "create_unique_name",
                    "to_camel_case",
                    "set_env_from_json_s3",
                    "get_yaml_from_s3"
                    ]

def set_env_from_json_s3(config_file_path: str ) -> None:
    Client = S3Client()
    s3_client: BaseClient = Client.create_client()
    s3_service = S3Service(s3_client=s3_client)
    logger.info(f"Fetching JSON file from {config_file_path}")
    params: Dict = s3_service.load_json(url=config_file_path)
    
    for key, value in params.items():
        os.environ[key] = str(value)
        logger.debug(f"Set env variable: {key}={value}")
    logger.info("env variables successfully set from S3 JSON file.")

def get_yaml_from_s3(config_file_path: str )->Dict:
    Client = S3Client()
    s3_client: BaseClient = Client.create_client()
    s3_service = S3Service(s3_client=s3_client)
    logger.info(f"Fetching yaml file from {config_file_path}")
    yaml_file: Dict = s3_service.load_yaml_from_s3(url=config_file_path)
    return yaml_file

def create_unique_name(base_name: str) -> str:
    timestamp: str = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S%f") # type: ignore
    return f"{base_name}_{timestamp}"
    
def to_camel_case(columns: List[str])->List[str]:
    res_list = []
    for col in columns:
        parts: List[str] = col.split("_")
        parts = [part.lower() for part in parts]
        new_word: str = "_".join(part.capitalize() for part in parts)
        res_list.append(new_word)
    return res_list