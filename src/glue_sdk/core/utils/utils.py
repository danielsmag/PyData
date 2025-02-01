from __future__ import annotations
import datetime
import json
import math
import os
from typing import TYPE_CHECKING, Any, Dict, List
from threading import RLock
from botocore.client import BaseClient

from ..logging.logger import logger

if TYPE_CHECKING:
    from pyspark.sql.types import StructType
    from pyspark.sql import DataFrame

__all__: List[str] = [
    "load_spark_schema",
    "get_optimal_partitions",
    "get_dataframe_size_in_mb",
    "create_unique_name",
    "to_camel_case",
    "set_env_from_json_s3",
    "get_yaml_from_s3",
    "SingletonMeta",
]


class SingletonMeta(type):
    """
    A thread-safe implementation of a Singleton metaclass.
    """

    _instances = {}
    _lock = RLock()

    def __call__(cls, *args, **kwargs):

        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]


def set_env_from_json_s3(config_file_path: str) -> None:
    from ...general.clients.s3_client import S3Client
    from ...general.services.s3_service import S3Service

    Client = S3Client()
    s3_client: BaseClient = Client.create_client()
    s3_service = S3Service(s3_client=s3_client)
    logger.info(f"Fetching JSON file from {config_file_path}")
    params: Dict = s3_service.load_json(url=config_file_path)

    for key, value in params.items():
        os.environ[key] = str(value)
        logger.debug(f"Set env variable: {key}={value}")
    logger.info("env variables successfully set from S3 JSON file.")


def get_yaml_from_s3(config_file_path: str) -> Dict:
    from ...general.clients.s3_client import S3Client
    from ...general.services.s3_service import S3Service

    Client = S3Client()
    s3_client: BaseClient = Client.create_client()
    s3_service = S3Service(s3_client=s3_client)
    logger.info(f"Fetching yaml file from {config_file_path}")
    yaml_file: Dict = s3_service.load_yaml_from_s3(url=config_file_path)
    return yaml_file


def load_spark_schema(s3_client, s3_bucket: str, schema_key: str) -> StructType:
    schema_obj: dict = s3_client.get_object(Bucket=s3_bucket, Key=schema_key)
    schema_json: str = schema_obj["Body"].read().decode("utf-8")
    res_schema: StructType = StructType.fromJson(json=json.loads(s=schema_json))
    return res_schema


def get_dataframe_size_in_mb_by_row(df) -> int | float:
    total_bytes: Any = (
        df.rdd.map(lambda row: len(str(object=row))).glom().map(len).sum()
    )
    size_in_mb: Any = total_bytes / (1024 * 1024)
    return size_in_mb


def get_dataframe_size_in_mb(df: DataFrame) -> float:
    sample_fraction = 0.01
    sample_df: DataFrame = df.sample(withReplacement=False, fraction=sample_fraction)
    sample_size_in_mb: int | float = get_dataframe_size_in_mb_by_row(df=sample_df)
    estimated_size_in_mb: float = sample_size_in_mb / sample_fraction
    return estimated_size_in_mb


def get_optimal_partitions(data_size_in_mb: int | float, target_partition_size_mb=128):
    try:
        num_cores: int = os.cpu_count() or 4
    except Exception as e:
        print(f"Cant extract num of cors: {e}")
        num_cores = 4

    optimal_partitions: int = max(
        math.ceil(data_size_in_mb / target_partition_size_mb), 2 * num_cores
    )
    print(f"Auto calculate partiton Optimum is : {optimal_partitions}")
    return optimal_partitions


def create_unique_name(base_name: str) -> str:
    timestamp: str = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S%f")  # type: ignore
    return f"{base_name}_{timestamp}"


def to_camel_case(columns: List[str]) -> List[str]:
    res_list = []
    for col in columns:
        parts: List[str] = col.split("_")
        parts = [part.lower() for part in parts]
        new_word: str = "_".join(part.capitalize() for part in parts)
        res_list.append(new_word)
    return res_list
