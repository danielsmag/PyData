import json 
import os

import math
from typing import List,Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType
    
__all__: list[str]=["load_spark_schema"]

def load_spark_schema(s3_client,s3_bucket: str, schema_key: str ) -> 'StructType':
        from pyspark.sql.types import StructType
        schema_obj: dict = s3_client.get_object(Bucket=s3_bucket, Key=schema_key)
        schema_json: str = schema_obj['Body'].read().decode('utf-8')
        res_schema: StructType = StructType.fromJson(json=json.loads(s=schema_json))
        return res_schema

def get_dataframe_size_in_mb_by_row(df)-> int | float:
    total_bytes: Any = df.rdd.map(lambda row: len(str(object=row))).glom().map(len).sum()
    size_in_mb: Any = total_bytes / (1024 * 1024)
    return size_in_mb

def get_dataframe_size_in_mb(df: 'DataFrame') -> float:
    sample_fraction = 0.01  
    sample_df: 'DataFrame' = df.sample(withReplacement=False, fraction=sample_fraction)
    sample_size_in_mb: int | float = get_dataframe_size_in_mb_by_row(df=sample_df)
    estimated_size_in_mb: float = sample_size_in_mb / sample_fraction
    return estimated_size_in_mb

def get_optimal_partitions(data_size_in_mb: int | float, target_partition_size_mb=128):
    try:
        num_cores: int  = os.cpu_count() or 4
    except Exception as e :
        print(f"Cant extract num of cors: {e}")
        num_cores = 4
        
    optimal_partitions: int = max(math.ceil(data_size_in_mb / target_partition_size_mb), 2 * num_cores)
    print(f"Auto calculate partiton Optimum is : {optimal_partitions}")
    return optimal_partitions