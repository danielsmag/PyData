from typing import (
    List,
    Dict,
    TYPE_CHECKING
)

from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from glue_sdk.interfaces import ISparkBaseService
from glue_sdk.services.base_service import BaseService

class SparkBaseService(ISparkBaseService, BaseService):
    def __init__(self) -> None:
        pass
    
    def flatten_df(self,
                   df: DataFrame,
                   sep: str = ".",
                   lower_case: bool = True
                   ) -> DataFrame:
        """
        Flatten a nested DataFrame schema and include levels in headers.

        :param df: PySpark DataFrame to flatten.
        :param sep: Separator for nested levels in column names.
        :return: Flattened DataFrame.
        """
        def flatten_schema(schema, prefix=None):
            """
            Recursively flatten the schema.
            :param schema: DataFrame schema.
            :param prefix: Prefix for nested columns.
            :return: List of tuples (flattened_name, column_expression).
            """
            fields:List = []
            for field in schema.fields:
                name = f"{prefix}{sep}{field.name}" if prefix else field.name
                if hasattr(field.dataType, "fields"):  # Check if it's a struct
                    # Recursively process nested fields
                    fields += flatten_schema(field.dataType, prefix=name)
                else:
                    if lower_case:
                        fields.append((name.lower(), F.col(name).alias(name.lower())))
                    else:
                        fields.append((name, F.col(name).alias(name)))
            return fields
        
        flattened_fields = flatten_schema(df.schema)

        return df.select([expr for _, expr in flattened_fields])