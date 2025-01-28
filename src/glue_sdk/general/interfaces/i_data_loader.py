from abc import ABC, abstractmethod
from typing import Union
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.dataframe import DataFrame

from typing_extensions import Self
from glue_sdk.interfaces.i_data_builder_base import IDataBuilder
    
class IDataLoader(IDataBuilder,ABC):

    @abstractmethod
    def load(self,
            db_name: str, 
            table_name: str,
            keep_dynamicframe: bool = False
            ) -> Self:
        pass
    
    @abstractmethod
    def get(self) -> Union[DynamicFrame, DataFrame]:
        pass
 
    @abstractmethod
    def to_persist(self) -> Self:
        pass
       
    @abstractmethod
    def get_df(self) -> DataFrame:
        pass
    