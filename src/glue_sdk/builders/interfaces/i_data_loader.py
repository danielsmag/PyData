from abc import ABC, abstractmethod
from typing import Union, TYPE_CHECKING

from typing_extensions import Self
from ..interfaces.i_data_builder_base import IDataBuilder
    
if TYPE_CHECKING:
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.dataframe import DataFrame
    
class IDataLoader(IDataBuilder,ABC):

    @abstractmethod
    def load(self,
            db_name: str, 
            table_name: str,
            keep_dynamicframe: bool = False
            ) -> Self:
        pass
    
    @abstractmethod
    def get(self) -> Union['DynamicFrame', 'DataFrame']:
        pass
 
    @abstractmethod
    def to_persist(self) -> Self:
        pass
       
    @abstractmethod
    def get_df(self) -> 'DataFrame':
        pass
    