from ..interfaces.i_aurora_pg_worker import IAuroraPgWorker
from typing import Dict, Optional, List,TYPE_CHECKING, Literal
from pyspark.sql import DataFrame
from ...core.services.base_service import BaseService

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

class PySparkAuroraPgWorker(BaseService,IAuroraPgWorker):
    def __init__(
        self,
        spark: "SparkSession",
        config: Dict,
        jdbc_url: str,
        db_user: str,
        db_password: str
    ) -> None:
        super().__init__() 
        self.spark: "SparkSession" = spark
        self.config: Dict = config
        self.jdbc_url: str = jdbc_url
        self._db_user: str = db_user
        self._db_password: str = db_password
        
    
    @property
    def db_user(self) -> str:
        return self._db_user

    @property
    def db_password(self) -> str:
        return self._db_password
    
    def fetch_data(self, table_name: str, push_down_predicate: Optional[str] = None) -> 'DataFrame':
        try:
            self.log_debug(f"Fetching data from table '{table_name}' with predicate: {push_down_predicate}")
            properties: Dict[str, str] = {
                "user": self._db_user,
                "password": self._db_password,
                "driver": "org.postgresql.Driver"
            }
            query = f"(SELECT * FROM {table_name}"
            if push_down_predicate:
                query += f" WHERE {push_down_predicate}"
            query += ") AS subquery"
            
            spark_df: 'DataFrame' = self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=properties)
            self.log_debug(f"Successfully fetched data from table '{table_name}'")
            return spark_df
        except Exception as e:
            self.log_error(f"Error fetching data from table '{table_name}': {e}")
            raise
    
    def load_data(self, 
                  spark_df: 'DataFrame', 
                  table_name: str,
                  db_name: Optional[str] = None,
                  schema:Optional[str]= None,
                  mode:Literal['overwrite','error','ignore','append'] = 'overwrite'
                  ) -> bool:
        try:
            self.log_debug(f"Loading data into table '{table_name}'")
            full_table_name: str = f"{schema}.{table_name}" 
            
            properties: Dict[str, str] = {
                "user": self._db_user,
                "password": self._db_password,
                "driver": "org.postgresql.Driver"
            }
            spark_df.write.jdbc(url=self.jdbc_url, table=full_table_name, mode=mode, properties=properties)
            self.log_info(f"Successfully loaded data into table '{table_name}'")
            return True
        except Exception as e:
            self.log_error(f"Error loading data into table '{table_name}': {e}")
            return False
    
