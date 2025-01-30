from typing import Dict,Literal, List,Optional, Tuple, TYPE_CHECKING
from pyspark.sql import DataFrame
from ...core.services.base_service import BaseService
from ..interfaces.i_aurora_pg_service import IAuroraPgService

if TYPE_CHECKING:
    from ..interfaces.i_aurora_pg_worker import IAuroraPgWorker



class AuroraPgService(IAuroraPgService,BaseService):
    __slots__: Tuple = ("glue_context","client","config")
    
    def __init__(self,
                config: Dict,
                # aurora_pg_client: "IClient",
                connection_name: Optional[str]=None,
                python_worker: Optional["IAuroraPgWorker"]=None,
                pyspark_worker: Optional["IAuroraPgWorker"]=None,
                glue_worker: Optional["IAuroraPgWorker"]=None
                ) -> None:
        """
        Args:
            glue_context (GlueContext): _description_
            config (Dict): _description_
            aurora_pg_client (IClient): _description_
        """
        self.config: Dict = config
        # self.client: "IClient" = aurora_pg_client
        self.connection_name: str = connection_name or self.config.get("connection_name","")  
        self.workers: Dict[str, Optional["IAuroraPgWorker"]] = {
            "python": python_worker,
            "pyspark": pyspark_worker,
            "glue": glue_worker,
        }
    
    def _get_worker(self, worker_type: str) -> "IAuroraPgWorker":
        worker: Optional["IAuroraPgWorker"] = self.workers.get(worker_type)
        if not worker:
            raise ValueError(f"Worker type '{worker_type}' is not initialized or unsupported")
        return worker
         
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        return None
    #     """
    #     Executes a query against the Aurora PostgreSQL database.

    #     Args:
    #         query (str): SQL query to execute.
    #         params (Optional[tuple]): Parameters for the SQL query.

    #     Returns:
    #         Optional[List[tuple]]: Query result if applicable, else None.
    #     """
    #     try:
    #         with self.client.create_client() as conn:
    #             with conn.cursor() as cursor:
    #                 self.log_debug(f"Executing query: {query}")
    #                 cursor.execute(query, params)
    #                 if cursor.description:  # If query returns data
    #                     result = cursor.fetchall()
    #                     self.log_debug(f"Query result: {result}")
    #                     return result
    #                 conn.commit()
    #                 self.logger.info("Query executed successfully.")
    #     except Exception as e:
    #         self.log_error(f"Error executing query: {e}")
    #     return None
    
    def fetch_data(self, table_name: str, 
                   push_down_predicate: Optional[str] = None,
                   worker_type: Literal['python', 'pyspark', 'glue'] ='glue'
                   ) -> 'DataFrame':
        """
        Fetches data from the Aurora PostgreSQL database as a Spark DataFrame.
        Using Glue context
        Args:
            table_name (str): Name of the table to fetch data from.
            push_down_predicate (Optional[str]): SQL WHERE clause for filtering data.
            worker_type - Literal['python', 'pyspark', 'glue'] 
        Returns:
            Optional[DataFrame]: The fetched data as a Spark DataFrame, or None if an error occurs.
        """
        worker: "IAuroraPgWorker" = self._get_worker(worker_type=worker_type)
        return worker.fetch_data(table_name=table_name, push_down_predicate=push_down_predicate)
        
        
           
    def load_data(self, 
                  spark_df: 'DataFrame', 
                  table_name: str,
                  db_name: Optional[str]= None,
                  schema: Optional[str] = None,
                  worker_type: Literal['python', 'pyspark', 'glue'] ='glue',
                  mode:Literal['overwrite','error','ignore','append'] = 'overwrite' 
                  ) -> bool:
        """
        Loads data into the Aurora PostgreSQL database from a Spark DataFrame.
        Using glue context
        Args:
            spark_df (DataFrame): Data to load into the database.
            table_name (str): Name of the target table.

        Returns:
            bool: True if data is successfully loaded, else False.
        """
        worker: "IAuroraPgWorker" = self._get_worker(worker_type=worker_type)
        return worker.load_data(spark_df=spark_df, 
                                schema=schema,
                                db_name=db_name,
                                table_name=table_name,
                                mode=mode
                                )
        