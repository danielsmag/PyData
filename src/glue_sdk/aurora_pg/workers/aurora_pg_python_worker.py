
from typing import Any, Dict, Optional, List, Tuple,TYPE_CHECKING, Literal
from ...core.services.base_service import BaseService 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType  
from ..interfaces.i_aurora_pg_worker import IAuroraPgWorker

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from ..interfaces.i_aurora_pg_client import IAuroraPgClient

class Psycopg2AuroraPgWorkerError(Exception):
    pass
class Psycopg2AuroraPgWorker(IAuroraPgWorker, BaseService):
    __slots__: Tuple = ("config", "client", "connection_name", "spark")

    def __init__(
        self,
        config: Dict,
        aurora_pg_client: "IAuroraPgClient"
    ) -> None:
        """
        Initializes the Psycopg2AuroraPgWorker.

        Args:
            config (Dict): Configuration dictionary containing database connection parameters.
            aurora_pg_client (IClient): Client interface for managing database connections.
            spark (SparkSession): Spark session for creating Spark DataFrames.
            connection_name (Optional[str], optional): Optional connection name. Defaults to None.
        """
        self.config: Dict = config
        self.client: "IAuroraPgClient" = aurora_pg_client
        
    def fetch_data(self, table_name: str, push_down_predicate: Optional[str] = None) -> 'DataFrame':
        """
        Fetches data from the Aurora PostgreSQL database as a Spark DataFrame using psycopg2.

        Args:
            table_name (str): Name of the table to fetch data from.
            push_down_predicate (Optional[str]): SQL WHERE clause for filtering data.

        Returns:
            DataFrame: The fetched data as a Spark DataFrame.

        Raises:
            Exception: If an error occurs during data fetching.
        """
        try:
            self.log_debug(f"Fetching data from table '{table_name}' with predicate: {push_down_predicate}")

            # Construct the SQL query
            query = f"SELECT * FROM {table_name}"
            if push_down_predicate:
                query += f" WHERE {push_down_predicate}"
            
            with self.client.create_client() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    if not cursor.description:
                        raise Psycopg2AuroraPgWorkerError("No  cursor.description exist")
                    columns: List[Any] = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    self.log_debug(f"Fetched {len(rows)} rows from table '{table_name}'")

            # Convert fetched data to Spark DataFrame
            if rows:
                # Infer schema based on the first row's data types
                schema = self._infer_schema(columns, rows[0])
                df = self.spark.createDataFrame(rows, schema=schema)
                self.log_debug(f"Successfully converted fetched data to Spark DataFrame for table '{table_name}'")
                return df
            else:
                self.log_info(f"No data found in table '{table_name}'")
                return self.spark.createDataFrame([], StructType([]))

        except Exception as e:
            self.log_error(f"Error fetching data from table '{table_name}': {e}")
            raise

    def load_data(self,  
                spark_df: 'DataFrame', 
                table_name: str,
                db_name: Optional[str] = None,
                schema:Optional[str]= None,
                mode:Literal['overwrite','error','ignore','append'] = 'overwrite') -> bool:
        """
        Loads data into the Aurora PostgreSQL database from a Spark DataFrame using psycopg2.

        Args:
            spark_df (DataFrame): Data to load into the database.
            table_name (str): Name of the target table.

        Returns:
            bool: True if data is successfully loaded, else False.
        """
        from psycopg2 import sql
        try:
           
            self.log_debug(f"Loading data into table '{table_name}'")

            # Convert Spark DataFrame to list of tuples
            data = [tuple(row) for row in spark_df.collect()]
            columns = spark_df.columns

            if not data:
                self.log_info("No data to load.")
                return True  # Nothing to load

            # Construct the SQL INSERT statement
            placeholders = ", ".join(["%s"] * len(columns))
            columns_formatted = ", ".join(columns)
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(columns_formatted),
                sql.SQL(placeholders)
            )

            with self.client.create_client() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query.as_string(conn), data)
                conn.commit()
                self.log_info(f"Successfully loaded {len(data)} records into table '{table_name}'")
                return True

        except Exception as e:
            self.log_error(f"Error loading data into table '{table_name}': {e}")
            return False
    
    def _infer_schema(self, columns: List[str], first_row: Tuple) -> StructType:
        """
        Infers the Spark DataFrame schema based on the first row's data types.

        Args:
            columns (List[str]): List of column names.
            first_row (Tuple): The first row of data.

        Returns:
            StructType: Inferred schema for the Spark DataFrame.
        """
        fields = []
        for col, val in zip(columns, first_row):
            if isinstance(val, int):
                fields.append(StructField(col, IntegerType(), True))
            elif isinstance(val, float):
                fields.append(StructField(col, FloatType(), True))
            elif isinstance(val, str):
                fields.append(StructField(col, StringType(), True))
            else:
                fields.append(StructField(col, StringType(), True))  # Default to StringType
        return StructType(fields)