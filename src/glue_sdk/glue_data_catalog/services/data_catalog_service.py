from botocore.exceptions import ClientError
from typing import Optional, Any, List,Literal , Dict,TYPE_CHECKING, Union
from pyspark.sql import DataFrame
from ...core.services.base_service import BaseService
from ..models.data_catalog_model import DataCatalogDatabase,DataCatalogTable
from ..interfaces.i_data_catalog_service import IDataCatalogService

if TYPE_CHECKING:
    from botocore.client import BaseClient
    from awsglue.context import GlueContext
    from pyspark.sql.readwriter import DataFrameWriter
    from awsglue import DynamicFrame
    
__all__:List[str] = ["DataCatalogService"]

class DataCatalogServiceError(Exception):
    pass

class DataCatalogService(BaseService,IDataCatalogService):
    """
        A service for interacting with AWS Glue.
        This class fetches database/table metadata and loads/writes data in Glue.
    """
    def __init__(
        self,
        glue_context: "GlueContext",
        glue_client: "BaseClient",
        aws_region: Optional[str] = 'eu-west1-1',
    ) -> None:
        """
        :param glueContext: An AWS GlueContext instance
        :param glue_client: Optional, otherwise will be created with boto3
        :param aws_region: Region for boto3 client
        """
        super().__init__()
        self.glue_client: "BaseClient" = glue_client 
        self.glue_context: "GlueContext" = glue_context

    def fetch_table(self,
                    table_name: str,
                    db: Union[str, "DataCatalogDatabase"],
                    )-> DataCatalogTable:
        """get table with metadata"""
        try:
            if not isinstance(db, DataCatalogDatabase):
                db = self.fetch_db(db_name=db)
            
            response: Dict = self.glue_client.get_table(DatabaseName=db.name, Name=table_name)
            table: Dict = response["Table"]
            table_metadata: Dict = {
                "name": table_name,
                "database": db,
                "description": table.get("Description"),
                "table_type": table.get("TableType"),
                "parameters": table.get("Parameters", {}),
                "create_time": table.get("CreateTime"),
                "update_time": table.get("UpdateTime"),
                "owner": table.get("Owner"),
                "last_access_time": table.get("LastAccessTime"),
                "retention": table.get("Retention"),
                "storage_descriptor": table.get("StorageDescriptor"),
                "view_original_text": table.get("ViewOriginalText"),
                "view_expanded_text": table.get("ViewExpandedText"),
                "catalog_id": table.get("CatalogId"),
                "version_id": table.get("VersionId"),
            }
            
            res_table = DataCatalogTable(**table_metadata)
            return res_table
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "EntityNotFoundException":
                raise ValueError(
                    f"Table {table_name} does not exist in db {db}"
                )
            else:
                raise Exception(f"Error connecting to Glue Table {table_name}: {e}")
        
    def fetch_db(self,
                db_name: str           
                ) -> "DataCatalogDatabase":
        """get db meta data"""
        try:
            response: Dict = self.glue_client.get_database(Name=db_name)
            database: Dict = response["Database"]
            db_metadata: Dict = {
                "name": db_name,
                "description": database.get("Description"),
                "location_uri": database.get("LocationUri"),
                "parameters": database.get("Parameters", {}),
                "create_time": database.get("CreateTime"),
                "update_time": database.get("UpdateTime"),
                "catalog_id": database.get("CatalogId"),
                "owner": database.get("Owner"),
                "version_id": database.get("VersionId"),
            }
            db = DataCatalogDatabase(**db_metadata)
            return db
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "EntityNotFoundException":
                raise ValueError(f"DB {db_name} does not exist")
            else:
                raise Exception(f"Error connecting to DB {db_name}: {e}")

    
        
    def load_dynamic_frame(self,database_name: str, table_name: str)-> "DynamicFrame":
        """
            Create a DynamicFrame from the Glue Data Catalog.

            :param database_name: The Glue database name
            :param table_name: The Glue table name
            :return: A DynamicFrame
            :raises Exception: if loading fails
        """
        try:
            dyf: "DynamicFrame" = self.glue_context.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name
            )
            if dyf is None:
                raise ValueError(f"fail to extract data from {database_name}.{table_name}. DynamicFrame is None")
            return dyf
        except Exception as e:
            self.log_error(message=f"error loading {table_name} {e}")
            raise
    
    def write_iceberg_s3(
        self,
        df: DataFrame,
        db_name: str,
        table_name: str,
        partition_by: Optional[Union[str, List[str]]] = None,
        mode: Literal['overwrite','ignore','error','errorifexists','append'] = "append",
        overwrite_mode: Literal['static','dynamic'] = "dynamic",
        merge_schema: Literal['true','false'] = "true",
        snapshot_id:Optional[str] = None
        ) -> None:
        """
            Writes a Spark DataFrame to an Apache Iceberg table in AWS S3 using the Glue Catalog.

            :param df: The Spark DataFrame to write.
            :param db_name: The Glue database name.
            :param table_name: The Glue table name.
            :param partition_by: Column name(s) to partition the data by. Can be a single string or a list of strings.
            :param mode: The write mode. Options: 'overwrite', 'ignore', 'error', 'errorifexists', 'append'.
        """
        try:
            iceberg_table: str = f"glue_catalog.{db_name}.{table_name}"
            self.log_info(message=f"Preparing to write DataFrame to Iceberg table: {iceberg_table} with mode {mode}")

            writer: DataFrameWriter = df.write.format("iceberg").mode(mode)
            writer = (writer.option(key="overwrite-mode", value=overwrite_mode)
                            .options(key="merge-schema",value=merge_schema))
                     
            self.log_info(message=f"Set overwrite-mode to {overwrite_mode} for selective partition overwriting")

            partition_columns: List[str] = []
            if partition_by:
                if isinstance(partition_by, str):
                    partition_columns.append(partition_by)
                elif isinstance(partition_by, list):
                    partition_columns.extend(partition_by)
                else:
                    error_msg: str = f"partition_by must be a string or list of strings, got {type(partition_by)}."
                    self.log_error(message=error_msg)
                    raise DataCatalogServiceError(error_msg)

                writer = writer.partitionBy(*partition_columns)
                self.log_info(message=f"Set up partitioning on columns: {', '.join(partition_columns)}")
                
            writer.save(path=iceberg_table)
            self.log_info(message=f"Data successfully written to Iceberg table: {iceberg_table}.")

        except Exception as e:
            error_msg = f"Failed to write DataFrame to Iceberg table {db_name}.{table_name}: {e}"
            self.log_error(message=error_msg)
            raise DataCatalogServiceError(error_msg) from e
        
    def write_to_catalog(self,
            data: "DynamicFrame | DataFrame| None",
            db_name: str,
            table_name: str,
            partition_keys: Optional[list] = None,
            mode: str = "append"
            ) -> None:
        
        from awsglue import DynamicFrame
        self.log_debug(f"write_to_dynamic get data type {type(data)} db:{db_name} table:{table_name}")
    
        if data is None:
            error_msg = "No data to write "
            self.log_error(message=error_msg)
            raise DataCatalogServiceError(error_msg)
        
        if not db_name and table_name:
            error_msg = f"No database: {db_name} or  table_name:{table_name}  "
            self.log_error(message=error_msg)
            raise DataCatalogServiceError(error_msg)
        
        try:
            if isinstance(data, DataFrame):
                dynamic_frame: 'DynamicFrame' = DynamicFrame.fromDF(data, self.glue_context, "dynamic_frame")
                self.log_info(message="Converted DataFrame to DynamicFrame.")
            elif isinstance(data, DynamicFrame):
                dynamic_frame = data
            else:
                raise TypeError(f"Unmatch data type: {type(self.data)}")

            if mode == "overwrite":
                error_msg = "Pls use Write to s3 fro overwrite data  "
                
            self.glue_context.write_dynamic_frame.from_catalog(
                frame=dynamic_frame,
                database=db_name,
                table_name=table_name,
                transformation_ctx="write_to_catalog",
                additional_options={
                    "partitionKeys": partition_keys
                } if partition_keys else {}
            )

            self.log_info(message=f"Data successfully uploaded Data Catalog: {db_name}.{table_name}")

        except Exception as e:
            error_msg: str = f"Failed to write data to Data Catalog: {e}"
            self.log_error(message=error_msg)
            raise DataCatalogServiceError(error_msg) from e

    def write_to_s3(
        self,
        df: "DynamicFrame | DataFrame",
        target_path: str,
        format: str = "parquet",
        partition_keys: Optional[List[str]] = None,
        transformation_ctx: Optional[str] = None,
        mode: Literal['overwrite','append','error','ignore'] ="overwrite"
        ) -> None:
        """
        Write a DynamicFrame or DataFrame to S3 in the specified format.

        :param df: DataFrame or DynamicFrame
        :param target_path: S3 path
        :param format: Data format (default 'parquet')
        :param partition_keys: List of partition keys
        :param transformation_ctx: Optional context name for job bookmarking
        :param mode: Save mode (e.g., 'overwrite', 'append')
        :raises Exception: if writing fails
        """
        from awsglue import DynamicFrame
        try:
            if isinstance(df, DataFrame):
                df = DynamicFrame.fromDF(dataframe=df, glue_ctx=self.glue_context, name="dynamic_frame")
            
            if not transformation_ctx:
                transformation_ctx="default_write_ctx"
                
            self.glue_context.write_dynamic_frame.from_options(
                frame=df,
                connection_type="s3",
                connection_options={"path": target_path,
                                    "partitionKeys": partition_keys or [],
                                    mode:mode},
                format=format,
                transformation_ctx=transformation_ctx
            )
            self.log_info(message=f"successfully save DynamicFrame to {target_path}")
        except Exception as e:
            self.log_error(message=f"error save DynamicFrame to {target_path}: {e}")
            raise
        
    def load_from_s3(self,
        source_path: str,
        format: str = "parquet",
        recurse: bool = True,
        transformation_ctx: Optional[str] = None
        ) -> DataFrame:
        """
            Loads data from S3 into a DataFrame.

            :param source_path: S3 path from where data will be read.
            :param format: Data format (default 'parquet').
            :param partition_keys: List of partition keys to read.
            :param transformation_ctx: Optional context name for job bookmarking.
            :return: Instance of DataWriter with loaded data.
            :raises DataWriterError: If loading fails.
        """
        try:
            if not transformation_ctx:
                transformation_ctx = "default_read_ctx"
            
            connection_options: Dict = {
                "paths": [source_path],
                "recurse": recurse
            }

            dynamic_frame: "DynamicFrame" = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options=connection_options,
                format=format,
                transformation_ctx=transformation_ctx
            )
            res: "DataFrame" = dynamic_frame.toDF()
            self.log_info(f"Successfully loaded data from {source_path}")
        except Exception as e:
            error_msg: str = f"Error loading data from {source_path}: {e}"
            self.log_error(message=error_msg)
            raise DataCatalogServiceError(error_msg)

        return res