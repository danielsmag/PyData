from typing import Dict, Optional, TYPE_CHECKING
from ...core.logging.logger import logger
from pyspark.context import SparkContext
from pyspark.sql import SparkSession   
from awsglue.context import GlueContext


class SparkClientError(Exception):
    pass

class SparkClient:
    
    def __init__(self, 
                 env: str | None = None,
                 dynamic_configs_spark_client: Optional[Dict[str,str]] = None
                 ) -> None:
        
        self._spark_context: "SparkContext | None" = None
        self._glue_context: "GlueContext | None" = None
        self._spark_session: "SparkSession | None" = None
        self.env: str | None = env
        self.dynamic_configs_spark_client: Dict[str,str] = dynamic_configs_spark_client or {}
        self.spark_conf: Dict[str,str] = {}

    def _get_spark_conf(self) -> Dict[str, str]:
        iceberg_configs: Dict[str, str]= {
            "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.glue_catalog.warehouse": "s3://migdal-data-silver/Dev/ods/AGENT_DESK/iceberg/"
            
        }
        adaptive_configs: Dict[str, str] = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1"
        }
        all_configs: Dict[str, str] = {**iceberg_configs, 
                                       **adaptive_configs,
                                       **self.dynamic_configs_spark_client}
        return all_configs
    
    @property
    def spark_context(self) -> "SparkContext":
        if not self._spark_context:
            self._initialize_spark_context()
        if not self._spark_context:
            raise SparkClientError("No spark_context to return")
        return self._spark_context

    @property
    def glue_context(self) -> "GlueContext":
        if not self._glue_context:
            _: "SparkContext" = self.spark_context
            self._initialize_glue_context()
        if not self._glue_context:
            raise SparkClientError("No spark_context to return")
        return self._glue_context

    @property
    def spark_session(self) -> "SparkSession":
        if not self._spark_session:
            _: "GlueContext" = self.glue_context
            self._initialize_spark_session()
            self.spark_conf = self._get_spark_conf()
            self._apply_spark_conf()
        if not self._spark_session:
            raise SparkClientError("No spark_session to return")
        return self._spark_session

    def _initialize_spark_session(self) -> None:
        """Initialize SparkSession via GlueContext."""
        if not self._glue_context:
            raise SparkClientError(RuntimeError("GlueContext must be initialized before SparkSession"))
        if not self._spark_session:
            self._spark_session = self._glue_context.spark_session
            logger.debug("SparkSession initialized.")
        else:
            logger.debug("SparkSession already initialized.")

    def _initialize_spark_context(self) -> None:
        """initialize spark context"""
        if not self._spark_context:
            self._spark_context = SparkContext.getOrCreate()
            logger.debug("create spark context")
        else:
            logger.debug("spark context exist")
          
    def _initialize_glue_context(self) -> None:
        """Initialize GlueContext using the existing SparkContext."""
        if not self._spark_context:
            raise SparkClientError(RuntimeError("SparkContext must be initialized before GlueContext."))
        if not self._glue_context:
            self._glue_context = GlueContext(sparkContext=self._spark_context)
            logger.debug("GlueContext initialized.")
        else:
            logger.debug("GlueContext already initialized.")
               
    def _apply_spark_conf(self) -> None:
        for key, value in self.spark_conf.items():
            self.spark_session.conf.set(key=key, value=value)

    def stop(self) -> None:
        if self.spark_session:
            self.spark_session.stop()
         
        if self.spark_context:
            self.spark_context.stop()

