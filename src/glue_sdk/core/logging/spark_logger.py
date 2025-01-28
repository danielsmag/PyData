from typing import TYPE_CHECKING,Any, List

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    
    
class Log4j:
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, 
                spark:"SparkSession"
                ) -> None:
        self.spark: "SparkSession" = spark 
        conf: "SparkConf" = spark.sparkContext.getConf()
        app_id: str | None = conf.get('spark.app.id')
        app_name: str | None = conf.get('spark.app.name')

        log4j: Any = spark._jvm.org.apache.log4j # type: ignore
        if not log4j:
            raise Exception("No log4j")
        if not (conf and app_id and app_name):
            raise Exception(f"Missing conf: {conf}, app_id {app_id}, app_name: {app_name}")
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def set_log_level(self, level: str): 
        """Set the logging level.""" 
        valid_levels: List[str] = ["ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"]
        level = level.upper() 
        if level not in valid_levels: 
            raise ValueError(f"Invalid log level: {level}. Choose from {valid_levels}") 
        self.logger.setLevel(getattr(self.spark._jvm.org.apache.log4j.Level, level)) # type: ignore
        
    def error(self, msg) -> None: 
        """Log an error.""" 
        self.logger.error(msg) 
        
    
    def warn(self, msg) -> None: 
        """Log a warning.""" 
        self.logger.warn(msg) 
    
    def info(self, msg) -> None: 
        """Log information.""" 
        self.logger.info(msg) 
    
    def debug(self, msg) -> None:
        """Log debug"""
        self.logger.debug(msg)