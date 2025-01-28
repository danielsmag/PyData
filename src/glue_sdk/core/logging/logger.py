import logging
from typing import List,Dict, Any
import sys
from awsglue.utils import getResolvedOptions

__all__:List[str] = [
    "logger"
]

def setup_logger(name: str = "job_logger") -> logging.Logger:
    try:
        args: Dict[str, Any] = getResolvedOptions(args=sys.argv, options=["LOG_LEVEL"])
        log_level_str: str = args.get('LOG_LEVEL', 'DEBUG').upper()
    except:
        log_level_str= "DEBUG" 
        print(f"logger fail import --LOG_LEVEL and run default {log_level_str}")
        
    log_level: Any | int = getattr(logging, log_level_str, logging.INFO)

    logger: logging.Logger = logging.getLogger(name=name)
    logger.info(msg=f"Logger run on {log_level_str} level ")
    if not logger.hasHandlers():
        logger.setLevel(level=log_level)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            fmt='[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger

logger: logging.Logger = setup_logger()

