import threading
from functools import wraps
import functools
from typing import Callable, Optional,Any
from ...core.models.results_model import FuncResult
from ..logging.logger import logger
import time
from botocore.exceptions import ClientError


def singleton(cls):
    """safe threads singleton decorator"""
    _instance_lock = threading.Lock()
    _instance = {}
    
    @wraps(cls)
    def get_instance(*args, **kwargs):
        with _instance_lock:
            if cls not in _instance:
                _instance[cls] =cls(*args,**kwargs)
        return _instance[cls]
    
    return get_instance

def validate_processed(action_name: str, on_failure: Optional[Callable] = None,*on_failure_args, **on_failure_kwargs):
    def decorator(func: Callable):
        start_time: float = time.time()
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result: FuncResult = func(*args, **kwargs)
                if not result.processed:
                    logger.warning(msg=f"Error during {action_name}")
                    raise Exception(f"Error during {action_name}")
                logger.info(msg=f"action {action_name} finish successfully: {result}")
                return result
            except Exception as e:
                logger.warning(msg=f"Exception in {action_name}: {e}")
                if on_failure:
                    logger.info(msg=f"failure handler for {action_name}")
                    failure_result = on_failure(*on_failure_args, **on_failure_kwargs)
                    logger.info(msg=f"failure handler for {action_name} returned: {failure_result}")
                    return failure_result
                raise e  
        end_time: float = time.time()
        logger.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return wrapper
    return decorator

class HandleS3Exception(Exception):
    pass

def handle_s3_exception(return_on_failure: Any = None, raise_exception: bool = False):
    def decorator(func):
        @functools.wraps(wrapped=func)
        def wrapper(*args, **kwargs) -> Any:
            # Override decorator arguments with function call arguments if provided
            rof: Any = kwargs.pop('return_on_failure', return_on_failure)
            re: bool = kwargs.pop('raise_exception', raise_exception)
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                error_code: str = e.response['Error']['Code']
                match error_code:
                    case "NoSuchKey":
                        logger.info(f"Error: The specified key does not exist {e}")
                    case "NoSuchBucket":
                        logger.info(f"Error: The specified bucket does not exist {e}")
                    case _:
                        logger.info(f"An error occurred in S3 operation: {e} Error code: {error_code}")
                return rof
            except KeyError as e:
                msg: str = f"KeyError: Missing key in response {e}"
                logger.warning(msg=msg)
                if re:
                    raise HandleS3Exception(msg)
                return rof
            except Exception as e:
                msg = f"Unexpected error: {e}"
                logger.error(msg=msg)
                if re:
                    raise HandleS3Exception(msg)
                return rof
        return wrapper
    return decorator