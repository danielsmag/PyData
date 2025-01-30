import threading
from functools import wraps
import functools
from typing import Callable, Optional
from ...core.models.results_model import FuncResult
from ..logging.logger import logger
import time



def singelton(cls):
    """safe threads singelton decorator"""
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