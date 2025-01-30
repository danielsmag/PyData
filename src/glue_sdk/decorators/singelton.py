import threading
from functools import wraps




def _thread_safe_singleton(cls):
    _instance_lock = threading.Lock()
    _instances = {}

    @wraps(cls)
    def get_instance(*args, **kwargs):
        with _instance_lock:
            if cls not in _instances:
                _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance

def singleton(cls):
    """
    A singleton decorator. Returns a wrapper objects. A call on that object
    returns a single instance object of decorated class.
    """
    return _thread_safe_singleton(cls)