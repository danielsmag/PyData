from threading import RLock
from typing import List, Any, Dict, Optional
from .i_cache import ICache
import time
from abc import ABCMeta
from pydantic import validate_call

__all__: List[str] = ["SharedDataService"]


class SingletonMeta(type):

    _instances: Dict = {}

    _lock: RLock = RLock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonABCMeta(SingletonMeta, ABCMeta):
    pass


class SharedDataService(ICache, metaclass=SingletonABCMeta):

    def __init__(self, cache_timeout: Optional[int] = None) -> None:
        self._shared_data: Dict[str, Any] = {}
        self._locks: Dict[str, RLock] = {}
        self._global_lock = RLock()
        self._lock = RLock()
        self.cache_timeout: int | None = cache_timeout
        print(2222222)

    def _chek_type(self, key: str, value: Any, type_assert: Any):
        assert isinstance(
            value, type_assert
        ), f"Missing: get. {key} have to be type: {type} but {type(value)}"

    def reset(self) -> None:

        with self._global_lock:
            self._shared_data.clear()
            self._locks.clear()

    def _get_lock(self, key: str) -> RLock:
        with self._lock:
            if key not in self._locks:
                self._locks[key] = RLock()
            return self._locks[key]

    @validate_call
    def get(self, key: str, type_assert: Any = None, default: Any = None) -> Any:
        lock: RLock = self._get_lock(key=key)
        with lock:
            value = self._shared_data.get(key, default)
            if type_assert:
                self._chek_type(key=key, value=value, type_assert=type_assert)
            if isinstance(value, (dict, list)):
                return value.copy()
            return value

    @validate_call
    def set(self, key: str, value: Any, timeout: int = 5) -> None:
        lock: RLock = self._get_lock(key=key)
        start_time: float = time.time()
        while not lock.acquire(blocking=False):
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Failed to acquire lock for key '{key}' within {timeout} seconds."
                )
            time.sleep(0.01)
        try:
            self._shared_data[key] = value
        finally:
            lock.release()

    def update(self, updates: Dict[str, Any]) -> None:
        with self._global_lock:
            self._shared_data.update(updates)

    def all(self) -> Dict[str, Any]:
        with self._global_lock:
            return self._shared_data.copy()

    def delete(self, key: str) -> None:
        lock: RLock = self._get_lock(key=key)
        with lock:
            if key in self._shared_data:
                del self._shared_data[key]
                with self._lock:
                    del self._locks[key]

    def has_key(self, key: str) -> bool:
        with self._global_lock:
            return key in self._shared_data
