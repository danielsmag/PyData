from abc import ABC, abstractmethod
from typing import Any, Dict

class ICache(ABC):
    
    @abstractmethod
    def get(self, key: str, type_assert: Any = None,default: Any = None) -> Any:
        pass

    @abstractmethod
    def set(self, key: str, value: Any, timeout: int = 5) -> None:
        pass

    @abstractmethod
    def update(self, updates: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def all(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass
    
    @abstractmethod
    def has_key(self, key: str) -> bool:
        pass