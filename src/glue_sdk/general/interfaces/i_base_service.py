from abc import ABC, abstractmethod
from typing import Optional, Any, List, Literal

class IBaseService(ABC):
    
    @abstractmethod
    def log_debug(self, message: str) -> None:
        pass
    @abstractmethod      
    def log_info(self, message: str) -> None:
        pass
    @abstractmethod
    def log_error(self, message: str) -> None:
        pass
    @abstractmethod
    def log_warning(self, message: str) -> None:
        pass
   