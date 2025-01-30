from abc import ABC, abstractmethod
from typing import Any  

class IClient(ABC):
    @abstractmethod
    def create_client(self) -> Any:
        pass

 