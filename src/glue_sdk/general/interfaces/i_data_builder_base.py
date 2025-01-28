from abc import ABC,abstractmethod
from typing import Optional
from typing_extensions import Self

class IDataBuilder(ABC):
    """
    Interface for the DataCatalogBuilderBase class.
    """

    @abstractmethod
    def to_camel_case_headers(self) -> Self:
        """
        Convert DataFrame column headers to camelCase with separator '_'.

        :return: self
        """
        pass

    @abstractmethod
    def cache_data(self, key: Optional[str] = None) -> Self:
        """
        Cache the current Spark DataFrame.

        :param key: Optional cache key override
        :return: self
        """
        pass

    @abstractmethod
    def load_from_cache(self, key: str) -> Self:
        """
        Load data from cache using the provided key.

        :param key: Cache key
        :return: self
        """
        pass

    @abstractmethod
    def _set_df_from_data(self) -> None:
        """
        Internal helper to ensure self.data is stored as a Spark DataFrame in self.df.
        """
        pass

    @abstractmethod
    def _ensure_df(self) -> None:
        """
        Ensures that self.df is set.
        """
        pass
    
    @abstractmethod
    def flatten_df(self,
                   sep: str = ".",
                   lower_case: bool = True
                   )-> Self:
        pass