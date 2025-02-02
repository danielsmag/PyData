from __future__ import annotations
from threading import RLock
from functools import wraps,cached_property
from typing import TYPE_CHECKING, Optional,ForwardRef
from pydantic import validate_call

if TYPE_CHECKING:
    from ..containers import ApplicationContainer

ApplicationContainerRef = ForwardRef("ApplicationContainer")

class SingletonMeta(type):
    """
    A thread-safe implementation of a Singleton metaclass.
    """

    _instances = {}
    _lock = RLock()  # Lock object to synchronize threads during first access

    def __call__(cls, *args, **kwargs):
        # First, check if the instance already exists (without acquiring the lock)
        if cls not in cls._instances:
            with cls._lock:
                # Double-check if the instance was created while waiting for the lock
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]


class ServicesEnabled(metaclass=SingletonMeta):
    def __init__(self):
        self._lock = RLock()
        self.USE_OPENSEARCH = False
        self.USE_AURORA_PG = False
        self.USE_DATA_CATALOG = False
        self.USE_CACHE = True
        self.USE_DATA_BUILDERS = True
        self.USE_GLUE = True
        self.USE_SPARK = True
        self.USE_EMR = False

    def update_values(self, **overrides):
        """
        Updates the instance attributes with provided overrides in a thread-safe manner.

        Args:
            **overrides: Key-value pairs representing attributes to update.

        Raises:
            AttributeError: If an invalid attribute is provided.
        """
        with self._lock:
            for key, value in overrides.items():
                if hasattr(self, key):
                    setattr(self, key, value)
                else:
                    raise AttributeError(
                        f"'{key}' is not a valid attribute of ServicesEnabled"
                    )

    @classmethod
    def reset(cls):
        """
        Resets the singleton instance, causing a new instance to be created upon next instantiation.
        Use with caution, as this will remove the existing singleton instance.
        """
        with SingletonMeta._lock:
            if cls in SingletonMeta._instances:
                del SingletonMeta._instances[cls]

    def reset_to_defaults(self):
        """
        Resets all configuration attributes to their default values in a thread-safe manner.
        """
        defaults: dict[str, bool] = {
            "USE_OPENSEARCH": False,
            "USE_AURORA_PG": False,
            "USE_DATA_CATALOG": False,
            "USE_CACHE": True,
            "USE_DATA_BUILDERS": True,
            "USE_GLUE": True,
            "USE_SPARK": True,
            "USE_EMR": False,
        }
        with self._lock:
            for key, value in defaults.items():
                setattr(self, key, value)

    def __str__(self):
        """
        Returns a string representation of the current configuration.
        """
        with self._lock:
            attrs = (
                f"USE_OPENSEARCH={self.USE_OPENSEARCH}",
                f"USE_AURORA_PG={self.USE_AURORA_PG}",
                f"USE_DATA_CATALOG={self.USE_DATA_CATALOG}",
                f"USE_CACHE={self.USE_CACHE}",
                f"USE_DATA_BUILDERS={self.USE_DATA_BUILDERS}",
                f"USE_GLUE={self.USE_GLUE}",
                f"USE_SPARK={self.USE_SPARK}",
                f"USE_EMR={self.USE_EMR}",
            )
            return f"ServicesEnabled({', '.join(attrs)})"


class SharedUtilsSettingsError(Exception):
    pass

class SharedUtilsSettings(metaclass=SingletonMeta):
    _container: Optional["ApplicationContainer"]

    def __init__(self):
        self._lock = RLock()
        self._container = None

    @property
    def container(self) -> "ApplicationContainer":
        if not self._container:
            raise SharedUtilsSettingsError("You must set up the container in SharedUtilsSettings.")
        return self._container

    @container.setter
    def container(self, container: "ApplicationContainer") -> None:
        from dependency_injector.containers import DynamicContainer
        if not isinstance(container, DynamicContainer):
            TypeError("U must intialize container before pass to settings")
        self._container = container

    def update_values(self, **overrides):
        with self._lock:
            for key, value in overrides.items():
                if hasattr(self, key):
                    setattr(self, key, value)
                else:
                    raise AttributeError(
                        f"'{key}' is not a valid attribute of SharedUtilsSettings"
                    )

    @classmethod
    def reset(cls):
        with SingletonMeta._lock:
            if cls in SingletonMeta._instances:
                del SingletonMeta._instances[cls]

    def reset_to_defaults(self):
        defaults = {} 
        with self._lock:
            for key, value in defaults.items():
                setattr(self, key, value)

    def __str__(self):
        with self._lock:
            attrs = []  
            return f"SharedUtilsSettings({', '.join(attrs)})"

