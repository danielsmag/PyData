from threading import RLock
from functools import wraps

_singleton_lock = RLock()
_singleton_instances = {}


def singleton(cls):
    """Thread-safe singleton decorator"""

    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in _singleton_instances:
            with _singleton_lock:
                if cls not in _singleton_instances:
                    _singleton_instances[cls] = cls(*args, **kwargs)
        return _singleton_instances[cls]

    return get_instance


@singleton
class ServicesEnabled:
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

    def reset_to_defaults(self):
        """
        Resets all configuration attributes to their default values in a thread-safe manner.
        """
        defaults = {
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


# class ServicesEnabled:
#     USE_OPENSEARCH: bool = False
#     USE_AURORA_PG: bool = False
#     USE_DATA_CATALOG: bool = False
#     USE_CACHE: bool = True
#     USE_DATA_BUILDERS: bool = True
#     USE_GLUE: bool = True
#     USE_SPARK: bool = True
#     USE_EMR: bool = False

#     @staticmethod
#     def _update_values(**overrides):
#         """
#         Updates the singleton instance with new values.
#         This is the ONLY method allowed to modify the instance.
#         """
#         # from glue_sdk.core.decorators.decorators import _singleton_instances,_singleton_lock
#         with _singleton_lock:
#             if ServicesEnabled not in _singleton_instances:
#                 _singleton_instances[ServicesEnabled] = ServicesEnabled()

#             instance = _singleton_instances[ServicesEnabled]
#             new_instance = replace(instance, **overrides)
#             _singleton_instances[ServicesEnabled] = new_instance
#         return new_instance
