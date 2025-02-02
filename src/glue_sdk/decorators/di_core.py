from __future__ import annotations
from typing import List, Callable, TYPE_CHECKING, Any, ParamSpec, TypeVar
import functools
from awsglue.job import Job


from ..core.shared import SharedUtilsSettings

if TYPE_CHECKING:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from glue_sdk.containers.application_container import ApplicationContainer

__all__: List[str] = [
    "spark_session",
    "spark_context",
    "glue_context",
    "glue_job",
]

P = ParamSpec("P")
R = TypeVar("R")

shared_settings = SharedUtilsSettings()
container: ApplicationContainer = shared_settings.container


def spark_session(func: Callable):
    """
    Decorator that injects the SparkSession (from Core.spark_session) into the function.
    Passes the SparkSession as `spark_session` kwarg.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        spark_session = container.core.spark_session()
        kwargs["spark_session"] = spark_session
        return func(*args, **kwargs)

    return wrapper


def spark_context(func: Callable):
    """
    Decorator that injects the SparkContext (from Core.spark_context).
    Passes the SparkContext as `spark_context` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer

    @functools.wraps(func)
    def wrapper(
        *args,
        spark_context: SparkContext = Provide[ApplicationContainer.core.spark_context],
        **kwargs,
    ) -> Any:
        kwargs["spark_context"] = spark_context
        return func(*args, **kwargs)

    return wrapper


def glue_context(func: Callable):
    """
    Decorator that injects the GlueContext (from Core.glue_context).
    Passes the GlueContext as `glue_context` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer

    @inject
    @functools.wraps(func)
    def wrapper(
        *args,
        glue_context: GlueContext = Provide[ApplicationContainer.core.glue_context],
        **kwargs,
    ) -> Any:
        kwargs["glue_context"] = glue_context
        return func(*args, **kwargs)

    return wrapper


def glue_job(func: Callable):
    """
    Decorator that injects the Glue Job instance (from Core.glue_job).
    Passes the job as `glue_job` kwarg.
    """
    from glue_sdk.containers.application_container import ApplicationContainer

    @inject
    @functools.wraps(func)
    def wrapper(
        *args, glue_job: Job = Provide[ApplicationContainer.core.glue_job], **kwargs
    ) -> Any:
        kwargs["glue_job"] = glue_job
        return func(*args, **kwargs)

    return wrapper
