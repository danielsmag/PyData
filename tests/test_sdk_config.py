import pytest
from glue_sdk.sdk.sdk_config import SdkConf
from glue_sdk.core.shared import ServicesEnabled


# If your singleton decorator stores the instance in an attribute (e.g. _instance),
# we can clear it between tests to ensure test isolation.


def test_default_services():
    """Test that a new SdkConf has a valid ServicesEnabled instance and default booleans."""
    conf = SdkConf()
    # Check that services_enabled is an instance of ServicesEnabled.
    assert isinstance(conf.services_enabled, ServicesEnabled)
    # Check that each service flag property returns a boolean.
    assert isinstance(conf.USE_OPENSEARCH, bool)
    assert isinstance(conf.USE_DATA_CATALOG, bool)
    assert isinstance(conf.USE_AURORA_PG, bool)
    assert isinstance(conf.USE_CACHE, bool)
    assert isinstance(conf.USE_DATA_BUILDERS, bool)
    assert isinstance(conf.USE_GLUE, bool)
    assert isinstance(conf.USE_SPARK, bool)
    assert isinstance(conf.USE_EMR, bool)


def test_set_services_to_use():
    """Test that set_services_to_use correctly updates service flags."""
    conf = SdkConf()
    conf.set_services_to_use(
        USE_OPENSEARCH=True,
        USE_DATA_CATALOG=True,
        USE_AURORA_PG=True,
        USE_CACHE=False,
        USE_DATA_BUILDERS=False,
        USE_GLUE=False,
        USE_SPARK=False,
        USE_EMR=True,
    )
    assert conf.USE_OPENSEARCH is True
    assert conf.USE_DATA_CATALOG is True
    assert conf.USE_AURORA_PG is True
    assert conf.USE_CACHE is False
    assert conf.USE_DATA_BUILDERS is False
    assert conf.USE_GLUE is False
    assert conf.USE_SPARK is False
    assert conf.USE_EMR is True


def test_set_spark_conf():
    """Test that set_spark_conf assigns the spark configuration dictionary."""
    conf = SdkConf()
    spark_conf = {"master": "local", "appName": "TestApp"}
    conf.set_spark_conf(spark_conf)
    assert conf._spark_conf == spark_conf


def test_singleton_behavior():
    """Test that SdkConf is a singleton (i.e. multiple instantiations return the same object)."""
    conf1 = SdkConf()
    conf2 = SdkConf()
    assert conf1 is conf2
