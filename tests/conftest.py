import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Creates a SparkSession instance for testing."""
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

