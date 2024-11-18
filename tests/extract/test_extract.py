from pyspark.sql import SparkSession


class TestExtract:

  def test_recreate_df(self, spark_session):

        df = spark_session.range(10)
        assert df.count() == 11