from pyspark.sql import SparkSession

class SparkSessionSingleton:
    _spark = None

    @staticmethod
    def get_spark_session():
        if SparkSessionSingleton._spark is None:
            SparkSessionSingleton._spark = SparkSession.builder \
                .appName("DataSourceApp") \
                .master("local[*]") \
                .getOrCreate()
        return SparkSessionSingleton._spark