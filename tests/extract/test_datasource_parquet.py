from etl_spark.datasource.datasource import ParquetDataSource

class TestParquetDataSource():

    def test_should_read_a_parquet_source(self, spark_session):
         file_path = "./tests/resource/userdata1.parquet"
         parquet_source = ParquetDataSource(spark_session, file_path)
         exepected_nb_rows = 1000
         exepected_nb_columns = 13
         df_pq = parquet_source.get_data_frame()
         assert len(df_pq.columns) == exepected_nb_columns, f" the number of columns should be equals to {exepected_nb_columns}"
         assert df_pq.count() == exepected_nb_rows, f" the number of rows should be equals to {exepected_nb_rows}"