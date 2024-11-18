from etl_spark.datasource.datasource import CSVDataSource

class TestCSVDataSource():

    def test_should_read_a_csv_source(self, spark_session):
         file_path = "./tests/resource/sample.csv"
         csvsource = CSVDataSource(spark_session, file_path)
         exepected_nb_rows = 1
         df_test = csvsource.get_data_frame()
         assert df_test.count() == exepected_nb_rows, f" the number of rown should be equals to ${exepected_nb_rows}"