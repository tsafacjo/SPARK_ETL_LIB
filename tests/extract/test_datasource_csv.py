from etl_spark.datasource.datasource import CSVDataSource


class TestCSVDataSource():

    def test_should_read_a_csv_source(self, spark_session):
        file_path = "./tests/resource/sample.csv"
        csvsource = CSVDataSource(spark_session, file_path)
        exepected_nb_rows = 1
        exepected_nb_columns = 2
        df_test = csvsource.get_data_frame()
        assert len(
            df_test.columns) == exepected_nb_columns, f" the number of columns should be equals to ${exepected_nb_columns}"
        assert df_test.count() == exepected_nb_rows, f" the number of rows should be equals to ${exepected_nb_rows}"
