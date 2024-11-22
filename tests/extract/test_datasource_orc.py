from etl_spark.datasource.datasource import OrcDataSource

class TestOrcDataSource():

    def test_should_read_a_orc_source(self, spark_session):
         #GIVEN

         file_path = "./tests/resource/orc-file-11-format.orc"
         orc_source = OrcDataSource(spark_session, file_path)
         exepected_nb_columns = 14
         exepected_nb_rows = 7500

         #WHEN
         df_orc = orc_source.get_data_frame()
         #THEN
         assert len(df_orc.columns) == exepected_nb_columns, f" the number of columns should be equals to {exepected_nb_columns}"
         assert df_orc.count() == exepected_nb_rows, f" the number of rows should be equals to {exepected_nb_rows}"