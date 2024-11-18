
from etl_spark.core.datasource import DataSource

class CSVDataSource(DataSource):
    def get_data_frame(self):
        return (self.spark.read.format("csv").options(header=True).load(self.path))

class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return (self.spark.read.format("parquet").load(self.path))

class DeltaDataSource(DataSource):
    def get_data_frame(self):
        table_name = self.path
        return (self.spark.read.table(table_name))

class OrcDataSource(DataSource):
    def get_data_frame(self):
        return (self.read.format("delta").load(self.path))