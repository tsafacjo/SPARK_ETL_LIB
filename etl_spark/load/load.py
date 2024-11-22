from etl_spark.core.load import AbstractLoader


class AirPodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
            sink_type="dbfs",
            df = self.transformedDF,
            path = "/FileStore/tables/apple_analysis/output/airpodsAfterIPhone",
            method = "overwrite"
        ).load_data_frame()


class OnlyAirPodsAndIphoneLoader(AbstractLoader):
    def sink(self):
        params = {
            "partitionByColumns" :["location"]}
        get_sink_source(
            sink_type="dbfs_with_partition",
            df = self.transformedDF,
            path = "/FileStore/tables/apple_analysis/output/onlyAirpodsAndIPhone",
            method = "overwrite",
            params = params
        ).load_data_frame()