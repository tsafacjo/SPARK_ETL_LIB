import logging
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_list

from etl_spark.core.transform import Transformer

class AirpordAfterIPhoneTransformer(Transformer):
    def transform(self, inputDFs):
        """
        customer who have bought Airpods after buying the iphone
        """
        transactionInputDF = inputDFs.get("transactionsInputDF")
        logging.info("TransactionInputDF in transform")
        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformed_df = transactionInputDF.withColumn("next_product_name",
                                                       lead("product_name").over(windowSpec)
                                                       )
        logging.info("AIrpods after buyig iphone")

        transformed_df.orderBy("customer_id", "transaction_date", "product_name").show()

        logging.info("filtering")

        filtered_df = transformed_df.filter((col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods'))

        customersInputDf = inputDFs.get("customersInputDf")
        joinDF = filtered_df.join(broadcast(customersInputDf), "customer_id")
        joinDF.show()
        return joinDF
