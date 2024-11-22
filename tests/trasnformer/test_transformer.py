from mockito import unstub, verify
from pyspark.sql import Row

from etl_spark.ecommerce.transform.airpord_after_iphone_transformer import AirpordAfterIPhoneTransformer


class TestTransformer:

    def test_airpods_after_iphone_transformer(self, spark_session):
        # Prepare test data
        transactions_data = [
            Row(customer_id=1, product_name="iPhone", transaction_date="2024-01-01"),
            Row(customer_id=1, product_name="AirPods", transaction_date="2024-01-02"),
            Row(customer_id=2, product_name="iPhone", transaction_date="2024-01-01"),
            Row(customer_id=2, product_name="MacBook", transaction_date="2024-01-03"),
        ]

        customers_data = [
            Row(customer_id=1, customer_name="John Doe"),
            Row(customer_id=2, customer_name="Jane Smith"),
        ]

        transactions_input_df = spark_session.createDataFrame(transactions_data)
        customers_input_df = spark_session.createDataFrame(customers_data)

        input_dfs = {
            "transactionsInputDF": transactions_input_df,
            "customersInputDf": customers_input_df,
        }

        transformer = AirpordAfterIPhoneTransformer()

        # Run the method
        output_df = transformer.transform(input_dfs)

        # Expected output
        expected_data = [
            Row(customer_id=1, customer_name="John Doe", transaction_date="2024-01-03", product_name="iPhone",
                next_product_name="AirPods"),
        ]
        expected_df = spark_session.createDataFrame(expected_data)

        # Validate output
        assert output_df is not None
        assert output_df.schema == expected_df.schema
        assert output_df.collect() == expected_df.collect()

        # Verify expected Spark actions were called
        verify(transformer).transform(...)

        # Clean up mockito
        unstub()
