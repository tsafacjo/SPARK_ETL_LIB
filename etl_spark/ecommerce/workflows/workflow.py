from etl_spark.core.workflow import WorkFlow


class FirstWorkFlow:
  """
  ETL Pipeline to list all customers who have bought Airpods just after buying iPhone
  """
  def __init__(self):
    pass
  def runner(self):
      #eStep 1 extract data
      input_dfs =  AirpordAfterIPhone().extract()
      # Step 2 Customers who have bought Airpods after buying the Iphone
      firstTransactionDf =  AirpordAfterIPhoneTransformer().transform(input_dfs)
      #Step 3 Load data
      AirPodsAfterIphoneLoader(firstTransactionDf).sink()

firstworkflow = FirstWorkFlow()

firstworkflow.runner()