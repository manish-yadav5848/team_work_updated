from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField('CLIENT_ID', StringType(), True),
    StructField('PLAN_ID', StringType(), True),
    StructField('DIV_SUB_ID', StringType(), True),
    StructField('FUND_ID', StringType(), True),
    StructField('AS_OF_DATE', StringType(), True),
    StructField('UNIT_PRICE', StringType(), True),
    StructField('UNIT_SHARE_BAL', StringType(), True),
    StructField('FUND_BAL_AMT', StringType(), True),
    StructField('FUND_VESTED_BAL', StringType(), True)])
