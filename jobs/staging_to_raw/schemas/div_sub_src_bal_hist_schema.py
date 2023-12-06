from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
        StructField('CLIENT_ID', StringType(), True),
        StructField('PLAN_ID', StringType(), True), 
        StructField('DIV_SUB_ID', StringType(), True), 
        StructField('MONEY_SRC', StringType(), True), 
        StructField('AS_OF_DATE', StringType(), True), 
        StructField('UNIT_SHARE_BAL', StringType(), True), 
        StructField('SOURCE_BAL_AMT', StringType(), True), 
        StructField('SOURCE_VESTED_BAL', StringType(), True)])
