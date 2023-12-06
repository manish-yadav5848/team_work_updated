from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("base_transaction_code", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("usage_code_1", StringType(), True),
    StructField("usage_code_2", StringType(), True),
    StructField("usage_code_3", StringType(), True),
    StructField("usage_code_4", StringType(), True),
    StructField("usage_code_5", StringType(), True),
    StructField("standard_usage_code_1", StringType(), True),
    StructField("standard_usage_code_2", StringType(), True),
    StructField("standard_usage_code_3", StringType(), True),
    StructField("standard_usage_code_4", StringType(), True),
    StructField("standard_usage_code_5", StringType(), True),
    StructField("category", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("activity_code_description", StringType(), True),
    StructField("process_flag", StringType(), True),
    StructField("last_contribution", StringType(), True)])
