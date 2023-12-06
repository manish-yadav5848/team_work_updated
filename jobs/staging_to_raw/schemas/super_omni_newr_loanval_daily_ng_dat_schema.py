from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("div_sub_id", StringType(), True),
    StructField("first_payment_date", StringType(), True),
    StructField("interest_paid", StringType(), True),
    StructField("loan_payment_frequency_long_desc", StringType(), True),
    StructField("loan_status_date", StringType(), True),
    StructField("loan_status_desc", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("loan_use_indicator", StringType(), True),
    StructField("loan_number", StringType(), True)])
