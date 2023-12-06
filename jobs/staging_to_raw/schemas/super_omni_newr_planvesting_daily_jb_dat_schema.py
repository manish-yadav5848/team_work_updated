from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("money_source_code", StringType(), True),
    StructField("vesting_method", StringType(), True),
    StructField("vesting_schedule", StringType(), True),
    StructField("vesting_schedule_label", StringType(), True),
    StructField("vesting_schedule_percent", StringType(), True),
    StructField("vesting_schedule_period", StringType(), True)])
