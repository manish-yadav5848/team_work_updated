from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("deferral_rate_actual", StringType(), True),
    StructField("money_source_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("money_source_code", StringType(), True)])
