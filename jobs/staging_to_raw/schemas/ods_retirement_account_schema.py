from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("cust_ods_agreement_id", StringType(), True),
    StructField("source_cycle_timestamp", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_system_underlying", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("upd_ts", StringType(), True),
    StructField("upd_by", StringType(), True),
    StructField("check_sum", StringType(), True),
    StructField("end_timestamp", StringType(), True)])
