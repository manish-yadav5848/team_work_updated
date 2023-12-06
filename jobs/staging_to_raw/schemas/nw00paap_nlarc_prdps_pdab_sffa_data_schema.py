from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("plan_number", StringType(), True),
    StructField("company_code", StringType(), True),
    StructField("soc_sec_no", StringType(), True),
    StructField("policy_number", StringType(), True),
    StructField("system_code", StringType(), True),
    StructField("fund_number", StringType(), True),
    StructField("money_source", StringType(), True),
    StructField("process_date", StringType(), True),
    StructField("allocation_pct", StringType(), True)])
