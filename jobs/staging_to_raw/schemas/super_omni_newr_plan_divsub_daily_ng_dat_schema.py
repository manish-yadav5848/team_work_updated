from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("div_sub_id", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("address_line_3", StringType(), True),
    StructField("address_state", StringType(), True),
    StructField("address_zip_code", StringType(), True),
    StructField("primary_name", StringType(), True),
    StructField("secondary_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True)])
