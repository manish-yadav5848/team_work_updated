from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type_code", StringType(), True),
    StructField("type_name", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("status_name", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("as_of_date", StringType(), True)])
