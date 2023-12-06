from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("plan_number", StringType(), True),
    StructField("company_code", StringType(), True),
    StructField("tin", StringType(), True),
    StructField("payment_group", StringType(), True),
    StructField("automation_method", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("as_of_date", StringType(), True),
    StructField("process_control_id", StringType(), True)])
