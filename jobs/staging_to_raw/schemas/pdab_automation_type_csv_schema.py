from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("automation_code", StringType(), True),
    StructField("automation_type_description", StringType(), True),
    StructField("automation_code_id", StringType(), True),
    StructField("automation_type_id", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("as_of_date", StringType(), True),
    StructField("process_control_id", StringType(), True)])
