from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("row_id", StringType(), True),
    StructField("source_system_nm", StringType(), True),
    StructField("source_system_field_nm", StringType(), True),
    StructField("source_system_field_value", StringType(), True),
    StructField("source_system_field_value_txt", StringType(), True),
    StructField("creation_ts", StringType(), True),
    StructField("last_modified_ts", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("as_of_date", StringType(), True),
    StructField("process_control_id", StringType(), True)])
