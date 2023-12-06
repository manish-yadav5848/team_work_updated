from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("company_code", StringType(), True),
    StructField("market_indicator", StringType(), True),
    StructField("market_code", StringType(), True),
    StructField("market_segment_code", StringType(), True),
    StructField("market_segment_category_desc", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("process_control_id", StringType(), True)])
