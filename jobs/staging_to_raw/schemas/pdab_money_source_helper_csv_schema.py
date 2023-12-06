from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("row_id", StringType(), True),
    StructField("comp_cd", StringType(), True),
    StructField("asset_acct", StringType(), True),
    StructField("user_area", StringType(), True),
    StructField("code_name", StringType(), True),
    StructField("pwe_money_source_code", StringType(), True),
    StructField("pwe_money_source_name", StringType(), True),
    StructField("source_cd", StringType(), True),
    StructField("money_type_code", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("as_of_date", StringType(), True),
    StructField("process_control_id", StringType(), True)])
