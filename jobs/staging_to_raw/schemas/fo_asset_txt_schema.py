from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("asset_code", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("risk_rtn", StringType(), True),
    StructField("asset_class_desc", StringType(), True),
    StructField("asset_class_type", StringType(), True)])
