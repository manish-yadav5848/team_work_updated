from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("agency_code", StringType(), True),
    StructField("market_segment_code", StringType(), True),
    StructField("field_office_description", StringType(), True)
])
