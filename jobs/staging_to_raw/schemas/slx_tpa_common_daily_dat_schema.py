from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("filler1", StringType(), True),
    StructField("tpa_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("tpa_tin", StringType(), True),
    StructField("filler2", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("billing_address_street_1", StringType(), True),
    StructField("billing_address_street_2", StringType(), True),
    StructField("billing_address_city", StringType(), True),
    StructField("billing_address_state", StringType(), True),
    StructField("billing_address_postal_code", StringType(), True)])
