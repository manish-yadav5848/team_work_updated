from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("employer_ein", StringType(), True),
    StructField("employer_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("street_address_1", StringType(), True),
    StructField("street_address_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("foreign_state_address", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("as_of_date", StringType(), True)])
