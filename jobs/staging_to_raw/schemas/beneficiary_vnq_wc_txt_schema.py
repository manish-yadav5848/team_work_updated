from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("beneficiary_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("relationship_other", StringType(), True),
    StructField("name", StringType(), True),
    StructField("street_address", StringType(), True),
    StructField("street_address_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("primary_percentage_allocation", StringType(), True),
    StructField("trustee", StringType(), True),
    StructField("trust_date", StringType(), True),
    StructField("as_of_date", StringType(), True),
])
