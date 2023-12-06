from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("beneficiary_tin", StringType(), True),
    StructField("beneficiary_type", StringType(), True),
    StructField("relationship_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("signed_date", StringType(), True),
    StructField("beneficiary_sequence_number", StringType(), True),
    StructField("primary_contingent_indicator", StringType(), True)])

