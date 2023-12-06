from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
schema = StructType([
StructField("client_id",StringType(),True),
StructField("plan_number",StringType(),True),
StructField("source_system",StringType(),True),
StructField("source_cycle_date",StringType(),True),
StructField("distribution_fee",StringType(),True),
StructField("withdrawal_code",StringType(),True),
StructField("withdrawal_description",StringType(),True),
StructField("withdrawal_type",StringType(),True)])