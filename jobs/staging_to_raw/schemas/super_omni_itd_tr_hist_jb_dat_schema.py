from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("money_source", StringType(), True),
    StructField("last_contribution_date", StringType(), True),
    StructField("last_contribution_amount", StringType(), True)])
