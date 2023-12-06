from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("source_id", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("fund_id", StringType(), True),
    StructField("fund_name", StringType(), True),
    StructField("fund_ticker", StringType(), True),
    StructField("fund_cusip", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("as_of_date", StringType(), True)])
