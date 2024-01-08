from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("cust_adv_program", StringType(), True),
    StructField("cust_adv_program_name", StringType(), True)])
