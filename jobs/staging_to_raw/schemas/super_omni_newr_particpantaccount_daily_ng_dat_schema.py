from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("enrollment_date", StringType(), True),
    StructField("free_look_end_date", StringType(), True),
    StructField("auto_enrollment_indicator", StringType(), True),
    StructField("hours_of_service", StringType(), True),
    StructField("participant_account_company_code", StringType(), True),
    StructField("participant_account_type_code", StringType(), True),
    StructField("participant_account_type_description", StringType(), True),
    StructField("participant_catchup_eligible", StringType(), True),
    StructField("participant_name_raw", StringType(), True),
    StructField("participant_address_line_1", StringType(), True),
    StructField("participant_address_line_2", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("alternate_account_number", StringType(), True)])
