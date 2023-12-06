from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("begin_date", StringType(), True),
    StructField("beneficiary_ssn", StringType(), True),
    StructField("current_life_expectancy", StringType(), True),
    StructField("distribution_due_date", StringType(), True),
    StructField("life_expectancy_calculation_method", StringType(), True),
    StructField("non_applicable_current_year_distributions", StringType(), True),
    StructField("original_life_expectancy", StringType(), True),
    StructField("override_amount", StringType(), True),
    StructField("rmd_amount", StringType(), True),
    StructField("rmd_amount_ytd", StringType(), True),
    StructField("shortfall_amount", StringType(), True),
    StructField("source_group", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True)])
