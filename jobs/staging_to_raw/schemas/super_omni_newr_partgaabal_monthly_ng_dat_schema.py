from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_cycle_date", StringType(), True),
    StructField("fund_number", StringType(), True),
    StructField("all_current_gaa_adjusted_amount", StringType(), True),
    StructField("all_current_gaa_adjusted_surrender_amount", StringType(), True),
    StructField("all_current_gaa_mva_amount", StringType(), True),
    StructField("current_gaa_contract_amount", StringType(), True),
    StructField("plan_end_year", StringType(), True),
    StructField("pm_oltpln_issue_state", StringType(), True),
    StructField("surrender_penalty_amt", StringType(), True)])
