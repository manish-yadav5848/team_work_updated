from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("source_cycle_date", StringType(), True),
    StructField("valuation_date", StringType(), True),
    StructField("action_code", StringType(), True),
    StructField("plan_system_key", StringType(), True),
    StructField("plan_source_sys_name", StringType(), True),
    StructField("participant_agreement_system_key", StringType(), True),
    StructField("participant_agreement_source_system_key", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("div_sub_id", StringType(), True),
    StructField("participant_account_number", StringType(), True),
    StructField("life_insurance_policy_system_key", StringType(), True),
    StructField("life_insurance_source_system_key", StringType(), True),
    StructField("life_insurance_policy_number", StringType(), True),
    StructField("carrier_name", StringType(), True),
    StructField("fund_name", StringType(), True),
    StructField("fund_number", StringType(), True),
    StructField("cash_value_amt", StringType(), True),
    StructField("life_surr_val_amt", StringType(), True),
    StructField("death_ben_val_amt", StringType(), True),
    StructField("error_code", StringType(), True)])
