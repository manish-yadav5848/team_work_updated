from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("cycle_time_stamp", StringType(), True),
    StructField("valuation_date", StringType(), True),
    StructField("action_code", StringType(), True),
    StructField("plan_system_key", StringType(), True),
    StructField("plan_source_system_name", StringType(), True),
    StructField("part_aggr_system_key", StringType(), True),
    StructField("part_aggr_src_syst_name", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("filler_1", StringType(), True),
    StructField("part_account_num", StringType(), True),
    StructField("fund_number", StringType(), True),
    StructField("fund_name", StringType(), True),
    StructField("filler_2", StringType(), True),
    StructField("filler_3", StringType(), True),
    StructField("fund_src_system_name", StringType(), True),
    StructField("money_source_code", StringType(), True),
    StructField("filler_4", StringType(), True),
    StructField("money_src_system_name", StringType(), True),
    StructField("cash_value", StringType(), True),
    StructField("number_of_units", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("filler_5", StringType(), True),
    StructField("filler_6", StringType(), True),
    StructField("money_src_ref_key", StringType(), True),
    StructField("pa_money_src_ref_key", StringType(), True)])
