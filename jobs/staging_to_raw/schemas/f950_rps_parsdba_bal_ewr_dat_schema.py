from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("cycle_date", StringType(), True),
    StructField("valm_date", StringType(), True),
    StructField("act_code", StringType(), True),
    StructField("plan_sys_key", StringType(), True),
    StructField("plan_sys_name", StringType(), True),
    StructField("part_agg_sys_key", StringType(), True),
    StructField("part_agg_sys_nam", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("sub_plan_id", StringType(), True),
    StructField("part_acc_num", StringType(), True),
    StructField("fund_num", StringType(), True),
    StructField("fund_name", StringType(), True),
    StructField("fund_mar_code", StringType(), True),
    StructField("fund_sep_acc", StringType(), True),
    StructField("fund_sor_sys_name", StringType(), True),
    StructField("money_sor_code", StringType(), True),
    StructField("money_sor_name", StringType(), True),
    StructField("money_sor_sys_name", StringType(), True),
    StructField("cash_value", StringType(), True),
    StructField("num_of_units", StringType(), True),
    StructField("unit_priceo", StringType(), True),
    StructField("vested_bal", StringType(), True),
    StructField("vested_per", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("ytd_contributions", StringType(), True)])
