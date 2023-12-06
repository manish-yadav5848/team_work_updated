from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("vesting_data_source", StringType(), True),
    StructField("so_plan_service_info_tpa_num", StringType(), True),
    StructField("so_product_information_prod_tp", StringType(), True),
    StructField("so_plan_name1", StringType(), True),
    StructField("so_plan_name2", StringType(), True),
    StructField("so_plan_service_info_mkt_ind", StringType(), True),
    StructField("acp_irc_section", StringType(), True),
    StructField("uses_eligibility_module", StringType(), True),
    StructField("uses_loan_sweep_module", StringType(), True),
    StructField("uses_contribution_rate_escalator", StringType(), True)])
