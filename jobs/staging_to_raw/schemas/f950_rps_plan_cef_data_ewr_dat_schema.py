from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("plan_client_id", StringType(), True),
    StructField("plan_sys_key", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("plan_src_system", StringType(), True),
    StructField("plan_comp_code", StringType(), True),
    StructField("plan_manager", StringType(), True),
    StructField("plan_legal_name", StringType(), True),
    StructField("plan_name", StringType(), True),
    StructField("plan_irc_code", StringType(), True),
    StructField("plan_irc_code_text", StringType(), True),
    StructField("plan_product_code", StringType(), True),
    StructField("plan_product_shrt_name", StringType(), True),
    StructField("plan_allocated_flag", StringType(), True),
    StructField("plan_tpa_code", StringType(), True),
    StructField("plan_tpa_full_name", StringType(), True),
    StructField("plan_spon_addr_line_01", StringType(), True),
    StructField("plan_spon_addr_line_02", StringType(), True),
    StructField("plan_spon_addr_city", StringType(), True),
    StructField("plan_spon_addr_state", StringType(), True),
    StructField("plan_spon_addr_zip", StringType(), True),
    StructField("plan_spon_addr_country", StringType(), True),
    StructField("plan_issue_date", StringType(), True),
    StructField("plan_expand_flag", StringType(), True),
    StructField("plan_spon_addr_line_03", StringType(), True),
    StructField("plan_year_end", StringType(), True),
    StructField("loans_allowed", StringType(), True),
    StructField("number_loans_allowed", StringType(), True),
    StructField("ing_send_notices", StringType(), True),
    StructField("hardship_suspension", StringType(), True),
    StructField("dwe_status", StringType(), True),
    StructField("sdbo_status", StringType(), True),
    StructField("employer_stock", StringType(), True)])
