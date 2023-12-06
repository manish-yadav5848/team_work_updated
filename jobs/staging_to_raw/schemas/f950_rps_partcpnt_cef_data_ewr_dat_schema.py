from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("part_client_id", StringType(), True),
    StructField("part_plan_number", StringType(), True),
    StructField("part_sys_key", StringType(), True),
    StructField("part_agg_sys_key", StringType(), True),
    StructField("part_account_num", StringType(), True),
    StructField("part_acc_comp_code", StringType(), True),
    StructField("part_src_system", StringType(), True),
    StructField("part_acc_prod_code", StringType(), True),
    StructField("part_acc_prod_shrt_nm", StringType(), True),
    StructField("part_acc_typ_code", StringType(), True),
    StructField("part_acc_type", StringType(), True),
    StructField("part_acc_plan_code", StringType(), True),
    StructField("part_acc_stat_code", StringType(), True),
    StructField("part_account_status", StringType(), True),
    StructField("part_acc_stat_date", StringType(), True),
    StructField("part_type", StringType(), True),
    StructField("part_acc_plan_entry_date", StringType(), True),
    StructField("part_empl_termin_date", StringType(), True),
    StructField("part_hire_date", StringType(), True),
    StructField("part_tin", StringType(), True),
    StructField("part_tin_last4", StringType(), True),
    StructField("part_date_of_birth", StringType(), True),
    StructField("part_current_age", StringType(), True),
    StructField("part_gender_code", StringType(), True),
    StructField("part_gender", StringType(), True),
    StructField("part_person_flag", StringType(), True),
    StructField("part_full_name", StringType(), True),
    StructField("part_first_name", StringType(), True),
    StructField("part_last_name", StringType(), True),
    StructField("part_middle_name", StringType(), True),
    StructField("part_suffix_title", StringType(), True),
    StructField("part_addr_line_01", StringType(), True),
    StructField("part_addr_line_02", StringType(), True),
    StructField("part_addr_line_03", StringType(), True),
    StructField("part_addr_city", StringType(), True),
    StructField("part_addr_state", StringType(), True),
    StructField("part_addr_zip", StringType(), True),
    StructField("part_addr_country", StringType(), True),
    StructField("part_lob", StringType(), True),
    StructField("part_roth_year", StringType(), True),
    StructField("part_ytd_ee_contrib", StringType(), True),
    StructField("part_ytd_er_contrib", StringType(), True),
    StructField("part_post88_surender", StringType(), True),
    StructField("part_payrole_location", StringType(), True),
    StructField("part_auto_enrolled", StringType(), True),
    StructField("default_enrolled_qdia", StringType(), True),
    StructField("part_hard_suspend_date", StringType(), True),
    StructField("part_email", StringType(), True),
    StructField("part_phone", StringType(), True),
    StructField("part_years_of_service", StringType(), True),
    StructField("part_ytd_hours_worked", StringType(), True),
    StructField("part_location_code", StringType(), True),
    StructField("part_rehire_date", StringType(), True),
    StructField("part_employee_id", StringType(), True),
    StructField("part_catchup_eligible", StringType(), True),
    StructField("part_marital_status", StringType(), True),
    StructField("part_hce_status", StringType(), True),
    StructField("part_16b_status", StringType(), True),
    StructField("part_qdro_status", StringType(), True),
    StructField("part_installment_status", StringType(), True)])