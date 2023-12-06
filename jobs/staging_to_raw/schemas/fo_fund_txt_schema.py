from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("fund_number", StringType(), True),
    StructField("mktg_fund_number", StringType(), True),
    StructField("fund_abbr_name", StringType(), True),
    StructField("fund_name", StringType(), True),
    StructField("incept_dt", StringType(), True),
    StructField("div_payfreq_cd", StringType(), True),
    StructField("fam_abbr_name", StringType(), True),
    StructField("acc_fund_ind", StringType(), True),
    StructField("perf_ind", StringType(), True),
    StructField("fixed_fund_ind", StringType(), True),
    StructField("incept_ind", StringType(), True),
    StructField("outsd_fund_num", StringType(), True),
    StructField("auv_move_dt", StringType(), True),
    StructField("advr_fee_d_fct", StringType(), True),
    StructField("advr_fee_rate", StringType(), True),
    StructField("trd_inst_ffam", StringType(), True),
    StructField("aet_ser_p_ind", StringType(), True),
    StructField("avail_prod_dt", StringType(), True),
    StructField("s_d_div_amt_lo", StringType(), True),
    StructField("s_d_div_amt_hi", StringType(), True),
    StructField("m_d_div_amt_lo", StringType(), True),
    StructField("m_d_div_amt_hi", StringType(), True),
    StructField("m_d_div_day", StringType(), True),
    StructField("stk_fund_ind", StringType(), True),
    StructField("sadv_fee_d_fct", StringType(), True),
    StructField("sadv_fee_rate", StringType(), True),
    StructField("exped_fund_ind", StringType(), True),
    StructField("fixed_fund_typ", StringType(), True),
    StructField("risk_rtn", StringType(), True),
    StructField("asset_cd", StringType(), True),
    StructField("index_id", StringType(), True),
    StructField("adj_price_ind", StringType(), True),
    StructField("adm_fee_d_fct", StringType(), True),
    StructField("adm_fee_rate", StringType(), True),
    StructField("yb04_d_fct", StringType(), True),
    StructField("yb04_rate", StringType(), True),
    StructField("at_file_ind", StringType(), True),
    StructField("prt_fee_rate", StringType(), True),
    StructField("prt_fee_fct", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("mid_fund_name", StringType(), True),
    StructField("adm_fund_num", StringType(), True),
    StructField("invst_style", StringType(), True)])
