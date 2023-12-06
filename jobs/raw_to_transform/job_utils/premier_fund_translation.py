
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    premier_fund_translation_df = spark.sql("SELECT voya_fund_number, premier_fund_number, voya_fund_name,fof.fund_name,fof.asset_cd as asset_code,fof.fixed_fund_ind as fixed_fund_indicator,fof.invst_style as fund_investment_style,fof.mid_fund_name,foa.asset_class_desc,foa.asset_class_type,foa.risk_rtn ,foa.asset_name,  to_date(created_timestamp) as created_timestamp, created_by, source_file_name, as_of_date as source_cycle_date, process_control_id,'IMPALA' as source_system from premier_fund_translation_csv pft left outer join fo_fund_txt fof on fof.fund_number=pft.voya_fund_number left outer join fo_asset_txt foa on fof.asset_cd=foa.asset_code")

    transform_df = premier_fund_translation_df

    return transform_df