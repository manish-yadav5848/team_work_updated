
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    slx_tpa_common_daily_dat_df = spark.sql("SELECT tpa_code, billing_address_street_1 AS tpa_address_line_1, trim(name) AS plan_tpa_full_name,billing_address_postal_code AS tpa_zip_code,phone AS tpa_phone,fax AS tpa_fax,tpa_tin,billing_address_street_2 AS tpa_address_line_2,billing_address_city AS tpa_address_city,billing_address_state AS tpa_address_state, CONCAT(billing_address_street_1,billing_address_street_2,billing_address_city,billing_address_state,billing_address_postal_code) AS ds_tpa_address,'SALESFORCE' as source_system FROM slx_tpa_common_daily_dat")

    transform_df = slx_tpa_common_daily_dat_df

    return transform_df
	