
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    transaction_type_df = spark.sql("select base_transaction_code, activity_type ,activity_code_description, usage_code_1, usage_code_2, usage_code_3, usage_code_4, usage_code_5, standard_usage_code_1, standard_usage_code_2, standard_usage_code_3, standard_usage_code_4,  standard_usage_code_5, category, activity, process_flag,last_contribution from transaction_type_csv")

    transform_df = transaction_type_df

    return transform_df