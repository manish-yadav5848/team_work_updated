from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from wealthcentral.utils.spark_utils import read_from_catalog

def add_delta_op_col(spark: SparkSession, database_name, table_name, df, primary_key):
    # Check if table exists in consumption database.
    #   If yes - perform left join. Where matched delta_op_col is 'I' else 'D'
    #   If no - delta_op_col is 'I'

    if not spark.catalog._jcatalog.tableExists(f"{database_name}.{table_name}"):
        updt_df = df.withColumn('delta_op_ind', lit('I'))
        return updt_df


    # df is computed df from query
    # table_df is the df already present in the consumption database table
    table_df = read_from_catalog(
        spark=spark,
        database_name=database_name,
        table_name=table_name,
    )

    df.createOrReplaceTempView('df1')
    table_df.createOrReplaceTempView('df2')


    select_columns = df.columns
    select_clause = "select\n"

    for column in select_columns:
        select_clause += "df1.{}, \n".format(column)

    select_clause += ",(case when df2.{} is not null then 'I' else 'D' end) as delta_op_col \n"

    join_clause = "from df1 left join df2 on\n"

    for pk in primary_key:
        join_clause += "df1.{} = df2.{} and\n".format(pk, pk)

    query_string = select_clause[:-2] + "\n" + join_clause[:-5]

    # print(query_string)

    # Execute query to add delta_op_col
    updt_df = spark.sql(query_string)

    return updt_df


def trim_string_columns(df: DataFrame) -> DataFrame:
    trimmed_df = df.select([(F.trim(c.name).alias(c.name) if isinstance(c.dataType, StringType) else c.name) for c in df.schema])
    return trimmed_df


def remove_junk_values_from_cols(df):
    # Remove '' and \r\n from columns
    for column_name in df.columns:
        df = df.withColumn(column_name, when(col(column_name) == '', lit(None)).otherwise(col(column_name))) \

    return df


def substitute_null_primary_keys(df, primary_keys):
    for key in primary_keys:
        if ("date" in key) or ("_dt" in key) or ("_dob" in key):
            default = '1900-01-01'
        else:
            default = '-9999'

        df = df.fillna(default, subset=[key])
        df = df.withColumn(key, coalesce(col(key), lit(default)))

        if ("date" in key) or ("_dt" in key) or ("_dob" in key):
            df = df.withColumn(key, to_date(col(key)))

    return df


def drop_duplicates(df, partition_key):
    """
    Drops duplicates over the specified partition key and keeps only the first occurrence of each duplicate.
    Parameters:
        df (DataFrame): The DataFrame to drop duplicates from.
        partition_key (list): A list of column names to partition by.
    Returns:
        DataFrame: The input DataFrame with duplicates over the partition key dropped.
    """
    # Define the window specification
    window_spec = Window.partitionBy(partition_key).orderBy(lit('NONE'))

    # Add a row number to each partition
    df = df.withColumn("row_number", row_number().over(window_spec))

    # Flag the first occurrence of each duplicate for dropping
    df = df \
        .filter(col("row_number") == 1)\
        .drop('row_number')

    return df
