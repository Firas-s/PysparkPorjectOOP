import logging.config
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lit
import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, when, avg, round, coalesce
from pyspark.sql.window import Window

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def perform_data_clean_csv(spark: SparkSession, df: DataFrame) -> DataFrame:
    df_clean_csv = df.select(df.npi.alias("presc_id"), df.nppes_provider_last_org_name.alias("presc_lname"),
                             df.nppes_provider_first_name.alias("presc_fname"),
                             df.nppes_provider_city.alias("presc_city"),
                             df.nppes_provider_state.alias("presc_state"),
                             df.specialty_description.alias("presc_spclt"), df.years_of_exp,
                             df.drug_name, df.total_claim_count.alias("trx_cnt"), df.total_day_supply,
                             df.total_drug_cost)
    df_clean_csv = df_clean_csv.withColumn("country_name", lit("USA"))
    # 3 Add a Country Field 'USA'
    df_clean_csv = df_clean_csv.withColumn("country_name", lit("USA"))

    # 4 Clean years_of_exp field
    pattern = '\d+'
    idx = 0
    df_clean_csv = df_clean_csv.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))
    # 5 Convert the yearS_of_exp datatype from string to Number
    df_clean_csv = df_clean_csv.withColumn("years_of_exp", col("years_of_exp").cast("int"))

    # 6 Combine First Name and Last Name
    df_clean_csv = df_clean_csv.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
    df_clean_csv = df_clean_csv.drop("presc_fname", "presc_lname")

    # 7 Check and clean all the Null/Nan Values
    # df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()

    # 8 Delete the records where the PRESC_ID is NULL
    df_clean_csv = df_clean_csv.dropna(subset="presc_id")

    # 9 Delete the records where the DRUG_NAME is NULL
    df_clean_csv = df_clean_csv.dropna(subset="drug_name")

    # 10 Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
    spec = Window.partitionBy("presc_id")
    df_clean_csv = df_clean_csv.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
    df_clean_csv = df_clean_csv.withColumn("trx_cnt", col("trx_cnt").cast('integer'))

    # Check and clean all the Null/Nan Values
    df_clean_csv.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_clean_csv.columns])
    df_clean_csv = df_clean_csv.withColumn("current_date", lit(current_date()))

    return df_clean_csv


def perform_data_clean_parquet(spark: SparkSession, df1: DataFrame) -> DataFrame:
    df_clean_parquet = df1.select(upper(df1.city).alias("city"),
                                  df1.state_id,
                                  upper(df1.state_name).alias("state_name"),
                                  upper(df1.county_name).alias("county_name"),
                                  df1.population,
                                  df1.zips)
    df_clean_parquet = df_clean_parquet.withColumn("current_date", lit(current_date()))
    return df_clean_parquet
