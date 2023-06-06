from spark_init import SparkObject
from ingestion import DataIngestor
import os
import Variable as gav
from validations import get_curr_date
from preprocessing import perform_data_clean_csv  # import the function instead of the class
from preprocessing import perform_data_clean_parquet
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def main() -> None:
    try:
        logger.info("Main function has started...")
        spark_obj = SparkObject(gav.envn, gav.appName)
        spark = spark_obj.get_spark_object()
        data_ingestor = DataIngestor(spark, gav.csv_directory, gav.parquet_directory, gav.processed_files_directory)
        dfs_csv = data_ingestor.ingest_data(gav.csv_directory, "csv")

        dfs_parquet = data_ingestor.ingest_data(gav.parquet_directory, "parquet")

        try:
            for df_csv in dfs_csv:
                logger.info(f"Starting data cleaning for csv DataFrame")
                df_clean_csv = perform_data_clean_csv(spark, df_csv)  # directly call the function here
                print("Displaying cleaned CSV DataFrame:")
                df_clean_csv.show()
                df_clean_csv.printSchema()
                logger.info(f"Data cleaning completed for csv DataFrame")
        except Exception as e:
            logger.error(f"An error occurred during data cleaning for DataFrame: {df_csv}. Error details: {str(e)}")

        try:
            for df_parquet in dfs_parquet:
                logger.info(f"Starting data cleaning for DataFrame: ")
                df_clean_parquet = perform_data_clean_parquet(spark, df_parquet)  # directly call the function here
                print("Displaying cleaned parquet DataFrame:")
                df_clean_parquet.show()
                df_clean_parquet.printSchema()
                logger.info(f"Data cleaning completed for DataFrame: ")
        except Exception as e:
            logger.error(f"An error occurred during data cleaning for DataFrame: {df_parquet}. Error details: {str(e)}")

        logger.info("Main function has completed.")
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    logger.info("Pipeline is Started ...")
    main()
