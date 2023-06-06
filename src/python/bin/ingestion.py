import logging.config
import os
from pyspark.sql import DataFrame

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

class DataIngestor:
    def __init__(self, spark_session, csv_directory, parquet_directory, processed_files_directory):
        self.spark = spark_session
        self.csv_directory = csv_directory
        self.parquet_directory = parquet_directory
        self.processed_files_directory = processed_files_directory

    def ingest_data(self, directory: str, file_format: str) -> list[DataFrame]:
        try:
            logger.info("ingest_data() has started...")
            dataframes = []
            processed_files_path = os.path.join(self.processed_files_directory, "processed_files")
            processed_files = set()

            if os.path.exists(processed_files_path):
                with open(processed_files_path, 'r') as f:
                    processed_files = set(f.read().splitlines())

            for filename in os.listdir(directory):
                if filename not in processed_files:
                    df = self.spark.read.format(file_format).option("header", "true").load(os.path.join(directory, filename))
                    dataframes.append(df)
                    processed_files.add(filename)

            with open(processed_files_path, 'w') as f:
                f.write("\n".join(processed_files))

            logger.info("ingest_data() has completed.")

            # Print the transformed DataFrames
            for df in dataframes:
                df.show()

            return dataframes
        except Exception as exp:
            logger.error("Error in the method - ingest_data(). " + str(exp), exc_info=True)