# spark/etl/extract.py
import os
from pyspark.sql import SparkSession,DataFrame
from spark.utils import setup_logger, write_df_to_mysql
from spark.spark_schema import SPARK_SCHEMA  

logger = setup_logger("extract", "/opt/airflow/logs/extract.log")

def read_csv_to_spark(spark: SparkSession, file_path: str) -> DataFrame:
    """Read CSV with schema validation"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")
    
    df = spark.read.csv(file_path, header=True, schema=SPARK_SCHEMA)
    initial_count = df.count()
    logger.info(f"Loaded {initial_count} rows from CSV")
    
    # Remove duplicates based on primary key
    pk_cols = ["Airline", "Source", "Destination", "Departure_Datetime", "Booking_Source"]
    df = df.dropDuplicates(pk_cols)
    
    final_count = df.count()
    if final_count < initial_count:
        logger.warning(f"Removed {initial_count - final_count} duplicate rows")
    
    return df




def stage_to_mysql(df: DataFrame, jdbc_url: str, user: str, password: str, table_name: str):
    """
    Stage data to MySQL using OVERWRITE mode for idempotency.
    Each run replaces the staging table completely.
    """
    logger.info(f"Staging {df.count()} rows to MySQL table: {table_name}")
    write_df_to_mysql(df, table_name, jdbc_url, user, password, mode="overwrite")
    logger.info(f"Data staged successfully (mode=overwrite)")