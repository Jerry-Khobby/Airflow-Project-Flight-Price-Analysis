# spark/etl/extract.py
import os
from pyspark.sql import SparkSession
from spark.utils import setup_logger, write_df_to_mysql
from spark.spark_schema import SPARK_SCHEMA  

logger = setup_logger("extract", "/opt/airflow/logs/extract.log")

def read_csv_to_spark(spark: SparkSession, file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found in Docker container")
    df = spark.read.csv(file_path, header=True, schema=SPARK_SCHEMA)
    logger.info(f"Loaded {df.count()} rows from CSV at {file_path}")
    return df

def stage_to_mysql(df, jdbc_url: str, user: str, password: str, table_name: str):
    write_df_to_mysql(df, table_name, jdbc_url, user, password, mode="append")
    logger.info(f"Data staged into MySQL table: {table_name}")