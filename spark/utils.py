import logging
from pyspark.sql import SparkSession, DataFrame
import os

jars_path = "/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar"

def setup_logger(name: str, log_file: str, level=logging.INFO):
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

def create_spark_session(app_name="FlightPriceETL"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")  # Use 2 cores locally
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", jars_path)
        .config("spark.driver.extraClassPath", jars_path)
        .config("spark.executor.extraClassPath", jars_path)
        .config("spark.sql.shuffle.partitions", "4")  # Reduce shuffle partitions
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
    return spark

def write_df_to_mysql(df: DataFrame, table_name: str, jdbc_url: str, user: str, password: str, mode="append"):
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", user) \
      .option("password", password) \
      .option("driver", "com.mysql.cj.jdbc.Driver") \
      .mode(mode) \
      .save()

def write_df_to_postgres(df: DataFrame, table_name: str, jdbc_url: str, user: str, password: str, mode="overwrite"):
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", user) \
      .option("password", password) \
      .option("driver", "org.postgresql.Driver") \
      .mode(mode) \
      .save()