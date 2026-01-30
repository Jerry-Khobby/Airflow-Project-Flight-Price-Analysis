from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark.utils import create_spark_session
from spark.etl.extract import read_csv_to_spark, stage_to_mysql
from spark.etl.transform import validate_and_clean, derive_season, compute_kpis
from spark.etl.load import load_transformed_to_postgres, load_kpis_to_postgres
from pyspark import StorageLevel
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

TRANSFORMED_PARQUET = "/opt/data/tmp/cleaned_flights.parquet"

def extract_task():
    spark = create_spark_session()
    try:
        df = read_csv_to_spark(spark, "/opt/data/Flight_Price_Dataset_of_Bangladesh.csv")
        stage_to_mysql(
            df,
            jdbc_url="jdbc:mysql://mysql:3306/mysql_db",
            user="mysql_user",
            password="mysql_pass",
            table_name="staging_flight_prices"
        )
    finally:
        spark.stop()

def transform_task(**context):
    spark = create_spark_session()
    try:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
            .option("dbtable", "staging_flight_prices") \
            .option("user", "mysql_user") \
            .option("password", "mysql_pass") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        cleaned_df = validate_and_clean(df)
        cleaned_df = derive_season(cleaned_df)
        cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)
        cleaned_df.count()

        os.makedirs(os.path.dirname(TRANSFORMED_PARQUET), exist_ok=True)
        cleaned_df.write.mode("overwrite").parquet(TRANSFORMED_PARQUET)

        context['ti'].xcom_push(key='parquet_path', value=TRANSFORMED_PARQUET)
        cleaned_df.unpersist()
    finally:
        spark.stop()

def load_cleaned_task(**context):
    spark = create_spark_session()
    try:
        parquet_path = context['ti'].xcom_pull(key='parquet_path', task_ids='transform')
        if not parquet_path or not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet not found at {parquet_path}")

        cleaned_df = spark.read.parquet(parquet_path)
        load_transformed_to_postgres(
            cleaned_df,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass",
            table_name="flights_cleaned"
        )
    finally:
        spark.stop()

def compute_and_load_kpis_task(**context):
    spark = create_spark_session()
    try:
        parquet_path = context['ti'].xcom_pull(key='parquet_path', task_ids='transform')
        if not parquet_path or not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet not found at {parquet_path}")

        cleaned_df = spark.read.parquet(parquet_path)
        kpis_df = compute_kpis(cleaned_df)
        load_kpis_to_postgres(
            kpis_df,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass"
        )
    finally:
        spark.stop()
        
        
with DAG(
    'flight_price_etl',
    default_args=default_args,
    description='ETL pipeline for flight price data',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True,
    )

    load_cleaned = PythonOperator(
        task_id='load_cleaned',
        python_callable=load_cleaned_task,
        provide_context=True,
    )

    compute_and_load_kpis = PythonOperator(
        task_id='compute_and_load_kpis',
        python_callable=compute_and_load_kpis_task,
        provide_context=True,
    )

    extract >> transform >> [load_cleaned, compute_and_load_kpis]
