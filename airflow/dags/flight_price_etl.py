# airflow/dags/flight_price_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark.utils import create_spark_session
from spark.etl.extract import read_csv_to_spark, stage_to_mysql
from spark.etl.transform import validate_and_clean, compute_kpis
from spark.etl.load import load_transformed_to_postgres, load_kpis_to_postgres
from pyspark import StorageLevel 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

def extract_task():
    """Extract: Read CSV and load to MySQL staging"""
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



def transform_and_load_task():
    spark = create_spark_session()
    try:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
            .option("dbtable", "staging_flight_prices") \
            .option("user", "mysql_user") \
            .option("password", "mysql_pass") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        # Transform
        cleaned_df = validate_and_clean(df)

        # CACHE HERE
        cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Force materialization (VERY IMPORTANT)
        cleaned_df.count()

        # KPIs reuse cleaned_df
        kpis = compute_kpis(cleaned_df)

        # Load cleaned data
        load_transformed_to_postgres(
            cleaned_df,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass",
            table_name="flights_cleaned"
        )

        # Load KPIs
        load_kpis_to_postgres(
            kpis,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass"
        )

        # Always unpersist
        cleaned_df.unpersist()

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
    
    transform_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load_task,
    )

    extract >> transform_load