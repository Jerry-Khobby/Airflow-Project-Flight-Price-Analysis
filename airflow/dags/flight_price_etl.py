from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from spark.utils import create_spark_session
from spark.extract import read_csv_to_spark, stage_to_mysql
from spark.transform import validate_and_clean, compute_kpis
from spark.load import load_transformed_to_postgres, load_kpis_to_postgres

# Airflow environment connections
MYSQL_CONN = os.environ["AIRFLOW_CONN_MYSQL_STAGING"] 
POSTGRES_CONN = os.environ["AIRFLOW_CONN_POSTGRES_ANALYTICS"]  

# Paths inside Docker
CSV_PATH = "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv"

def extract_task():
    spark = create_spark_session("ExtractFlightData")
    
    df = read_csv_to_spark(spark, CSV_PATH)
    
    # Parse MySQL connection
    import re
    m = re.match(r"mysql://(.*):(.*)@(.*):(\d+)/(.*)", MYSQL_CONN)
    user, password, host, port, database = m.groups()
    jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
    
    stage_to_mysql(df, jdbc_url, user, password, "staging_flight_prices")
    spark.stop()

def transform_task():
    spark = create_spark_session("TransformFlightData")

    # MySQL connection parsing
    import re
    m = re.match(r"mysql://(.*):(.*)@(.*):(\d+)/(.*)", MYSQL_CONN)
    user, password, host, port, database = m.groups()
    jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"

    # Pull from staging
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "staging_flight_prices") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    
    df_clean = validate_and_clean(df)
    kpis = compute_kpis(df_clean)

    # Save cleaned DataFrame & KPIs temporarily to XCom for loading
    df_clean.createOrReplaceTempView("df_clean")
    spark.sql("CACHE TABLE df_clean")
    kpis_cache = {name: df for name, df in kpis.items()}
    spark.stop()
    return kpis_cache

def load_task(ti):
    spark = create_spark_session("LoadFlightData")
    kpis_cache = ti.xcom_pull(task_ids='transform_task')
    
    # PostgreSQL connection parsing
    import re
    m = re.match(r"postgres://(.*):(.*)@(.*):(\d+)/(.*)", POSTGRES_CONN)
    user, password, host, port, database = m.groups()
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    # Load transformed data from staging again
    df_clean = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{host}:{port}/flight_staging") \
        .option("dbtable", "staging_flight_prices") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    
    load_transformed_to_postgres(df_clean, jdbc_url, user, password, "flight_analytics")
    
    # Load KPIs
    for name, df in kpis_cache.items():
        load_kpis_to_postgres({name: df}, jdbc_url, user, password)
    
    spark.stop()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 20),
    "retries": 1
}

with DAG(
    "flight_price_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task
    )

    extract >> transform >> load
