# airflow/dags/flight_price_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark.utils import create_spark_session
from spark.etl.extract import read_csv_to_spark, stage_to_mysql
from spark.etl.transform import validate_and_clean, compute_kpis
from spark.etl.load import load_transformed_to_postgres, load_kpis_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
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

def transform_task():
    """Transform: Read from MySQL, clean, compute KPIs, save back to MySQL"""
    spark = create_spark_session()
    try:
        # Read from MySQL staging
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
            .option("dbtable", "staging_flight_prices") \
            .option("user", "mysql_user") \
            .option("password", "mysql_pass") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        # Transform and clean
        cleaned_df = validate_and_clean(df)
        kpis = compute_kpis(cleaned_df)
        
        # Save transformed data back to MySQL (intermediate storage)
        from spark.utils import write_df_to_mysql
        write_df_to_mysql(
            cleaned_df, 
            "transformed_flight_prices", 
            "jdbc:mysql://mysql:3306/mysql_db",
            "mysql_user", 
            "mysql_pass", 
            mode="overwrite"
        )
        
        # Save KPIs to MySQL (intermediate storage)
        for kpi_name, kpi_df in kpis.items():
            write_df_to_mysql(
                kpi_df, 
                f"kpi_{kpi_name}", 
                "jdbc:mysql://mysql:3306/mysql_db",
                "mysql_user", 
                "mysql_pass", 
                mode="overwrite"
            )
    finally:
        spark.stop()

def load_task():
    """Load: Read transformed data from MySQL and load to PostgreSQL"""
    spark = create_spark_session()
    try:
        # Read transformed data from MySQL
        cleaned_df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
            .option("dbtable", "transformed_flight_prices") \
            .option("user", "mysql_user") \
            .option("password", "mysql_pass") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        # Load cleaned data to PostgreSQL
        load_transformed_to_postgres(
            cleaned_df,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass",
            table_name="flights_cleaned"
        )
        
        # Read and load KPIs to PostgreSQL
        kpi_names = ["avg_fare_by_airline", "booking_count_by_airline", "popular_routes"]
        kpis = {}
        
        for kpi_name in kpi_names:
            kpi_df = spark.read.format("jdbc") \
                .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
                .option("dbtable", f"kpi_{kpi_name}") \
                .option("user", "mysql_user") \
                .option("password", "mysql_pass") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            kpis[kpi_name] = kpi_df
        
        # Load KPIs to PostgreSQL
        load_kpis_to_postgres(
            kpis,
            jdbc_url="jdbc:postgresql://postgres_analytics:5432/psql_db",
            user="psql_user",
            password="psql_pass"
        )
    finally:
        spark.stop()

# DAG Definition
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
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )

    # Task dependencies
    extract >> transform >> load