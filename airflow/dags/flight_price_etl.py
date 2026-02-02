from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark.utils import create_spark_session, read_from_mysql
from spark.etl.extract import read_csv_to_spark, stage_to_mysql
from spark.etl.transform import (
    validate_and_clean,
    derive_season,
    write_transformed_to_mysql,
    compute_avg_fare_by_airline,
    compute_booking_count_by_airline,
    compute_popular_routes,
    compute_seasonal_fare_variation
)
from spark.etl.load import load_to_postgres, load_single_kpi

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

# Configuration
CSV_FILE = "/opt/data/Flight_Price_Dataset_of_Bangladesh.csv"

MYSQL_CONFIG = {
    'jdbc_url': "jdbc:mysql://mysql:3306/mysql_db",
    'user': "mysql_user",
    'password': "mysql_pass",
}

POSTGRES_CONFIG = {
    'jdbc_url': "jdbc:postgresql://postgres_analytics:5432/psql_db",
    'user': "psql_user",
    'password': "psql_pass",
}


#CSV to MySQL Staging
def extract_task():
    """
    Extract: Read CSV and stage to MySQL
    - Reads CSV with schema validation
    - Removes duplicates
    - Writes to staging_raw (OVERWRITE mode for idempotency)
    """
    spark = create_spark_session()
    try:
        # Read and deduplicate CSV
        df = read_csv_to_spark(spark, CSV_FILE)
        
        # Stage to MySQL (OVERWRITE mode)
        stage_to_mysql(
            df,
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            table_name="staging_raw"
        )
    finally:
        spark.stop()

# Validate, Clean, and Store in MySQL
def transform_task():
    """
    Transform: Read from MySQL, validate, clean, and write back to MySQL
    - Reads from staging_raw
    - Applies comprehensive validation
    - Derives features
    - Writes to staging_cleaned (OVERWRITE mode for idempotency)
    """
    spark = create_spark_session()
    try:
        # Read raw data from MySQL
        df = read_from_mysql(
            spark,
            table_name="staging_raw",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Validate and clean
        cleaned_df = validate_and_clean(df)
        
        # Derive season
        cleaned_df = derive_season(cleaned_df)
        
        # Write cleaned data back to MySQL (OVERWRITE mode)
        write_transformed_to_mysql(
            cleaned_df,
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            table_name="staging_cleaned"
        )
    finally:
        spark.stop()


# MySQL to PostgreSQL
def load_main_table_task():
    """
    Load: Read cleaned data from MySQL and load to PostgreSQL
    - Reads from staging_cleaned
    - Writes to flights_cleaned (OVERWRITE mode for idempotency)
    """
    spark = create_spark_session()
    try:
        # Read cleaned data from MySQL
        cleaned_df = read_from_mysql(
            spark,
            table_name="staging_cleaned",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Load to PostgreSQL
        load_to_postgres(
            cleaned_df,
            jdbc_url=POSTGRES_CONFIG['jdbc_url'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'],
            table_name="flights_cleaned"
        )
    finally:
        spark.stop()

# Average Fare by Airline
def compute_avg_fare_task():
    """Compute and load: Average Fare by Airline KPI"""
    spark = create_spark_session()
    try:
        # Read cleaned data from MySQL
        cleaned_df = read_from_mysql(
            spark,
            table_name="staging_cleaned",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Compute KPI
        kpi_df = compute_avg_fare_by_airline(cleaned_df)
        
        # Load to PostgreSQL
        load_single_kpi(
            kpi_df,
            kpi_name="avg_fare_by_airline",
            jdbc_url=POSTGRES_CONFIG['jdbc_url'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
    finally:
        spark.stop()

#Booking Count by Airline
def compute_booking_count_task():
    """Compute and load: Booking Count by Airline KPI"""
    spark = create_spark_session()
    try:
        # Read cleaned data from MySQL
        cleaned_df = read_from_mysql(
            spark,
            table_name="staging_cleaned",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Compute KPI
        kpi_df = compute_booking_count_by_airline(cleaned_df)
        
        # Load to PostgreSQL
        load_single_kpi(
            kpi_df,
            kpi_name="booking_count_by_airline",
            jdbc_url=POSTGRES_CONFIG['jdbc_url'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
    finally:
        spark.stop()


# Popular Routes
def compute_popular_routes_task():
    """Compute and load: Top 10 Popular Routes KPI"""
    spark = create_spark_session()
    try:
        # Read cleaned data from MySQL
        cleaned_df = read_from_mysql(
            spark,
            table_name="staging_cleaned",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Compute KPI
        kpi_df = compute_popular_routes(cleaned_df)
        
        # Load to PostgreSQL
        load_single_kpi(
            kpi_df,
            kpi_name="popular_routes",
            jdbc_url=POSTGRES_CONFIG['jdbc_url'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
    finally:
        spark.stop()

# Seasonal Fare Variation
def compute_seasonal_fares_task():
    """Compute and load: Seasonal Fare Variation KPI"""
    spark = create_spark_session()
    try:
        # Read cleaned data from MySQL
        cleaned_df = read_from_mysql(
            spark,
            table_name="staging_cleaned",
            jdbc_url=MYSQL_CONFIG['jdbc_url'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        
        # Compute KPI
        kpi_df = compute_seasonal_fare_variation(cleaned_df)
        
        # Load to PostgreSQL
        load_single_kpi(
            kpi_df,
            kpi_name="seasonal_fare_variation",
            jdbc_url=POSTGRES_CONFIG['jdbc_url'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
    finally:
        spark.stop()



# DAG DEFINITION WITH PARALLEL KPI TASKS
with DAG(
    'flight_price_etl_parallel_kpis',
    default_args=default_args,
    description='Production ETL with Parallel KPI Computation',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl', 'parallel-kpis']
) as dag:

    # Stage 1: Extract
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    # Stage 2: Transform
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    # Stage 3: Load Main Table
    load_main = PythonOperator(
        task_id='load_main_table',
        python_callable=load_main_table_task,
    )

    # Stage 4: Compute and Load KPIs (all run in parallel)
    kpi_avg_fare = PythonOperator(
        task_id='kpi_avg_fare_by_airline',
        python_callable=compute_avg_fare_task,
    )

    kpi_booking_count = PythonOperator(
        task_id='kpi_booking_count_by_airline',
        python_callable=compute_booking_count_task,
    )

    kpi_popular_routes = PythonOperator(
        task_id='kpi_popular_routes',
        python_callable=compute_popular_routes_task,
    )

    kpi_seasonal_fares = PythonOperator(
        task_id='kpi_seasonal_fare_variation',
        python_callable=compute_seasonal_fares_task,
    )

    # Task dependencies
    # Linear: extract → transform → load_main
    # Then all KPIs run in parallel after transform completes
    extract >> transform >> load_main
    transform >> [kpi_avg_fare, kpi_booking_count, kpi_popular_routes, kpi_seasonal_fares]