## Analytics Database Queries (PostgreSQL)

This section contains SQL queries used to validate that Spark successfully cleaned, transformed, and loaded analytical data into the PostgreSQL analytics database.

---

### Analytics Database Service

The analytics database is defined in Docker Compose as:

```yaml
postgres_analytics:
  container_name: analytics
  ports:
    - "5433:5432"
````

---

### Connect to PostgreSQL Analytics Database

```bash
psql -h localhost -p 5433 -U psql_user -d psql_db
```

**Password**: `psql_pass`

---

## 1 Verify Cleaned Data Was Loaded

```sql
SELECT COUNT(*) 
FROM flights_cleaned;
```

```sql
SELECT *
FROM flights_cleaned
LIMIT 10;
```

✔ Confirms that Spark cleaning and validation logic executed successfully
✔ Confirms data was written to PostgreSQL

---

##  Average Fare by Airline (Spark KPI Output)

```sql
SELECT *
FROM avg_fare_by_airline
ORDER BY Avg_Fare_BDT DESC;
```

This verifies that:

* Spark computed the KPI
* Spark wrote the result to PostgreSQL
* PostgreSQL now serves analytical results

---

## Booking Count by Airline

```sql
SELECT *
FROM booking_count_by_airline
ORDER BY Booking_Count DESC;
```

---

## Most Popular Routes

```sql
SELECT *
FROM popular_routes
ORDER BY Booking_Count DESC;
```

---

##  Seasonal Fare Variation

```sql
SELECT *
FROM seasonal_fare_variation
ORDER BY Avg_Fare_BDT DESC;
```

---

##  Cross-Check: Spark vs PostgreSQL Calculation

Recompute the average fare directly in PostgreSQL to validate Spark results:

```sql
SELECT Airline,
       ROUND(AVG(Total_Fare_BDT), 2) AS avg_fare
FROM flights_cleaned
GROUP BY Airline
ORDER BY avg_fare DESC;
```

✔ The result should match the `avg_fare_by_airline` table
✔ Confirms correctness of Spark transformations

````

# airflow/dags/flight_price_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark.utils import create_spark_session
from spark.etl.extract import read_csv_to_spark, stage_to_mysql
from spark.etl.transform import validate_and_clean, compute_kpis,derive_season
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
        cleaned_df = derive_season(cleaned_df)

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