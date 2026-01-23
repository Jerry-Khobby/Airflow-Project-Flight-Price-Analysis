# Airflow Project: Flight Price Analysis

##  Overview

An end-to-end data pipeline using Apache Airflow to process and analyze flight price data for Bangladesh. The pipeline ingests raw CSV data, validates and transforms it, computes key performance indicators (KPIs), and stores results in a PostgreSQL analytics database.

**Data Source**: [Flight Price Dataset of Bangladesh (Kaggle)](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)


##  Architecture

```
CSV File → Airflow ETL Pipeline → MySQL (Staging) → Transformation → PostgreSQL (Analytics)
```

### Pipeline Flow

1. **Extract**: Read CSV data and load into MySQL staging database
2. **Transform**: Validate, clean, and compute KPIs using PySpark
3. **Load**: Store transformed data and KPIs in PostgreSQL analytics database

---

##  Technologies

- **Apache Airflow 2.8.1**: Workflow orchestration
- **Apache Spark 3.5.1**: Distributed data processing
- **MySQL 8.4**: Staging database
- **PostgreSQL 14**: Analytics database
- **Python 3.11**: Data processing and scripting
- **Docker & Docker Compose**: Containerization

---

##  Project Structure

```
project-root/
├── airflow/
│   ├── dags/
│   │   └── flight_price_etl.py       # Main DAG definition
│   ├── logs/                          # Airflow logs
│   └── plugins/                       # Custom Airflow plugins
├── spark/
│   ├── __init__.py
│   ├── utils.py                       # Spark session & utilities
│   ├── spark_schema.py                # Data schema definition
│   └── etl/
│       ├── __init__.py
│       ├── extract.py                 # Data extraction logic
│       ├── transform.py               # Data transformation & KPIs
│       └── load.py                    # Data loading logic
├── sql/
│   ├── init_staging.sql               # MySQL initialization
│   └── init_analytics.sql             # PostgreSQL initialization
├── jars/
│   ├── mysql-connector-j-9.5.0.jar    # MySQL JDBC driver
│   └── postgresql-42.7.6.jar          # PostgreSQL JDBC driver
├── data/
│   └── flight_prices.csv              # Input dataset
├── Dockerfile                          # Airflow + Spark image
├── docker-compose.yml                  # Multi-container setup
├── requirements.txt                    # Python dependencies
├── .env                                # Environment variables
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker Desktop installed
- Docker Compose installed
- Minimum 8GB RAM available for Docker
- 10GB free disk space

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Airflow-Project-Flight-Price-Analysis
   ```

2. **Prepare the data**
   - Download the Flight Price Dataset from Kaggle
   - Place the CSV file in `./data/flight_prices.csv`

3. **Create required directories**
   ```bash
   mkdir -p airflow/dags airflow/logs airflow/plugins data sql jars
   ```

4. **Set environment variables**
   - Review and update `.env` file if needed
   - Ensure `AIRFLOW_FERNET_KEY` is set (already provided)

5. **Build and start services**
   ```bash
   docker-compose build --no-cache
   docker-compose up -d
   ```

6. **Wait for services to be healthy** (2-3 minutes)
   ```bash
   docker-compose ps
   ```

7. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

---

## Data Schema

### Source CSV Columns

| Column | Type | Description |
|--------|------|-------------|
| Airline | String | Airline name |
| Source | String | Departure airport code |
| Source_Name | String | Departure city name |
| Destination | String | Arrival airport code |
| Destination_Name | String | Arrival city name |
| Departure_Datetime | Timestamp | Flight departure time |
| Arrival_Datetime | Timestamp | Flight arrival time |
| Duration_Hrs | Float | Flight duration in hours |
| Stopovers | String | Number/type of stops |
| Aircraft_Type | String | Aircraft model |
| Class | String | Travel class (Economy/Business) |
| Booking_Source | String | Booking platform |
| Base_Fare_BDT | Decimal(10,2) | Base ticket price |
| Tax_Surcharge_BDT | Decimal(10,2) | Taxes and surcharges |
| Total_Fare_BDT | Decimal(10,2) | Total ticket price |
| Seasonality | String | Peak/Off-peak season |
| Days_Before_Departure | Integer | Booking advance time |

---

## Pipeline Components

### 1. Data Ingestion (Extract)

**File**: `spark/etl/extract.py`

- Reads CSV data using PySpark with predefined schema
- Validates file existence
- Loads data into MySQL staging table `staging_flights`
- Handles large datasets efficiently

**Key Functions**:
- `read_csv_to_spark()`: Reads CSV with schema validation
- `stage_to_mysql()`: Writes DataFrame to MySQL using JDBC

### 2. Data Validation & Transformation

**File**: `spark/etl/transform.py`

**Validation Checks**:
-  All required columns present
-  Handle missing/null values with defaults
-  Validate data types (numeric fares, non-empty strings)
-  Correct negative fares (set to 0)
-  Recalculate Total Fare = Base Fare + Tax & Surcharge

**Key Functions**:
- `validate_and_clean()`: Implements all validation rules
- `compute_kpis()`: Calculates performance metrics

### 3. KPI Computation

**Metrics Computed**:

1. **Average Fare by Airline**
   - Calculates mean total fare for each airline
   - Table: `avg_fare_by_airline`

2. **Booking Count by Airline**
   - Counts total bookings per airline
   - Table: `booking_count_by_airline`

3. **Most Popular Routes**
   - Top 10 source-destination pairs by booking count
   - Table: `popular_routes`

4. **Seasonal Fare Variation** (Future Enhancement)
   - Compare peak vs off-peak season fares
   - Define peak seasons (Eid, Winter holidays)

### 4. Data Loading

**File**: `spark/etl/load.py`

- Loads cleaned data to PostgreSQL `flights_cleaned` table
- Loads KPI results to separate analytical tables
- Uses JDBC for efficient bulk transfers
- Ensures data consistency with `overwrite` mode

---

##  Airflow DAG

**File**: `airflow/dags/flight_price_etl.py`

### DAG Configuration

```python
DAG ID: flight_price_etl
Schedule: @daily
Start Date: 2024-01-01
Catchup: False
Retries: 1
Retry Delay: 5 minutes
```

### Tasks

1. **extract_and_stage**
   - Type: PythonOperator
   - Function: `extract_task()`
   - Action: Read CSV → Write to MySQL staging

2. **transform_and_load**
   - Type: PythonOperator
   - Function: `transform_task()`
   - Action: Read MySQL → Transform → Compute KPIs → Write to PostgreSQL
   - Dependencies: `extract_and_stage`

### Task Flow

```
extract_and_stage >> transform_and_load
```

---

##  Database Configuration

### MySQL (Staging Database)

- **Container**: `staging`
- **Host**: `mysql`
- **Port**: `3306`
- **Database**: `mysql_db`
- **User**: `mysql_user`
- **Password**: `mysql_pass`

**Tables**:
- `staging_flights`: Raw data from CSV

### PostgreSQL (Analytics Database)

- **Container**: `analytics`
- **Host**: `postgres_analytics`
- **Port**: `5432` (exposed as `5433` on host)
- **Database**: `psql_db`
- **User**: `psql_user`
- **Password**: `psql_pass`

**Tables**:
- `flights_cleaned`: Validated and transformed flight data
- `avg_fare_by_airline`: Average fare KPI
- `booking_count_by_airline`: Booking count KPI
- `popular_routes`: Popular routes KPI

---

##  Configuration

### Environment Variables

Key variables in `.env`:

```bash
AIRFLOW_FERNET_KEY=<encryption-key>
MYSQL_USER=mysql_user
MYSQL_PASSWORD=mysql_pass
POSTGRES_USER=psql_user
POSTGRES_PASSWORD=psql_pass
```

### JDBC Drivers

Located in `./jars/`:
- `mysql-connector-j-9.5.0.jar`
- `postgresql-42.7.6.jar`

These are automatically loaded by Spark via the utils configuration.

---

##  Usage

### Running the Pipeline

1. **Trigger DAG manually**:
   - Go to Airflow UI → DAGs
   - Find `flight_price_etl`
   - Click the play button ▶️

2. **Monitor execution**:
   - View task logs in real-time
   - Check task duration and status
   - Review error messages if failures occur

3. **Verify results**:
   ```bash
   # Connect to PostgreSQL
   docker exec -it analytics psql -U psql_user -d psql_db
   
   # Query results
   SELECT * FROM flights_cleaned LIMIT 10;
   SELECT * FROM avg_fare_by_airline ORDER BY Avg_Fare_BDT DESC;
   SELECT * FROM popular_routes;
   ```

### Viewing Logs

```bash
# Airflow scheduler logs
docker logs -f airflow-scheduler

# Airflow webserver logs
docker logs -f airflow-server

# Application logs (inside container)
docker exec airflow-scheduler cat /opt/airflow/logs/extract.log
docker exec airflow-scheduler cat /opt/airflow/logs/transform.log
docker exec airflow-scheduler cat /opt/airflow/logs/load.log
```

---

##  Troubleshooting

### Common Issues

1. **Broken DAG Error**
   ```
   ModuleNotFoundError: No module named 'spark'
   ```
   **Solution**: Ensure `./spark:/opt/jobs/spark` volume mount is correct in docker-compose.yml

2. **Database Connection Refused**
   ```
   Connection refused: mysql:3306
   ```
   **Solution**: Wait for healthchecks to pass. Check with `docker-compose ps`

3. **Spark Session Fails**
   ```
   JDBC driver not found
   ```
   **Solution**: Verify JAR files are in `./jars/` directory and mounted correctly

4. **Out of Memory**
   ```
   Container killed (OOMKilled)
   ```
   **Solution**: Increase Docker memory allocation to 8GB minimum

### Debugging Commands

```bash
# Check container status
docker-compose ps

# Restart specific service
docker-compose restart airflow-scheduler

# View all logs
docker-compose logs -f

# Execute commands inside container
docker exec -it airflow-scheduler bash

# Test Python imports
docker exec airflow-scheduler python -c "import spark; print('Success!')"

# Check PYTHONPATH
docker exec airflow-scheduler python -c "import sys; print(sys.path)"
```

---

##  Cleanup

### Stop Services

```bash
docker-compose down
```

### Remove Volumes (Delete all data)

```bash
docker-compose down -v
```

### Remove Everything

```bash
docker-compose down -v
docker system prune -a --volumes
```

---

##  Challenges & Solutions

### Challenge 1: Module Import Errors
**Problem**: `spark` module not found in Airflow DAGs  
**Solution**: Fixed volume mount structure to `/opt/jobs/spark` and ensured `__init__.py` files exist

### Challenge 2: JDBC Driver Loading
**Problem**: Spark couldn't connect to databases  
**Solution**: Pre-configured JAR paths in `utils.py` and mounted `/opt/jars` volume

### Challenge 3: Container Orchestration
**Problem**: Airflow services starting before databases were ready  
**Solution**: Implemented healthchecks and service dependencies in docker-compose.yml

### Challenge 4: Data Type Handling
**Problem**: Decimal precision loss during transformations  
**Solution**: Used `DecimalType(10,2)` in schema and proper casting in transformations

---

##  Future Enhancements

1. **Seasonal Analysis**
   - Implement peak season detection algorithm
   - Add seasonal fare comparison metrics

2. **Data Quality Monitoring**
   - Add Great Expectations for data validation
   - Create data quality dashboards

3. **Performance Optimization**
   - Implement incremental loading
   - Add data partitioning strategies

4. **Visualization**
   - Integrate Apache Superset or Metabase
   - Create interactive dashboards

5. **Alerting & Monitoring**
   - Set up Airflow email alerts
   - Add Prometheus + Grafana monitoring

---
## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---