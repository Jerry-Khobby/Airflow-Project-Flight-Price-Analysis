#  Airflow Project: Flight Price Analytics Pipeline

##  Overview

This project implements a **production-style end-to-end data engineering pipeline** using Apache Airflow and Apache Spark to process and analyze flight price data for Bangladesh.

The pipeline follows a **multi-layer architecture (Bronze → Silver → Analytics)** with proper staging, transformation, and parallel KPI computation.

**Data Source:**
Flight Price Dataset of Bangladesh (Kaggle)

---

# Architecture

## High-Level Architecture

```
CSV File
   ↓
Spark (Extract)
   ↓
MySQL → staging_raw  (Bronze Layer)
   ↓
Spark (Transform & Clean)
   ↓
MySQL → staging_cleaned (Silver Layer)
   ↓
Spark (Load + KPIs)
   ↓
PostgreSQL (Analytics Layer)
```

---

## Architectural Principles

###  Layered Data Architecture

| Layer     | Database   | Table           | Purpose                  |
| --------- | ---------- | --------------- | ------------------------ |
| Bronze    | MySQL      | staging_raw     | Raw ingested data        |
| Silver    | MySQL      | staging_cleaned | Cleaned & validated data |
| Analytics | PostgreSQL | flights_cleaned | BI-ready data            |
| Analytics | PostgreSQL | KPI tables      | Aggregated metrics       |

---

###  Idempotent Pipeline Design

All Spark writes use:

```
mode="overwrite"
```

This guarantees:

* No duplicate accumulation
* Safe re-runs
* Deterministic outputs
* Production-grade behavior

You can safely run:

```bash
docker-compose down -v
docker-compose up -d
```

And the results will remain consistent.

---

# Technologies Used

* Apache Airflow 2.8.1 – Workflow orchestration
* Apache Spark 3.5.1 – Distributed data processing
* MySQL 8.4 – Staging database (Bronze & Silver layers)
* PostgreSQL 14 – Analytics database
* Python 3.11 – ETL logic
* Docker & Docker Compose – Container orchestration

---

# Project Structure

```
project-root/
│
├── airflow/
│   ├── dags/
│   │   └── flight_price_etl.py
│   ├── logs/
│   └── plugins/
│
├── spark/
│   ├── utils.py
│   ├── spark_schema.py
│   └── etl/
│       ├── extract.py
│       ├── transform.py
│       └── load.py
│
├── sql/
│   ├── init_staging.sql
│   └── init_analytics.sql
│
├── jars/
│   ├── mysql-connector-j-9.5.0.jar
│   └── postgresql-42.7.6.jar
│
├── data/
│   └── flight_prices.csv
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

#  Detailed Pipeline Flow

##  Extract Stage

**File:** `spark/etl/extract.py`

* Reads CSV using predefined Spark schema
* Validates file existence
* Deduplicates data
* Writes to:

```
MySQL → staging_raw
```

This creates the **Bronze layer**.

---

##  Transform Stage

**File:** `spark/etl/transform.py`

Spark reads:

```
MySQL → staging_raw
```

Applies:

* Null handling
* Data type validation
* Negative fare correction
* Recalculation of total fare
* Stopover normalization
* Derived feature: `Derived_Season`

Writes cleaned output to:

```
MySQL → staging_cleaned
```

This creates the **Silver layer**.

---

## Load Main Analytics Table

Spark reads:

```
MySQL → staging_cleaned
```

Writes to:

```
PostgreSQL → flights_cleaned
```

This is the main BI-ready table.

---

## Parallel KPI Computation

After transformation completes, Spark computes KPIs in **parallel Airflow tasks**:

* `avg_fare_by_airline`
* `booking_count_by_airline`
* `popular_routes`
* `seasonal_fare_variation`

Each KPI:

1. Reads from `staging_cleaned`
2. Aggregates using Spark
3. Writes results to PostgreSQL

Parallelism improves:

* DAG performance
* Scalability
* Real-world production realism

---

# Airflow DAG Design

### DAG ID

```
flight_price_etl_parallel_kpis
```

### Schedule

```
@daily
```

### Task Structure

```
extract
   ↓
transform
   ↓
load_main_table

transform
   ↓
[kpi_avg_fare,
 kpi_booking_count,
 kpi_popular_routes,
 kpi_seasonal_fare_variation]
```

Key Notes:

* `transform` is the central dependency
* KPIs run in parallel
* `load_main_table` runs independently after transform
* Each task initializes its own Spark session

---

#  Database Configuration

## MySQL – Staging Layer

* Host: `mysql`
* Port: `3306`
* Database: `mysql_db`

Tables:

* `staging_raw`
* `staging_cleaned`

Purpose:

* Raw persistence
* Data validation checkpoint
* Debugging support

---

## PostgreSQL – Analytics Layer

* Host: `postgres_analytics`
* Port: `5432`

Tables:

* `flights_cleaned`
* `avg_fare_by_airline`
* `booking_count_by_airline`
* `popular_routes`
* `seasonal_fare_variation`

Purpose:

* Analytical queries
* BI reporting
* Aggregated insights

---

# Getting Started

## Prerequisites

* Docker Desktop
* 8GB RAM minimum
* 10GB free disk space

---

## Setup

###  Clone Repository

```bash
git clone <repository-url>
cd Airflow-Project-Flight-Price-Analysis
```

---

###  Add Dataset

Place:

```
flight_prices.csv
```

Inside:

```
/data
```

---

###  Start Services

```bash
docker-compose build --no-cache
docker-compose up -d
```

Wait 2–3 minutes for all services to become healthy:

```bash
docker-compose ps
```

---

###  Access Airflow

URL:

```
http://localhost:8080
```

Username: `admin`
Password: `admin`

---

# Verifying Results

Connect to PostgreSQL:

```bash
docker exec -it analytics psql -U psql_user -d psql_db
```

Example queries:

```sql
SELECT COUNT(*) FROM flights_cleaned;

SELECT * 
FROM avg_fare_by_airline 
ORDER BY "Avg_Fare_BDT" DESC;

SELECT * FROM popular_routes;
```

---

# 🛠 Troubleshooting

### Spark JDBC Driver Error

Ensure JAR files exist in:

```
./jars/
```

And are mounted correctly.

---

### Database Not Ready

Run:

```bash
docker-compose ps
```

Wait for healthchecks to pass.

---

### Module Import Error

Ensure:

* `__init__.py` files exist
* Proper volume mounts are configured

---

#  Cleanup

Stop services:

```bash
docker-compose down
```

Remove volumes:

```bash
docker-compose down -v
```

---

#  Future Enhancements

* Incremental loading
* Data partitioning
* Great Expectations integration
* BI dashboard (Superset / Metabase)
* Monitoring with Prometheus & Grafana
* Data quality alerting

---

# Architecture Diagram

Include:

```
airflow.drawio.png
```
