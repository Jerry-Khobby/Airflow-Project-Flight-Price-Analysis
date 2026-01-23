# Airflow Project: Flight Price Analysis

## Challenges & Solutions

This project evolved through several architectural and operational iterations. Below is a clear, honest account of the real challenges encountered and how each was resolved.

### 1. Passing Spark DataFrames via XCom (Design Flaw)

**Problem**:
The initial implementation attempted to pass Spark DataFrames directly between Airflow tasks using XCom.

This caused:

* Serialization failures (Spark DataFrames are not JSON-serializable)
* Excessive memory usage
* Tasks hanging or failing unpredictably

**Evidence**:
* Zombie task detection
* Silent task retries
* SIGTERM signals during execution

**Solution**:

* Removed DataFrame passing through XCom entirely
* Redefined task boundaries so each task independently reads from storage
* Restricted XCom usage to lightweight metadata only (row counts, status flags)

---

### 2. Writing Directly from Spark to PostgreSQL Inside Long Airflow Tasks

**Problem**:
Spark transformations and JDBC writes were executed inside a single long-running Airflow task.

This resulted in:

* Missed Airflow heartbeats
* Py4J communication failures
* Zombie jobs detected by the scheduler

**Log Evidence**:

* `Detected zombie job`
* `Task received SIGTERM signal`
* `Py4JNetworkError`

**Solution**:

* Reduced Spark session lifetime per task
* Optimized JDBC writes to complete faster
* Ensured Spark jobs finish well within Airflow heartbeat limits

---

### 3. Overuse of MySQL as an Intermediate Staging Layer

**Problem**:
The initial pipeline relied heavily on MySQL as an intermediate store between every stage.

This caused:

* Unnecessary disk I/O
* Increased pipeline latency
* Tighter coupling between tasks

**Solution**:

* Simplified the architecture
* Retained MySQL only for raw staging
* Let Spark handle transformations in-memory where appropriate

---

### 4. Spark Recomputing Lineage Multiple Times

**Problem**:
Multiple KPI actions were triggered on the same transformed DataFrame, causing Spark to recompute the entire lineage repeatedly.

**Symptoms**:

* Long execution times
* High CPU usage with minimal progress

**Solution**:

* Introduced `cache()` / `persist()` on cleaned DataFrames
* Cached only after validation and cleaning
* Explicitly unpersisted DataFrames after KPI computation

---

### 5. Python Module Import Errors in Airflow Containers

**Problem**:
Airflow failed with:

```
ModuleNotFoundError: No module named 'spark'
```

Root cause:

* Incorrect volume mounts
* Missing `__init__.py` files

**Solution**:

* Mounted `./spark` to `/opt/jobs/spark`
* Added `__init__.py` files
* Verified `PYTHONPATH` inside containers

---

### 7. Container Startup Order and Service Readiness

**Problem**:
Airflow attempted database connections before MySQL/PostgreSQL were ready.

**Solution**:

* Added healthchecks to database containers
* Used `depends_on` with health conditions in Docker Compose

---

### Key Lessons Learned

* Airflow orchestrates; Spark processes
* Never pass large objects via XCom
* Cache strategically in Spark
* Logs drive architecture decisions

---
