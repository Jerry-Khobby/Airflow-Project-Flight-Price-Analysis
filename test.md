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

