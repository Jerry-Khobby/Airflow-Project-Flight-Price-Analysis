# spark/etl/transform.py
from pyspark.sql import DataFrame, functions as F
from spark.utils import setup_logger

logger = setup_logger("transform", "/opt/airflow/logs/transform.log")

STRING_COLS = [
    "Airline", "Source", "Source_Name",
    "Destination", "Destination_Name",
    "Stopovers", "Aircraft_Type",
    "Class", "Booking_Source"
]

NUMERIC_COLS = [
    "Base_Fare_BDT",
    "Tax_Surcharge_BDT",
    "Total_Fare_BDT",
    "Duration_Hrs",
    "Days_Before_Departure"
]

def validate_and_clean(df: DataFrame) -> DataFrame:
    # 1. Empty string validation
    for col in STRING_COLS:
        df = df.withColumn(
            col,
            F.when(F.trim(F.col(col)) == "", "Unknown").otherwise(F.col(col))
        )

    # 2. Explicit numeric validation
    # (NULL = invalid cast from CSV)
    for col in NUMERIC_COLS:
        invalid_count = df.filter(F.col(col).isNull()).count()
        if invalid_count > 0:
            logger.warning(f"{invalid_count} invalid numeric values found in {col}")

        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

    # 3. Inconsistency correction
    for col in ["Base_Fare_BDT", "Tax_Surcharge_BDT", "Total_Fare_BDT", "Duration_Hrs"]:
        df = df.withColumn(
            col,
            F.when(F.col(col) < 0, 0).otherwise(F.col(col))
        )

    # 4. Recalculate Total Fare
    df = df.withColumn(
        "Total_Fare_BDT",
        F.col("Base_Fare_BDT") + F.col("Tax_Surcharge_BDT")
    )

    logger.info("Schema-aligned data validation and cleaning completed")
    return df




def derive_season(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "Derived_Season",
        F.when(F.month("Departure_Datetime").isin([4, 5]), "Eid_Peak")
         .when(F.month("Departure_Datetime") == 12, "Winter_Peak")
         .otherwise("Non_Peak")
    )



def compute_kpis(df: DataFrame) -> dict:
    avg_fare = (
        df.groupBy("Airline")
          .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT"))
    )

    booking_count = (
        df.groupBy("Airline")
          .count()
          .withColumnRenamed("count", "Booking_Count")
    )

    popular_routes = (
        df.groupBy("Source", "Destination")
          .count()
          .withColumnRenamed("count", "Booking_Count")
          .orderBy(F.desc("Booking_Count"))
          .limit(10)
    )

    seasonal_fares = (
        df.groupBy("Derived_Season")
          .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT"))
    )

    kpis = {
        "avg_fare_by_airline": avg_fare,
        "booking_count_by_airline": booking_count,
        "popular_routes": popular_routes,
        "seasonal_fare_variation": seasonal_fares
    }

    logger.info("KPIs computed including seasonal fare variation")
    return kpis
