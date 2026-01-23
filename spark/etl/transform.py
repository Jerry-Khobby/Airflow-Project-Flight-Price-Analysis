# spark/etl/transform.py
from pyspark.sql import DataFrame, functions as F
from spark.utils import setup_logger  # FIXED

logger = setup_logger("transform", "/opt/airflow/logs/transform.log")

def validate_and_clean(df: DataFrame) -> DataFrame:
    df = df.fillna({
        "Airline": "Unknown",
        "Source": "UNK",
        "Source_Name": "Unknown",
        "Destination": "UNK",
        "Destination_Name": "Unknown",
        "Base_Fare_BDT": 0,
        "Tax_Surcharge_BDT": 0,
        "Total_Fare_BDT": 0,
        "Days_Before_Departure": 0
    })

    for col in ["Base_Fare_BDT", "Tax_Surcharge_BDT", "Total_Fare_BDT"]:
        df = df.withColumn(col, F.when(F.col(col) < 0, 0).otherwise(F.col(col)))
    
    df = df.withColumn("Total_Fare_BDT", F.col("Base_Fare_BDT") + F.col("Tax_Surcharge_BDT"))

    logger.info("Data validated and cleaned")
    return df

def compute_kpis(df: DataFrame) -> dict:
    avg_fare = df.groupBy("Airline").agg(F.round(F.avg("Total_Fare_BDT"),2).alias("Avg_Fare_BDT"))
    booking_count = df.groupBy("Airline").count().withColumnRenamed("count", "Booking_Count")
    popular_routes = df.groupBy("Source", "Destination") \
                       .count() \
                       .withColumnRenamed("count", "Booking_Count") \
                       .orderBy(F.desc("Booking_Count")) \
                       .limit(10)
    
    kpis = {
        "avg_fare_by_airline": avg_fare,
        "booking_count_by_airline": booking_count,
        "popular_routes": popular_routes
    }
    logger.info("KPIs computed")
    return kpis