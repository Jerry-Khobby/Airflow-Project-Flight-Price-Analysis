from pyspark.sql import DataFrame, functions as F
from spark.utils import setup_logger, write_df_to_mysql

logger = setup_logger("transform", "/opt/airflow/logs/transform.log")

def validate_and_clean(df: DataFrame) -> DataFrame:
    initial_count = df.count()
    logger.info(f"Starting validation on {initial_count} rows")
    

    # Filter NULL primary keys
    pk_columns = ["Airline", "Source", "Destination", "Departure_Datetime", "Booking_Source"]
    for col in pk_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            logger.warning(f"Removing {null_count} rows with NULL {col}")
            df = df.filter(F.col(col).isNotNull())
    
    
    # String validation
    
    # Airline: Required, non-empty
    df = df.withColumn("Airline", 
        F.when(F.trim(F.col("Airline")) == "", "Unknown")
         .otherwise(F.trim(F.col("Airline"))))
    
    # Source: IATA code, 3 chars, uppercase
    df = df.withColumn("Source", 
        F.when(F.length(F.trim(F.col("Source"))) == 3, F.upper(F.trim(F.col("Source"))))
         .otherwise("UNK"))
    
    # Source_Name: Clean and trim
    df = df.withColumn("Source_Name",
        F.when(F.trim(F.col("Source_Name")) == "", "Unknown")
         .otherwise(F.trim(F.col("Source_Name"))))
    
    # Destination: IATA code, 3 chars, uppercase
    df = df.withColumn("Destination",
        F.when(F.length(F.trim(F.col("Destination"))) == 3, F.upper(F.trim(F.col("Destination"))))
         .otherwise("UNK"))
    
    # Destination_Name: Clean and trim
    df = df.withColumn("Destination_Name",
        F.when(F.trim(F.col("Destination_Name")) == "", "Unknown")
         .otherwise(F.trim(F.col("Destination_Name"))))
    
    # Aircraft_Type: Allow NULL or non-empty string
    df = df.withColumn("Aircraft_Type",
        F.when((F.col("Aircraft_Type").isNull()) | (F.trim(F.col("Aircraft_Type")) == ""), None)
         .otherwise(F.trim(F.col("Aircraft_Type"))))
    
    # Class: Must be Economy, Business, or First
    df = df.withColumn("Class",
        F.when(F.upper(F.col("Class")).isin(["ECONOMY", "ECO", "E"]), "Economy")
         .when(F.upper(F.col("Class")).isin(["BUSINESS", "BUS", "B"]), "Business")
         .when(F.upper(F.col("Class")).isin(["FIRST", "FIRST CLASS", "F"]), "First")
         .otherwise("Economy"))  # Default to Economy
    
    # Booking_Source: Clean and trim
    df = df.withColumn("Booking_Source",
        F.when(F.trim(F.col("Booking_Source")) == "", "Unknown")
         .otherwise(F.trim(F.col("Booking_Source"))))
    
    # Seasonality: Clean and trim
    df = df.withColumn("Seasonality",
        F.when(F.trim(F.col("Seasonality")) == "", None)
         .otherwise(F.trim(F.col("Seasonality"))))
    

    #numeric validations
    # Stopovers: Convert to integer, default 0
    df = df.withColumn("Stopovers",
        F.when(F.col("Stopovers").rlike("^[0-9]+$"), F.col("Stopovers").cast("int"))
         .when(F.col("Stopovers").contains("+"), 
               F.regexp_extract(F.col("Stopovers"), r"(\d+)", 1).cast("int"))
         .otherwise(0))
    
    # Duration_Hrs: Must be positive
    invalid_duration = df.filter((F.col("Duration_Hrs").isNull()) | (F.col("Duration_Hrs") <= 0)).count()
    if invalid_duration > 0:
        logger.warning(f"Found {invalid_duration} invalid Duration_Hrs values, setting to NULL")
    
    df = df.withColumn("Duration_Hrs",
        F.when((F.col("Duration_Hrs").isNull()) | (F.col("Duration_Hrs") <= 0), None)
         .otherwise(F.col("Duration_Hrs")))
    
    # Base_Fare_BDT: Must be non-negative
    invalid_base_fare = df.filter((F.col("Base_Fare_BDT").isNull()) | (F.col("Base_Fare_BDT") < 0)).count()
    if invalid_base_fare > 0:
        logger.warning(f"Found {invalid_base_fare} invalid Base_Fare_BDT, setting to 0")
    
    df = df.withColumn("Base_Fare_BDT",
        F.when((F.col("Base_Fare_BDT").isNull()) | (F.col("Base_Fare_BDT") < 0), 0)
         .otherwise(F.col("Base_Fare_BDT")))
    
    # Tax_Surcharge_BDT: Must be non-negative
    invalid_tax = df.filter((F.col("Tax_Surcharge_BDT").isNull()) | (F.col("Tax_Surcharge_BDT") < 0)).count()
    if invalid_tax > 0:
        logger.warning(f"Found {invalid_tax} invalid Tax_Surcharge_BDT, setting to 0")
    
    df = df.withColumn("Tax_Surcharge_BDT",
        F.when((F.col("Tax_Surcharge_BDT").isNull()) | (F.col("Tax_Surcharge_BDT") < 0), 0)
         .otherwise(F.col("Tax_Surcharge_BDT")))
    
    # Total_Fare_BDT: Recalculate from Base + Tax
    df = df.withColumn("Total_Fare_BDT",
        F.col("Base_Fare_BDT") + F.col("Tax_Surcharge_BDT"))
    
    # Days_Before_Departure: Must be non-negative
    df = df.withColumn("Days_Before_Departure",
        F.when((F.col("Days_Before_Departure").isNull()) | (F.col("Days_Before_Departure") < 0), 0)
         .otherwise(F.col("Days_Before_Departure")))
    
    
    
    # Filter records where Arrival <= Departure (invalid)
    invalid_datetime = df.filter(F.col("Arrival_Datetime") <= F.col("Departure_Datetime")).count()
    if invalid_datetime > 0:
        logger.warning(f"Removing {invalid_datetime} rows where Arrival <= Departure")
        df = df.filter(F.col("Arrival_Datetime") > F.col("Departure_Datetime"))
    

#deduplications
    pk_columns = ["Airline", "Source", "Destination", "Departure_Datetime", "Booking_Source"]
    df = df.dropDuplicates(pk_columns)
    
    final_count = df.count()
    removed = initial_count - final_count
    
    logger.info(f"Validation complete: {final_count} valid rows, {removed} removed")
    
    return df

def derive_season(df: DataFrame) -> DataFrame:
    """Derive seasonal information"""
    df = df.withColumn("Derived_Season",
        F.when(F.month("Departure_Datetime").isin([4, 5]), "Eid_Peak")
         .when(F.month("Departure_Datetime") == 12, "Winter_Peak")
         .otherwise("Non_Peak"))
    
    logger.info("Derived seasonal information")
    return df

def write_transformed_to_mysql(df: DataFrame, jdbc_url: str, user: str, password: str, table_name: str):
    """
    Write transformed data back to MySQL staging (separate table).
    Uses OVERWRITE for idempotency.
    """
    logger.info(f"Writing {df.count()} transformed rows to MySQL: {table_name}")
    write_df_to_mysql(df, table_name, jdbc_url, user, password, mode="overwrite")
    logger.info("Transformed data written to MySQL staging")

def compute_kpis(df: DataFrame) -> dict:
    """Compute KPIs from cleaned data"""
    
    avg_fare = df.groupBy("Airline") \
        .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT")) \
        .orderBy("Airline")
    
    booking_count = df.groupBy("Airline") \
        .count() \
        .withColumnRenamed("count", "Booking_Count") \
        .orderBy("Airline")
    
    popular_routes = df.groupBy("Source", "Destination") \
        .count() \
        .withColumnRenamed("count", "Booking_Count") \
        .orderBy(F.desc("Booking_Count")) \
        .limit(10)
    
    seasonal_fares = df.groupBy("Derived_Season") \
        .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT")) \
        .orderBy("Derived_Season")
    
    kpis = {
        "avg_fare_by_airline": avg_fare,
        "booking_count_by_airline": booking_count,
        "popular_routes": popular_routes,
        "seasonal_fare_variation": seasonal_fares
    }
    
    logger.info("KPIs computed successfully")
    return kpis




#individual kpis 
def compute_avg_fare_by_airline(df: DataFrame) -> DataFrame:
    """Compute average fare by airline"""
    logger.info("Computing KPI: Average Fare by Airline")
    
    result = df.groupBy("Airline") \
        .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT")) \
        .orderBy("Airline")
    
    logger.info(f"Computed avg fare for {result.count()} airlines")
    return result

def compute_booking_count_by_airline(df: DataFrame) -> DataFrame:
    """Compute booking count by airline"""
    logger.info("Computing KPI: Booking Count by Airline")
    
    result = df.groupBy("Airline") \
        .count() \
        .withColumnRenamed("count", "Booking_Count") \
        .orderBy("Airline")
    
    logger.info(f"Computed booking count for {result.count()} airlines")
    return result

def compute_popular_routes(df: DataFrame) -> DataFrame:
    """Compute top 10 popular routes"""
    logger.info("Computing KPI: Popular Routes")
    
    result = df.groupBy("Source", "Destination") \
        .count() \
        .withColumnRenamed("count", "Booking_Count") \
        .orderBy(F.desc("Booking_Count")) \
        .limit(10)
    
    logger.info(f"Computed top {result.count()} popular routes")
    return result

def compute_seasonal_fare_variation(df: DataFrame) -> DataFrame:
    """Compute seasonal fare variation"""
    logger.info("Computing KPI: Seasonal Fare Variation")
    
    result = df.groupBy("Derived_Season") \
        .agg(F.round(F.avg("Total_Fare_BDT"), 2).alias("Avg_Fare_BDT")) \
        .orderBy("Derived_Season")
    
    logger.info(f"Computed seasonal fares for {result.count()} seasons")
    return result