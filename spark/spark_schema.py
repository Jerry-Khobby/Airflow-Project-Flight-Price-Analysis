
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, DecimalType

SPARK_SCHEMA = StructType([
    StructField("Airline", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Source_Name", StringType(), True),
    StructField("Destination", StringType(), True),
    StructField("Destination_Name", StringType(), True),
    StructField("Departure_Datetime", TimestampType(), True),
    StructField("Arrival_Datetime", TimestampType(), True),
    StructField("Duration_Hrs", FloatType(), True),
    StructField("Stopovers", StringType(), True),
    StructField("Aircraft_Type", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Booking_Source", StringType(), True),
    StructField("Base_Fare_BDT", DecimalType(10, 2), True),
    StructField("Tax_Surcharge_BDT", DecimalType(10, 2), True),
    StructField("Total_Fare_BDT", DecimalType(10, 2), True),
    StructField("Seasonality", StringType(), True),
    StructField("Days_Before_Departure", IntegerType(), True)
])