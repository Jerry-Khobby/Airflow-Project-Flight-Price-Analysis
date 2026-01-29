CREATE TABLE IF NOT EXISTS flights_cleaned (
    Airline VARCHAR(100),
    Source CHAR(3),
    Source_Name VARCHAR(200),
    Destination CHAR(3),
    Destination_Name VARCHAR(200),
    Departure_Datetime TIMESTAMP,
    Arrival_Datetime TIMESTAMP,
    Duration_Hrs FLOAT,
    Stopovers VARCHAR(20),
    Aircraft_Type VARCHAR(50),
    Class VARCHAR(20),
    Booking_Source VARCHAR(50),
    Base_Fare_BDT NUMERIC(10,2),
    Tax_Surcharge_BDT NUMERIC(10,2),
    Total_Fare_BDT NUMERIC(10,2),
    Seasonality VARCHAR(20),
    Days_Before_Departure INT
);



CREATE TABLE IF NOT EXISTS avg_fare_by_airline (
    Airline VARCHAR(100),
    Avg_Fare_BDT NUMERIC(10,2)
);



CREATE TABLE IF NOT EXISTS booking_count_by_airline (
    Airline VARCHAR(100),
    Booking_Count INT
);



CREATE TABLE IF NOT EXISTS popular_routes (
    Source CHAR(3),
    Destination CHAR(3),
    Booking_Count INT
);



CREATE TABLE IF NOT EXISTS seasonal_fare_variation (
    Derived_Season VARCHAR(20),
    Avg_Fare_BDT NUMERIC(10,2)
);



--to be placed in the read me for testing purposes 
SELECT *
FROM avg_fare_by_airline
ORDER BY Avg_Fare_BDT DESC;
