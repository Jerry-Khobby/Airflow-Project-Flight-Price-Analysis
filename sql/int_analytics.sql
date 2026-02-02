CREATE TABLE IF NOT EXISTS flights_cleaned (
    Airline VARCHAR(100) NOT NULL,
    Source CHAR(3) NOT NULL,
    Source_Name VARCHAR(200) NOT NULL,
    Destination CHAR(3) NOT NULL,
    Destination_Name VARCHAR(200) NOT NULL,
    Departure_Datetime TIMESTAMP NOT NULL,
    Arrival_Datetime TIMESTAMP NOT NULL,
    Duration_Hrs NUMERIC(5,2),
    Stopovers INT NOT NULL DEFAULT 0,
    Aircraft_Type VARCHAR(50),
    Class VARCHAR(20) NOT NULL CHECK (Class IN ('Economy','Business','First')),
    Booking_Source VARCHAR(50) NOT NULL,
    Base_Fare_BDT NUMERIC(10,2) NOT NULL,
    Tax_Surcharge_BDT NUMERIC(10,2) NOT NULL,
    Total_Fare_BDT NUMERIC(10,2) NOT NULL,
    Seasonality VARCHAR(20),
    Days_Before_Departure INT,
    Derived_Season VARCHAR(20),
    PRIMARY KEY (Airline, Source, Destination, Departure_Datetime, Booking_Source)
);

CREATE TABLE IF NOT EXISTS avg_fare_by_airline (
    Airline VARCHAR(100) NOT NULL PRIMARY KEY,
    Avg_Fare_BDT NUMERIC(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS booking_count_by_airline (
    Airline VARCHAR(100) NOT NULL PRIMARY KEY,
    Booking_Count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS popular_routes (
    Source CHAR(3) NOT NULL,
    Destination CHAR(3) NOT NULL,
    Booking_Count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (Source, Destination)
);

CREATE TABLE IF NOT EXISTS seasonal_fare_variation (
    Derived_Season VARCHAR(20) NOT NULL PRIMARY KEY,
    Avg_Fare_BDT NUMERIC(10,2) NOT NULL
);



/* SELECT COUNT(*) FROM flights_cleaned;
SELECT 
    "Airline",
    "Source",
    "Destination",
    "Departure_Datetime",
    "Booking_Source",
    COUNT(*)
FROM flights_cleaned
GROUP BY 
    "Airline",
    "Source",
    "Destination",
    "Departure_Datetime",
    "Booking_Source"
HAVING COUNT(*) > 1; */
