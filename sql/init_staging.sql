-- Table 1: Raw staging (from CSV)
CREATE TABLE IF NOT EXISTS staging_raw (
    Airline VARCHAR(100),
    Source CHAR(3),
    Source_Name VARCHAR(200),
    Destination CHAR(3),
    Destination_Name VARCHAR(200),
    Departure_Datetime DATETIME,
    Arrival_Datetime DATETIME,
    Duration_Hrs FLOAT,
    Stopovers VARCHAR(20),
    Aircraft_Type VARCHAR(50),
    Class VARCHAR(20),
    Booking_Source VARCHAR(50),
    Base_Fare_BDT DECIMAL(10,2),
    Tax_Surcharge_BDT DECIMAL(10,2),
    Total_Fare_BDT DECIMAL(10,2),
    Seasonality VARCHAR(20),
    Days_Before_Departure INT,
    
    INDEX idx_raw_lookup (Airline, Source, Destination, Departure_Datetime, Booking_Source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table 2: Cleaned staging (after transformation)
CREATE TABLE IF NOT EXISTS staging_cleaned (
    Airline VARCHAR(100) NOT NULL,
    Source CHAR(3) NOT NULL,
    Source_Name VARCHAR(200) NOT NULL,
    Destination CHAR(3) NOT NULL,
    Destination_Name VARCHAR(200) NOT NULL,
    Departure_Datetime DATETIME NOT NULL,
    Arrival_Datetime DATETIME NOT NULL,
    Duration_Hrs FLOAT,
    Stopovers INT NOT NULL DEFAULT 0,
    Aircraft_Type VARCHAR(50),
    Class VARCHAR(20) NOT NULL,
    Booking_Source VARCHAR(50) NOT NULL,
    Base_Fare_BDT DECIMAL(10,2) NOT NULL,
    Tax_Surcharge_BDT DECIMAL(10,2) NOT NULL,
    Total_Fare_BDT DECIMAL(10,2) NOT NULL,
    Seasonality VARCHAR(20),
    Days_Before_Departure INT,
    Derived_Season VARCHAR(20),
    
    INDEX idx_cleaned_pk (Airline, Source, Destination, Departure_Datetime, Booking_Source),
    INDEX idx_cleaned_airline (Airline),
    INDEX idx_cleaned_route (Source, Destination)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Note: Both tables use OVERWRITE mode, so no primary keys needed
-- Data is temporary and gets replaced on each DAG run