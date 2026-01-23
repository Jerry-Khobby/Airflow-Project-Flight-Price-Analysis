CREATE TABLE IF NOT EXISTS staging_flight_prices (
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
    Days_Before_Departure INT
);
