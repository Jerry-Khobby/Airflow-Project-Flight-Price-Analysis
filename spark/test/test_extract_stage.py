import os
from pyspark.sql import SparkSession
from spark.utils import create_spark_session
from spark.etl.extract import read_csv_to_spark, stage_to_mysql

def main():
    print("Starting Extract & Stage test...")

    spark = create_spark_session("TestExtractStage")

    csv_path = "/opt/data/Flight_Price_Dataset_of_Bangladesh.csv"
    print(f"Reading CSV from {csv_path} ...")
    df = read_csv_to_spark(spark, csv_path)
    print(f"CSV loaded. Row count: {df.count()}")

    print("Writing to MySQL staging table...")
    stage_to_mysql(
        df,
        jdbc_url="jdbc:mysql://mysql:3306/mysql_db",
        user="mysql_user",
        password="mysql_pass",
        table_name="staging_flight_prices"
    )

    print(f"Data successfully staged to MySQL! Total rows: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
