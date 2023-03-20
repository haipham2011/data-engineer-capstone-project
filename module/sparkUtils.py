from typing import List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os


def validate_data_quality(df: DataFrame, col_ids: List[str], table_name: str) -> None:
    """Check if the table contains data and the id column is not null."""
    assert df.count() > 0, f"{table_name} is an emty table"

    for id in col_ids:
        assert (
            df.where(f.col(id).isNull()).count() == 0
        ), f"Column {id} cannot have null values."


def initialize_spark() -> SparkSession:
    """Initialize a spark session."""
    spark = SparkSession.builder.config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def create_taxi_trip_df(spark: SparkSession, trip_data_path) -> DataFrame:
    """Create a trip data frame from input file"""
    # Read taxi parquet files
    df_trip = spark.read.parquet(trip_data_path)
    df_trip.createOrReplaceTempView("taxiTable")

    df_trip = df_trip.withColumn("uuid", f.expr("uuid()")).withColumnRenamed("VendorId", "vendor_id") \
                        .withColumnRenamed("Passenger_count", "passenger_count") \
                        .withColumnRenamed("Trip_distance", "trip_distance") \
                        .withColumnRenamed("PULocationID","pick_up_location_id") \
                        .withColumnRenamed("DOLocationID","drop_off_location_id") \
                        .withColumnRenamed("RateCodeID","rate_code_id") \
                        .withColumnRenamed("Store_and_fwd_flag","store_and_fwd_flag") \
                        .withColumnRenamed("Payment_type","payment_type") \
                        .withColumnRenamed("Fare_amount","fare_amount") \
                        .withColumnRenamed("Extra","extra") \
                        .withColumnRenamed("MTA_tax","mta_tax") \
                        .withColumnRenamed("Improvement_surcharge","improvement_surcharge") \
                        .withColumnRenamed("Tip_amount","tip_amount") \
                        .withColumnRenamed("Tolls_amount","tolls_amount") \
                        .withColumnRenamed("Total_amount","total_amount") \
                        .withColumnRenamed("Congestion_Surcharge","congestion_surcharge") \
                        .withColumnRenamed("Airport_fee","airport_fee") \
                        .withColumn("passenger_count", f.col("passenger_count").cast(t.LongType()))

    validate_data_quality(df_trip, ["vendor_id", "rate_code_id"], "Trip table")
    return df_trip


def create_taxi_zone_df(spark: SparkSession, zone_data_path) -> DataFrame:
    """Create a zone data frame from input file"""
    # Read zone lookup csv file
    df_zone = spark.read.option("header", True).csv(zone_data_path)
    df_zone.createOrReplaceTempView("zoneTable")
    df_zone = df_zone.withColumnRenamed("LocationID", "location_id") \
                    .withColumnRenamed("Borough", "borough") \
                    .withColumnRenamed("Zone", "zone").dropna()
    
    validate_data_quality(df_zone, ["LocationID", "Zone"], "Zone table")
    return df_zone


def create_date_df(df_trip: DataFrame) -> DataFrame:
    df_date = df_trip.select(f.col("tpep_pickup_datetime")) \
                .withColumnRenamed("tpep_pickup_datetime", "timestamp") \
                .withColumn("year", f.year(f.col("timestamp"))) \
                .withColumn("month", f.month(f.col("timestamp"))) \
                .withColumn("day", f.dayofmonth(f.col("timestamp"))) \
                .withColumn("hour", f.hour(f.col("timestamp"))) \
                .withColumn("minute", f.minute(f.col("timestamp"))) \
                .withColumn("second", f.second(f.col("timestamp"))) \
                .withColumn("day_of_week", f.dayofweek(f.col("timestamp")))
    
    return df_date

def create_location_gain_table(df_trip: DataFrame, df_zone: DataFrame, type_location: str) -> DataFrame:
    """Create location gain table"""
    df_location_gain = df_trip.select(f.col(type_location),
                                      f.col("fare_amount"), f.col(
                                          "extra"), f.col("mta_tax"),
                                      f.col("tip_amount"), f.col("tolls_amount"), f.col("total_amount")) \
        .groupBy(type_location).agg(f.sum("fare_amount").alias("total_fare_amount"),
                                    f.sum("extra").alias(
            "total_extra"),
        f.sum("mta_tax").alias(
            "total_mta_tax"),
        f.sum("tip_amount").alias(
            "total_tip_amount"),
        f.sum("tolls_amount").alias("total_tolls_amount"))

    df_zone_simplified = df_zone.select(f.col("zone"), f.col("location_id"))
    df_location_gain = df_location_gain.join(df_zone_simplified) \
        .where(df_zone_simplified["location_id"] == df_location_gain[type_location]) \
        .drop(type_location)

    return df_location_gain


def create_daily_passenger_distance_df(df_trip: DataFrame, df_date: DataFrame) -> DataFrame:
    """Create a daily passenger distance data frame"""
    df_trip_date = df_trip.join(df_date).where(df_trip["tpep_pickup_datetime"] == df_date["timestamp"]) \
                    .select(f.col("day_of_week"), f.col("passenger_count"), f.col("trip_distance")).dropna()
                            
    df_daily_passenger = df_trip_date.groupBy("day_of_week").agg(f.sum("passenger_count").alias("total_passenger_count"),
                                                             f.sum("trip_distance").alias("total_trip_distance"))

    return df_daily_passenger


def write_data_to_dir(df_table: DataFrame, output_dir_path: str, table_name: str) -> None:
    """Write data frames in parquet format."""
    os.makedirs(output_dir_path, exist_ok=True)
    output_path = os.path.join(output_dir_path, table_name)
    df_table.write.mode("overwrite").parquet(output_path)
