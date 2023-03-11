from typing import List
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col
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
    df_trip = df_trip.withColumn("uuid", f.expr("uuid()"))

    validate_data_quality(df_trip, ["VendorID", "RatecodeID"], "Trip table")
    return df_trip


def create_taxi_zone_df(spark: SparkSession, zone_data_path) -> DataFrame:
    """Create a zone data frame from input file"""
    # Read zone lookup csv file
    df_zone = spark.read.option("header", True).csv(zone_data_path)
    df_zone.createOrReplaceTempView("zoneTable")

    validate_data_quality(df_zone, ["LocationID", "Zone"], "Zone table")
    return df_zone


def create_pickup_location_df(df_trip: DataFrame, df_zone: DataFrame) -> DataFrame:
    """Create a pickup location data frame"""
    df_taxi_location = df_trip.withColumnRenamed("PULocationID", "pick_up_location_id") \
        .select(col("uuid"), col("pick_up_location_id"))

    df_taxi_location_pick_up = df_taxi_location.join(df_zone,
                                                     (df_taxi_location["pick_up_location_id"] == df_zone["LocationID"])) \
        .select(df_taxi_location["uuid"], df_taxi_location["pick_up_location_id"],
                df_zone["Borough"], df_zone["Zone"]) \
        .withColumnRenamed("Borough", "pick_up_borough") \
        .withColumnRenamed("Zone", "pick_up_zone")

    return df_taxi_location_pick_up


def create_dropoff_location_df(df_trip: DataFrame, df_zone: DataFrame) -> DataFrame:
    """Create a dropoff location data frame"""
    df_taxi_location = df_trip.withColumnRenamed("DOLocationID", "drop_off_location_id") \
        .select(col("uuid"), col("drop_off_location_id"))

    df_taxi_location_drop_off = df_taxi_location.join(df_zone, (df_taxi_location["drop_off_location_id"] == df_zone["LocationID"])) \
        .select(df_taxi_location["uuid"], df_taxi_location["drop_off_location_id"], df_zone["Borough"], df_zone["Zone"]) \
        .withColumnRenamed("Borough", "drop_off_borough") \
        .withColumnRenamed("Zone", "drop_off_zone")

    return df_taxi_location_drop_off


def create_fare_df(df_trip: DataFrame) -> DataFrame:
    """Create a fare information data frame"""
    df_taxi_fare = df_trip.select(col("uuid"), col("VendorID"), col("payment_type"), col(
        "fare_amount"), col("extra"), col("mta_tax"), col("tip_amount"), col("total_amount"))

    return df_taxi_fare


def create_passenger_df(df_trip: DataFrame) -> DataFrame:
    """Create a passenger with trip distance information data frame"""
    df_taxi_passenger = df_trip.select(col("uuid"), col(
        "VendorID"), col("passenger_count"), col("trip_distance"))

    return df_taxi_passenger


def create_surcharge_df(df_trip: DataFrame) -> DataFrame:
    """Create a surcharge information data frame"""
    df_surcharge = df_trip.select(col("uuid"), col("VendorID"),
                                  col("improvement_surcharge"), col("congestion_surcharge"))

    return df_surcharge


def write_data_to_dir(df_table: DataFrame, output_dir_path: str, table_name: str) -> None:
    """Write data frames in parquet format."""
    os.makedirs(output_dir_path, exist_ok=True)
    output_path = os.path.join(output_dir_path, table_name)
    df_table.write.mode("overwrite").parquet(output_path)
