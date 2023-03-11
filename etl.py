import boto3

from configparser import ConfigParser
from module.config import get_config
from module.sparkUtils import *


def write_to_s3(config: ConfigParser, data_path: str) -> None:
    """Write the output data from output dir to S3."""

    aws_key = config["AWS"]["KEY"]
    aws_secret = config["AWS"]["SECRET"]
    s3_bucket = config["AWS"]["S3"]

    session = boto3.Session(aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret,
                            region_name="us-east-1")
    s3_session = session.resource("s3")
    bucket = s3_session.Bucket(s3_bucket)

    for subdir, _, files in os.walk(data_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, "rb") as data:
                bucket.put_object(
                    Key=full_path[len(data_path) + 1:], Body=data)


def main():
    """Main ETL function"""
    spark = initialize_spark()

    input_dir = "input_data"
    output_dir = "output_data"
    config = get_config("dl.cfg")

    df_trip = create_taxi_trip_df(
        spark, f"{input_dir}/yellow_tripdata_2019-01.parquet")
    df_zone = create_taxi_zone_df(spark, f"{input_dir}/taxi_zones_lookup.csv")

    df_taxi_location_pick_up = create_pickup_location_df(df_trip, df_zone)
    df_taxi_location_drop_off = create_dropoff_location_df(df_trip, df_zone)

    df_fare = create_fare_df(df_trip)
    df_passenger = create_fare_df(df_trip)
    df_surcharge = create_surcharge_df(df_trip)

    write_data_to_dir(df_taxi_location_pick_up,
                      output_dir, "taxi_location_pickup")
    write_data_to_dir(df_taxi_location_drop_off,
                      output_dir, "taxi_location_drop_off")
    write_data_to_dir(df_fare, output_dir, "fare")
    write_data_to_dir(df_passenger, output_dir, "passenger")
    write_data_to_dir(df_surcharge, output_dir, "surcharge")

    write_to_s3(config, data_path=output_dir)


if __name__ == "__main__":
    main()
