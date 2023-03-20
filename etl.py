import boto3

from configparser import ConfigParser
from module.config import get_config
from module.sparkUtils import *


def write_to_s3(config: ConfigParser, output_path: str) -> None:
    """Write the output data from output dir to S3."""

    aws_key = config["AWS"]["KEY"]
    aws_secret = config["AWS"]["SECRET"]
    s3_bucket = config["AWS"]["S3"]

    session = boto3.Session(aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret,
                            region_name="us-east-1")
    s3_session = session.resource("s3")
    bucket = s3_session.Bucket(s3_bucket)

    for subdir, _, files in os.walk(output_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, "rb") as data:
                bucket.put_object(
                    Key=full_path[len(output_path) + 1:], Body=data)


def main():
    """Main ETL function"""
    spark = initialize_spark()

    input_dir = "input_data"
    output_dir = "output_data"
    config = get_config("dl.cfg")

    df_trip = create_taxi_trip_df(
        spark, f"{input_dir}/yellow_tripdata_2019-01.parquet")
    df_zone = create_taxi_zone_df(spark, f"{input_dir}/taxi_zones_lookup.csv")
    df_date = create_date_df(df_trip)

    df_pickup_location_gain = create_location_gain_table(df_trip, df_zone, "pick_up_location_id")
    df_dropoff_location_gain = create_location_gain_table(df_trip, df_zone, "drop_off_location_id")
    df_daily_passenger_distance = create_daily_passenger_distance_df(df_trip, df_date)


    write_data_to_dir(df_pickup_location_gain,
                      output_dir, "pickup_location_gain")
    write_data_to_dir(df_dropoff_location_gain,
                      output_dir, "dropoff_location_gain")
    write_data_to_dir(df_daily_passenger_distance, output_dir, "daily_passenger_distance")

    write_to_s3(config, output_dir)

if __name__ == "__main__":
    main()
