import argparse

from pyspark.sql import SparkSession


def main(date):
    # Create a Spark session
    spark = SparkSession.builder.appName("DataprocOrderProcessing").getOrCreate()

    # Define the path where the datasets are located
    input_path = f"gs://landing-zone-1/orders_input/order_{date}.csv"

    # Read CSV files
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filter the records
    df_filtered = df.filter(df.order_status == "Completed")

    # Write back to GCP with the date naming convention
    output_path = f"gs://landing-zone-1/orders_output/processed_orders_{date}.csv"
    df_filtered.write.csv(output_path, mode="overwrite", header=True)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process date argument")
    parser.add_argument("--date", type=str, required=True, help="Date in yyyymmdd format")

    args = parser.parse_args()
    main(args.date)
