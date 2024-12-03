# Databricks notebook source
# Import neccessary libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# COMMAND ----------

# Define base paths
mount_name = "data_lake"

silver_electricity_path = f"/mnt/{mount_name}/electricity_data/tier=silver/"
silver_weather_path = f"/mnt/{mount_name}/weather_data/tier=silver/"
gold_combined_path = f"/mnt/{mount_name}/tier=gold/electricity_weather_combined/"

yesterday = datetime.now() - timedelta(days=1)
year, month, day = yesterday.year, yesterday.month, yesterday.day

electricity_file_path = f"{silver_electricity_path}year={year}/month={month:02d}/day={day:02d}/electricity_data.parquet"
weather_file_path = f"{silver_weather_path}year={year}/month={month:02d}/day={day:02d}/weather_data.parquet"
combined_file_path = f"{gold_combined_path}year={year}/month={month:02d}/day={day:02d}/electricity_weather_combined.parquet"


# COMMAND ----------

# Function to check if data for a specific file path exists
def data_exists(file_path):
    try:
        # List all files in the directory of the file path
        directory = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        print(f"Checking for file in directory: {directory}")  # Debug statement
        files = dbutils.fs.ls(directory)
        
        # Check for a directory with the matching name (Parquet files are written as directories)
        for file in files:
            print(f"Found file: {file.name}")  # Debug statement
            if file.name.rstrip("/") == file_name:  # Remove trailing slash before comparison
                print(f"File exists: {file_path}")  # Debug statement
                return True
        return False
    except Exception as e:
        # If directory doesn't exist or another error occurs, assume data doesn't exist
        print(f"Error checking existence of {file_path}: {e}")
        return False


# COMMAND ----------

if not data_exists(combined_file_path):
    try:
        # Check if electricity and weather files exist
        if data_exists(electricity_file_path) and data_exists(weather_file_path):
            # Read electricity and weather data
            electricity_df = spark.read.parquet(electricity_file_path)
            weather_df = spark.read.parquet(weather_file_path)

            # Rename `time` in weather_df to `time_start` for joining
            weather_df = weather_df.withColumnRenamed("time", "time_start")

            # Join both datasets on the "time_start" column
            combined_df = electricity_df.join(weather_df, on="time_start", how="inner")

            # Add additional features for gold tier
            combined_df = combined_df.withColumn("hour_of_day", F.hour("time_start"))
            combined_df = combined_df.withColumn("day_of_week", F.dayofweek("time_start"))
            combined_df = combined_df.withColumn("is_weekend", (F.dayofweek("time_start") >= 6).cast("int"))

            # Create daily aggregates
            daily_aggregates = combined_df.groupBy(F.to_date("time_start").alias("date")).agg(
                F.avg("temperature_2m").alias("avg_temperature"),
                F.max("temperature_2m").alias("max_temperature"),
                F.min("temperature_2m").alias("min_temperature"),
                F.sum("precipitation").alias("total_precipitation"),
                F.avg("SEK_per_kWh").alias("avg_price"),
                F.max("SEK_per_kWh").alias("max_price"),
                F.min("SEK_per_kWh").alias("min_price"),
                F.sum("sunshine_duration").alias("total_sunshine_duration"),
                F.sum("uv_index").alias("sum_uv_index")
            )

            # Join daily aggregates back to the hourly data
            combined_with_daily = combined_df.join(
                daily_aggregates,
                on=F.to_date("time_start") == daily_aggregates["date"],
                how="left"
            ).drop("date")  # Drop duplicate "date" column

            # Save the combined data to the gold tier directory
            combined_with_daily.write.mode("overwrite").parquet(combined_file_path)
            print(f"Combined data for {year}-{month:02d}-{day:02d} saved to {combined_file_path}")
        else:
            print(f"Missing data for {year}-{month:02d}-{day:02d}. Skipping processing.")
    except Exception as e:
        print(f"Error processing data for {year}-{month:02d}-{day:02d}: {e}")
else:
    print(f"Data for {year}-{month:02d}-{day:02d} already exists at {combined_file_path}.")