# Databricks notebook source
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Define base paths
mount_name = "data_lake"
silver_electricity_path = f"/mnt/{mount_name}/electricity_data/tier=silver/"
silver_weather_path = f"/mnt/{mount_name}/weather_data/tier=silver/"
gold_combined_path = f"/mnt/{mount_name}/tier=gold/electricity_weather_combined/"

spark = SparkSession.builder.appName("ElectricityWeatherGoldTier").getOrCreate()

# COMMAND ----------

# Get a list of all available dates from the given directory
def get_dates(path):
    try:
        year_folders = dbutils.fs.ls(path)
        dates = []
        for year in year_folders:
            year_str = year.name.replace("year=", "").replace("/", "")  # Remove 'year=' prefix
            month_folders = dbutils.fs.ls(year.path)
            for month in month_folders:
                month_str = month.name.replace("month=", "").replace("/", "")  # Remove 'month=' prefix
                day_folders = dbutils.fs.ls(month.path)
                for day in day_folders:
                    day_str = day.name.replace("day=", "").replace("/", "")  # Remove 'day=' prefix
                    dates.append((int(year_str), int(month_str), int(day_str)))
        return dates
    except Exception as e:
        print(f"Error listing dates in {path}: {e}")
        return []

# COMMAND ----------

# Fetch all unique dates from electricity and weather data
electricity_dates = set(get_dates(silver_electricity_path))
weather_dates = set(get_dates(silver_weather_path))
common_dates = sorted(electricity_dates.intersection(weather_dates))  # Sort the dates

# Process each date and save combined data to gold tier
for year, month, day in common_dates:
    try:
        # Define file paths for the current date
        electricity_file_path = f"{silver_electricity_path}year={year}/month={month:02d}/day={day:02d}/electricity_data.parquet"
        weather_file_path = f"{silver_weather_path}year={year}/month={month:02d}/day={day:02d}/weather_data.parquet"
        combined_file_path = f"{gold_combined_path}year={year}/month={month:02d}/day={day:02d}/electricity_weather_combined.parquet"

        # Read the electricity and weather data for the current date
        electricity_df = spark.read.parquet(electricity_file_path)
        weather_df = spark.read.parquet(weather_file_path)

        # Rename `time` in weather_df to `time_start` for joining
        weather_df = weather_df.withColumnRenamed("time", "time_start")

        # Join both datasets on the "time" column
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

    except Exception as e:
        print(f"Error processing data for {year}-{month:02d}-{day:02d}: {e}")


# COMMAND ----------


file_path = "/mnt/data_lake/tier=gold/electricity_weather_combined/year=2022/month=11/day=02/electricity_weather_combined.parquet"

# Read the Parquet file into a Spark DataFrame
df_loaded = spark.read.parquet(file_path)

# Display the DataFrame
display(df_loaded)

