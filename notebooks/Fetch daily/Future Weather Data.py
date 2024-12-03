# Databricks notebook source
# Import neccessary libraries
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import math

# COMMAND ----------

# Define variables
mount_name = "data_lake"

base_url = "https://api.open-meteo.com/v1/forecast"
latitude = 59.3294
longitude = 18.0687
yesterday = datetime.now() - timedelta(days=1)
file_path = f"/mnt/{mount_name}/tier=gold/weather_forecast/"

spark = SparkSession.builder.appName("ElectricityPriceETL").getOrCreate()

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Forecasted Weather Data") \
    .getOrCreate()

# API parameters for forecasted data
BASE_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = 59.3294  # Example: Stockholm
LONGITUDE = 18.0687
PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "hourly": "temperature_2m,relative_humidity_2m,precipitation,weather_code,cloud_cover,"
              "wind_speed_80m,uv_index,is_day,sunshine_duration",
    "wind_speed_unit": "ms"
}

# Fetch forecasted data from the Open-Meteo API
response = requests.get(BASE_URL, params=PARAMS)
if response.status_code != 200:
    raise Exception(f"API request failed with status code {response.status_code}")

# Parse JSON response
data = response.json()

# Extract hourly data
hourly_data = data["hourly"]
dates = hourly_data["time"]
temperature_2m = hourly_data["temperature_2m"]
relative_humidity_2m = hourly_data["relative_humidity_2m"]
precipitation = hourly_data["precipitation"]
cloud_cover = hourly_data["cloud_cover"]
wind_speed_80m = hourly_data["wind_speed_80m"]
uv_index = hourly_data["uv_index"]
is_day = hourly_data["is_day"]
sunshine_duration = hourly_data["sunshine_duration"]

# Create a Pandas DataFrame
df = pd.DataFrame({
    "date": dates,
    "temperature_2m": temperature_2m,
    "relative_humidity_2m": relative_humidity_2m,
    "precipitation": precipitation,
    "cloud_cover": cloud_cover,
    "wind_speed_80m": wind_speed_80m,
    "uv_index": uv_index,
    "is_day": is_day,
    "sunshine_duration": sunshine_duration
})

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Rename 'time' column to 'time_start'
spark_df = spark_df.withColumnRenamed("date", "time_start")

# Add additional features for gold tier
spark_df = spark_df.withColumn("hour_of_day", F.hour("time_start"))
spark_df = spark_df.withColumn("day_of_week", F.dayofweek("time_start"))
spark_df = spark_df.withColumn("is_weekend", (F.dayofweek("time_start") >= 6).cast("int"))

# Create daily aggregates
daily_aggregates = spark_df.groupBy(F.to_date("time_start").alias("date")).agg(
    F.avg("temperature_2m").alias("avg_temperature"),
    F.max("temperature_2m").alias("max_temperature"),
    F.min("temperature_2m").alias("min_temperature"),
    F.sum("precipitation").alias("total_precipitation"),
    F.sum("sunshine_duration").alias("total_sunshine_duration"),
    F.sum("uv_index").alias("sum_uv_index")
)

# Join daily aggregates back to the hourly data
spark_df = spark_df \
    .withColumn("date", F.to_date("time_start")) \
    .join(daily_aggregates, on="date", how="left") \
    .drop("date")  # Drop the temporary date column used for the join

# Adjust column datatypes
spark_df = spark_df \
    .withColumn("time_start", F.to_timestamp(F.col("time_start"), "yyyy-MM-dd'T'HH:mm")) \
    .withColumn("temperature_2m", F.col("temperature_2m").cast("double")) \
    .withColumn("precipitation", F.col("precipitation").cast("double")) \
    .withColumn("wind_speed_80m", F.col("wind_speed_80m").cast("double")) \
    .withColumn("uv_index", F.col("uv_index").cast("double")) \
    .withColumn("sunshine_duration", F.col("sunshine_duration").cast("double")) \
    .withColumn("relative_humidity_2m", F.col("relative_humidity_2m").cast("bigint")) \
    .withColumn("cloud_cover", F.col("cloud_cover").cast("bigint")) \
    .withColumn("is_day", F.col("is_day").cast("bigint"))

spark_df = spark_df.withColumn("day_of_year", F.dayofyear(F.col("time_start")))

spark_df = (
    spark_df.withColumn("hour_sin", F.sin(2 * F.lit(math.pi) * F.col("hour_of_day") / 24))
        .withColumn("hour_cos", F.cos(2 * F.lit(math.pi) * F.col("hour_of_day") / 24))
        .withColumn("day_sin", F.sin(2 * F.lit(math.pi) * F.col("day_of_year") / 365))
        .withColumn("day_cos", F.cos(2 * F.lit(math.pi) * F.col("day_of_year") / 365))
)

# Save as a Parquet file
output_path = "/mnt/data_lake/weather_forecast.parquet"
if not os.path.exists(os.path.dirname(output_path)):
    os.makedirs(os.path.dirname(output_path))

spark_df.write.mode("overwrite").parquet(output_path)

print(f"Forecasted data saved to {output_path}")


# COMMAND ----------

'''
# Read the Parquet file into a Spark DataFrame
parquet_file_path = "/mnt/data_lake/weather_forecast.parquet"
df_parquet = spark.read.parquet(parquet_file_path)

# Display the DataFrame
display(df_parquet)
'''

# COMMAND ----------

'''
 Read the Parquet file from the mounted Azure Data Lake Storage (tier=gold)
gold_file_path = "/mnt/data_lake/tier=gold/electricity_weather_combined/year=2024/month=11/day=30/electricity_weather_combined.parquet"
gold_df = spark.read.parquet(gold_file_path)

# Display the DataFrame
display(gold_df)
'''