# Databricks notebook source
# Import neccessary libraries
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F

# COMMAND ----------

# Define variables
mount_name = "data_lake"

base_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
latitude = 59.3294
longitude = 18.0687
yesterday = datetime.now() - timedelta(days=1)
file_path = (
    f"/mnt/{mount_name}/weather_data/tier=silver/"
    f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    f"weather_data.parquet"
)

spark = SparkSession.builder.appName("ElectricityPriceETL").getOrCreate()

# COMMAND ----------

# Function to check if yesterday's data already exists
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
    
# Function to fetch weather data
def fetch_weather_data(date):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date.strftime("%Y-%m-%d"),
        "end_date": date.strftime("%Y-%m-%d"),
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "precipitation", 
            "cloud_cover", "wind_speed_80m", 
            "uv_index", "is_day", "sunshine_duration"
        ],
        "wind_speed_unit": "ms"
    }
    
    # Make the API call
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        hourly_data = data.get("hourly", {})
        return pd.DataFrame(hourly_data)
    else:
        print(f"Failed to fetch data for {date.strftime('%Y-%m-%d')}. Status Code: {response.status_code}")
        return None


# COMMAND ----------

# Main process
if not data_exists(file_path):
    try:
        # Fetch data for yesterday
        df = fetch_weather_data(yesterday)
        if df is not None and not df.empty:
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(df)

            formatted_spark_df = (
                spark_df
                .withColumn("time", F.to_timestamp("time"))
            )            
            filtered_spark_df = formatted_spark_df.select("time", "temperature_2m", "relative_humidity_2m", "precipitation", "cloud_cover", "wind_speed_80m", "uv_index", "is_day", "sunshine_duration")

            # Save the data to Azure Data Lake in Parquet format
            filtered_spark_df.write.mode("overwrite").parquet(file_path)
            print(f"Data for {yesterday.strftime('%Y-%m-%d')} saved to {file_path}")
        else:
            print(f"No data available for {yesterday.strftime('%Y-%m-%d')}.")
    except Exception as e:
        print(f"Error processing data for {yesterday.strftime('%Y-%m-%d')}: {e}")
else:
    print(f"Data for {yesterday.strftime('%Y-%m-%d')} already exists at {file_path}.")

# COMMAND ----------

'''
file_path = "/mnt/data_lake/weather_data/tier=silver/year=2024/month=11/day=19/weather_data.parquet"

# Read the Parquet file into a Spark DataFrame
df_loaded = spark.read.parquet(file_path)

# Display the DataFrame
display(df_loaded)
'''