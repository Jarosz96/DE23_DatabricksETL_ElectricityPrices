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

base_url = "https://www.elprisetjustnu.se/api/v1/prices"
yesterday = datetime.now() - timedelta(days=1)
file_path = (
    f"/mnt/{mount_name}/electricity_data/tier=silver/"
    f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    f"electricity_data.parquet"
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

# Fetch data through API
def get_api_url(year, month, day, price_class="SE3"):
    return f"{base_url}/{year}/{month:02d}-{day:02d}_{price_class}.json"
  
def fetch_data(year, month, day):
    url = get_api_url(year, month, day)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        print(f"Failed to fetch data for {year}-{month:02d}-{day:02d}. Status Code: {response.status_code}")
        return None

# COMMAND ----------

# Main process
if not data_exists(file_path):
    try:
        # Fetch data for yesterday
        df = fetch_data(yesterday.year, yesterday.month, yesterday.day)
        if df is not None and not df.empty:
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(df)

            # Remove timezone offset from `time_start` and `time_end` before converting to timestamp
            formatted_spark_df = (
                spark_df
                .withColumn("time_start", F.expr("substring(time_start, 1, 19)"))  # Remove timezone part
                .withColumn("time_end", F.expr("substring(time_end, 1, 19)"))      # Remove timezone part
                .withColumn("time_start", F.to_timestamp("time_start"))           # Convert to timestamp
                .withColumn("time_end", F.to_timestamp("time_end"))               # Convert to timestamp
            )
            # Select only the required columns
            filtered_spark_df = formatted_spark_df.select("SEK_per_kWh", "time_start", "time_end")

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
file_path = "/mnt/data_lake/electricity_data/tier=silver/year=2024/month=11/day=18/electricity_data.parquet"

# Read the Parquet file into a Spark DataFrame
df_loaded = spark.read.parquet(file_path)

# Display the DataFrame
display(df_loaded)
'''