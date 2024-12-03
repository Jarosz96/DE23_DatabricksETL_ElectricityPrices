# Databricks notebook source
# Import neccessary libraries
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# COMMAND ----------

# Define variables
mount_name = "data_lake"

base_url = "https://www.elprisetjustnu.se/api/v1/prices"
start_date = datetime(2022, 11, 1)
end_date = datetime.now() - timedelta(days=1)

spark = SparkSession.builder.appName("ElectricityPriceETL").getOrCreate()

# COMMAND ----------

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

# Loop through each day and save data
current_date = start_date
while current_date <= end_date:
    try:
        # Fetch data for the current date
        df = fetch_data(current_date.year, current_date.month, current_date.day)
        if df is not None and not df.empty:
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(df)

            # Remove timezone offset from `time_start` and `time_end` before converting to timestamp
            formatted_spark_df = (
                spark_df
                .withColumn("time_start", F.expr("substring(time_start, 1, 19)"))   # Remove timezone part
                .withColumn("time_end", F.expr("substring(time_end, 1, 19)"))       # Remove timezone part
                .withColumn("time_start", F.to_timestamp("time_start"))             # Convert to timestamp
                .withColumn("time_end", F.to_timestamp("time_end"))                 # Convert to timestamp
            )
            # Select only the required columns
            filtered_spark_df = formatted_spark_df.select("SEK_per_kWh", "time_start", "time_end")

            # Define the file path with the directory structure
            file_path = (
                f"/mnt/{mount_name}/electricity_data/tier=silver/"
                f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"
                f"electricity_data.parquet"
            )

            # Save the data to Azure Data Lake in Parquet format
            filtered_spark_df.write.mode("overwrite").parquet(file_path)
            print(f"Data for {current_date.strftime('%Y-%m-%d')} saved to {file_path}")
        else:
            print(f"No data available for {current_date.strftime('%Y-%m-%d')}.")

    except Exception as e:
        print(f"Error processing data for {current_date.strftime('%Y-%m-%d')}: {e}")

    # Increment the date
    current_date += timedelta(days=1)

# COMMAND ----------


file_path = "/mnt/data_lake/electricity_data/tier=silver/year=2022/month=11/day=02/electricity_data.parquet"

# Read the Parquet file into a Spark DataFrame
df_loaded = spark.read.parquet(file_path)

# Display the DataFrame
display(df_loaded)
