# Databricks notebook source
# Import neccessary libraries
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# Define variables
mount_name = "data_lake"

base_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
latitude = 59.3294
longitude = 18.0687
start_date = datetime(2022, 11, 1)
end_date = datetime.now() - timedelta(days=1)

spark = SparkSession.builder.appName("ElectricityPriceETL").getOrCreate()

# COMMAND ----------

# Fetch data through API
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

# Loop through each day and save data
current_date = start_date
while current_date <= end_date:
    try:
        # Fetch data for the current date
        df = fetch_weather_data(current_date)
        if df is not None and not df.empty:
            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(df)

            formatted_spark_df = (
                spark_df
                .withColumn("time", F.to_timestamp("time"))
            )            
            filtered_spark_df = formatted_spark_df.select("time", "temperature_2m", "relative_humidity_2m", "precipitation", "cloud_cover", "wind_speed_80m", "uv_index", "is_day", "sunshine_duration")

            # Define the file path with the required directory structure
            file_path = (
                f"/mnt/{mount_name}/weather_data/tier=silver/"
                f"year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"
                f"weather_data.parquet"
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


file_path = "/mnt/data_lake/weather_data/tier=silver/year=2022/month=11/day=02/weather_data.parquet"

# Read the Parquet file into a Spark DataFrame
df_loaded = spark.read.parquet(file_path)

# Display the DataFrame
display(df_loaded)
