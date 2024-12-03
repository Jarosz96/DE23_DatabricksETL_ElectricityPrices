# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressionModel

# Initialize Spark session
spark = SparkSession.builder.appName("ElectricityPricePrediction").getOrCreate()

# Define file paths
forecast_file_path = "/mnt/data_lake/weather_forecast.parquet"
model_path = "/mnt/data_lake/models/gbt_electricity_price_model/"
output_path = "/mnt/data_lake/electricity_price_predictions4/"

# Step 1: Load Weather Forecast Data
print("Loading weather forecast data...")
forecast_df = spark.read.parquet(forecast_file_path)

# Step 2: Preprocess the Data
print("Preprocessing data...")

# Add derived columns
forecast_df = forecast_df.withColumn("month", F.month(F.col("time_start")))

# Step 3: Assemble Features
# Define the list of feature columns (matching training data)
feature_columns = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "cloud_cover", "wind_speed_80m", "uv_index", "sunshine_duration",
    "avg_temperature", "max_temperature", "min_temperature",
    "total_precipitation", "total_sunshine_duration", "sum_uv_index",
    "hour_sin", "hour_cos", "day_sin", "day_cos", "day_of_week", "is_weekend", "month"
]

# Create feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
prepared_df = assembler.transform(forecast_df)

# Step 4: Load the Model
print("Loading the trained model...")
model = GBTRegressionModel.load(model_path)

# Step 5: Make Predictions
print("Running predictions...")
predictions = model.transform(prepared_df)

# Step 6: Select all relevant columns and rename the prediction column
output_df = predictions.select(
    "time_start", *feature_columns, "prediction"
).withColumnRenamed("prediction", "SEK_per_kWh")

# Step 7: Calculate Aggregates and Add Columns
print("Calculating aggregates by date...")

# Extract date from time_start
output_df = output_df.withColumn("date", F.to_date("time_start"))
output_df = output_df.withColumn("hour_of_day", F.hour("time_start"))

# Calculate average, min, and max grouped by date
aggregates_df = output_df.groupBy("date").agg(
    F.avg("SEK_per_kWh").alias("avg_SEK_per_kWh"),
    F.min("SEK_per_kWh").alias("min_SEK_per_kWh"),
    F.max("SEK_per_kWh").alias("max_SEK_per_kWh")
)

# Join the aggregates back to the main DataFrame
output_df = output_df.join(aggregates_df, on="date", how="left")

# Step 8: Save Predictions with Aggregates
print("Saving predicted data with aggregates...")
output_df.write.mode("overwrite").parquet(output_path)
print(f"Predictions with aggregates saved to {output_path}")


# COMMAND ----------

'''
# Load and display the saved predictions with aggregates
saved_predictions_df = spark.read.parquet(output_path)
display(saved_predictions_df)
'''