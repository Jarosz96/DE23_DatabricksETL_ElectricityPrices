# Databricks notebook source
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import math

# COMMAND ----------

# Define variables
mount_name = "data_lake"

data_path = f"/mnt/{mount_name}/combined_gold_data.csv"

spark = SparkSession.builder.appName("ElectricityPricePrediction").getOrCreate()
data = spark.read.csv(data_path, header=True, inferSchema=True)

# COMMAND ----------

# Add cyclical features
data = data.withColumn("day_of_year", F.dayofyear(F.col("time_start")))

data = (
    data.withColumn("hour_sin", F.sin(2 * F.lit(math.pi) * F.col("hour_of_day") / 24))
        .withColumn("hour_cos", F.cos(2 * F.lit(math.pi) * F.col("hour_of_day") / 24))
        .withColumn("day_sin", F.sin(2 * F.lit(math.pi) * F.col("day_of_year") / 365))
        .withColumn("day_cos", F.cos(2 * F.lit(math.pi) * F.col("day_of_year") / 365))
)

# Normalize numerical features if necessary
numerical_features = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "cloud_cover", "wind_speed_80m", "uv_index", "sunshine_duration",
    "avg_temperature", "max_temperature", "min_temperature",
    "total_precipitation", "total_sunshine_duration", "sum_uv_index"
]

# Prepare the feature vector
feature_cols = numerical_features + ["hour_sin", "hour_cos", "day_of_week", "is_weekend", "month", "day_sin", "day_cos"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# COMMAND ----------

# Step 2: Prepare Training Data
data = assembler.transform(data)
train_data = data.filter((data["time_start"] >= "2023-01-01") & (data["time_start"] < "2024-01-01"))
test_data = data.filter(data["time_start"] >= "2024-01-01")

# COMMAND ----------

# Step 3: Train the Model
gbt = GBTRegressor(featuresCol="features", labelCol="SEK_per_kWh", maxIter=50, maxDepth=5, stepSize=0.05)
model = gbt.fit(train_data)

# COMMAND ----------

# Step 4: Evaluate the Model
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="SEK_per_kWh", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2_evaluator = RegressionEvaluator(labelCol="SEK_per_kWh", predictionCol="prediction", metricName="r2")
r2 = r2_evaluator.evaluate(predictions)

print(f"Model RMSE: {rmse}")
print(f"Model R²: {r2}")

# COMMAND ----------

# Step 5: Save the Model
model.save(f"/mnt/{mount_name}/models/gbt_electricity_price_model/")